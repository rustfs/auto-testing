"""Exercise the real Tier MQTT lifecycle with isolated command fixtures."""

import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parent
CONTAINER_ID = "a" * 64


class TierMqttTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="tier-mqtt-test-")
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name)
        self.bin = self.path / "bin"
        self.bin.mkdir()
        self.env = {
            **os.environ,
            "PATH": f"{self.bin}:{os.environ['PATH']}",
            "TIER_SOURCE_ONLY": "1",
            "RUSTFS_TIER_TEST_TMP": str(self.path / "tmp"),
            "RUSTFS_TIER_ARTIFACTS_DIR": str(self.path / "artifacts"),
            "RUSTFS_HEAL_IP": "127.0.0.1",
            "RUSTFS_MQTT_PORT": "1883",
            "RUSTFS_TIER_MQTT_MODE": "auto",
            "MQTT_FIXTURE": str(self.path),
            "MQTT_FIXTURE_MODE": "ready",
        }
        self.command("mosquitto_sub", """
import json, os, pathlib, sys, time
root = pathlib.Path(os.environ['MQTT_FIXTURE'])
(root / 'subscriber-pid').write_text(str(os.getpid()))
mode = os.environ['MQTT_FIXTURE_MODE']
if mode == 'exit':
    print('Connection refused', file=sys.stderr, flush=True)
    raise SystemExit(1)
while True:
    probe = root / 'probe'
    if mode == 'ready' and probe.exists():
        topic, payload = json.loads(probe.read_text())
        print(topic + ' ' + payload, flush=True)
        probe.unlink()
    time.sleep(0.01)
""")
        self.command("mosquitto_pub", """
import json, os, pathlib, sys
root = pathlib.Path(os.environ['MQTT_FIXTURE'])
args = sys.argv[1:]
(root / 'probe').write_text(json.dumps([args[args.index('-t') + 1], args[args.index('-m') + 1]]))
""")
        self.command("docker", f"""
import json, os, pathlib, sys
root = pathlib.Path(os.environ['MQTT_FIXTURE'])
args = sys.argv[1:]
with (root / 'docker-calls').open('a') as out:
    out.write(json.dumps(args) + '\\n')
if args[0] == 'run':
    pathlib.Path(args[args.index('--cidfile') + 1]).write_text('{CONTAINER_ID}\\n')
    if os.environ.get('MQTT_FIXTURE_DOCKER_FAIL'):
        print('cannot bind published port', file=sys.stderr)
        raise SystemExit(125)
    (root / 'port-open').touch()
    print('{CONTAINER_ID}')
elif args[0] == 'logs':
    print('broker lifecycle diagnostic')
elif args[0] == 'rm' and os.environ.get('MQTT_FIXTURE_CLEANUP_FAIL'):
    raise SystemExit(1)
""")

    def command(self, name, body):
        path = self.bin / name
        path.write_text(f"#!{sys.executable}\n" + body)
        path.chmod(0o755)

    def run_shell(self, body, **env):
        script = f"""
source {shlex.quote(str(ROOT / 'rustfs-tier-test.sh'))}
init_artifacts
{body}
"""
        return subprocess.run(
            ["bash", "-c", script], env={**self.env, **env}, text=True,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=25,
        )

    def docker_calls(self):
        path = self.path / "docker-calls"
        return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []

    def assert_subscriber_stopped(self):
        pid_file = self.path / "subscriber-pid"
        if pid_file.exists():
            with self.assertRaises(ProcessLookupError):
                os.kill(int(pid_file.read_text()), 0)

    def test_existing_broker_requires_delivery_and_is_not_removed(self):
        result = self.run_shell("mqtt_port_open() { return 0; }; mqtt_prepare")
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertIn("publish/subscribe readiness passed", result.stdout)
        self.assertEqual(self.docker_calls(), [])
        self.assertEqual((self.path / "tmp/rustfs-mqtt-events.jsonl").read_text(), "")
        self.assert_subscriber_stopped()

    def test_missing_external_broker_fails_before_subscriber_start(self):
        result = self.run_shell(
            "mqtt_port_open() { return 1; }; mqtt_prepare",
            RUSTFS_TIER_MQTT_MODE="external",
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("MQTT prerequisite unavailable", result.stdout)
        self.assertFalse((self.path / "subscriber-pid").exists())
        self.assertEqual(self.docker_calls(), [])

    def test_main_rejects_missing_dependency_before_node_setup(self):
        result = self.run_shell("""
need_cmd() { :; }
resolve_rc_binary() { RC_BIN=/bin/true; }
write_provenance() { :; }
verify_rc_digest() { :; }
mqtt_port_open() { return 1; }
env_setup() { touch "$MQTT_FIXTURE/node-was-modified"; exit 99; }
main --topology single-single -y --package-url https://example.invalid/rustfs.deb
""", RUSTFS_TIER_MQTT_MODE="external")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("MQTT prerequisite unavailable", result.stdout)
        self.assertFalse((self.path / "node-was-modified").exists())

    def test_local_missing_broker_is_owned_and_cleaned_by_id(self):
        result = self.run_shell('mqtt_port_open() { test -f "$MQTT_FIXTURE/port-open"; }; mqtt_prepare')
        self.assertEqual(result.returncode, 0, result.stdout)
        calls = self.docker_calls()
        start = next(call for call in calls if call[0] == "run")
        self.assertIn("127.0.0.1:1883:1883", start)
        self.assertIn("eclipse-mosquitto:2", start)
        self.assertEqual([call for call in calls if call[0] == "rm"], [["rm", "-f", CONTAINER_ID]])
        self.assertFalse((self.path / "artifacts/mqtt-container-id").exists())
        self.assertIn("broker lifecycle", (self.path / "artifacts/logs/mqtt-broker.log").read_text())
        self.assert_subscriber_stopped()

    def test_failed_container_start_retains_diagnostic_and_cleans_receipt(self):
        result = self.run_shell(
            "mqtt_port_open() { return 1; }; mqtt_prepare", MQTT_FIXTURE_DOCKER_FAIL="1",
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("MQTT broker startup failed", result.stdout)
        self.assertIn(["rm", "-f", CONTAINER_ID], self.docker_calls())
        self.assertIn("cannot bind", (self.path / "artifacts/logs/mqtt-broker-start.log").read_text())

    def test_term_cleans_only_owned_container_and_preserves_exit_status(self):
        result = self.run_shell('mqtt_port_open() { test -f "$MQTT_FIXTURE/port-open"; }; mqtt_prepare; kill -TERM $$')
        self.assertEqual(result.returncode, 143, result.stdout)
        self.assertEqual([call for call in self.docker_calls() if call[0] == "rm"], [["rm", "-f", CONTAINER_ID]])
        self.assert_subscriber_stopped()

    def test_invalid_container_receipt_is_never_used_for_deletion(self):
        result = self.run_shell('MQTT_BROKER_OWNED=1; printf "someone-elses-container" > "$MQTT_CIDFILE"')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("refusing cleanup", result.stdout)
        self.assertEqual(self.docker_calls(), [])

    def test_failed_cleanup_preserves_receipt_for_a_later_recovery(self):
        result = self.run_shell(
            'mqtt_port_open() { test -f "$MQTT_FIXTURE/port-open"; }; mqtt_prepare',
            MQTT_FIXTURE_CLEANUP_FAIL="1",
        )
        self.assertNotEqual(result.returncode, 0)
        receipt = self.path / "artifacts/mqtt-container-id"
        self.assertEqual(receipt.read_text().strip(), CONTAINER_ID)
        recovered = self.run_shell("MQTT_BROKER_OWNED=1; mqtt_broker_stop")
        self.assertEqual(recovered.returncode, 0, recovered.stdout)
        self.assertFalse(receipt.exists())
        self.assertEqual([call for call in self.docker_calls() if call[0] == "rm"], [["rm", "-f", CONTAINER_ID]] * 2)

    def test_subscriber_exit_does_not_report_started(self):
        result = self.run_shell("mqtt_port_open() { return 0; }; mqtt_prepare", MQTT_FIXTURE_MODE="exit")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("subscriber exited before readiness", result.stdout)
        self.assertNotIn("readiness passed", result.stdout)
        self.assertIn("Connection refused", (self.path / "artifacts/logs/mqtt-preflight.log").read_text())
        self.assert_subscriber_stopped()

    def test_live_subscriber_without_delivery_is_not_ready(self):
        result = self.run_shell(
            "sleep() { SECONDS=$((SECONDS + 15)); command sleep 0.02; }; mqtt_start",
            MQTT_FIXTURE_MODE="quiet",
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("readiness timed out", result.stdout)
        self.assertNotIn("readiness passed", result.stdout)
        self.assert_subscriber_stopped()

    def test_cleanup_preserves_unrelated_subscriber(self):
        other = subprocess.Popen(
            [str(self.bin / "mosquitto_sub")], env={**self.env, "MQTT_FIXTURE_MODE": "quiet"},
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        try:
            result = self.run_shell("mqtt_start; mqtt_stop; mqtt_stop")
            self.assertEqual(result.returncode, 0, result.stdout)
            self.assertIsNone(other.poll(), "cleanup terminated an unrelated subscriber")
        finally:
            other.terminate()
            other.wait(timeout=5)

    def test_remote_missing_broker_is_not_created_locally(self):
        result = self.run_shell("mqtt_port_open() { return 1; }; mqtt_prepare", RUSTFS_HEAL_IP="192.0.2.1")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("not a local bind address", result.stdout)
        self.assertEqual(self.docker_calls(), [])

    def test_vm_network_failure_stops_before_destructive_setup(self):
        self.command("ssh", "import sys; print('Connection timed out', file=sys.stderr); raise SystemExit(1)\n")
        result = self.run_shell('cleanup_node() { touch "$MQTT_FIXTURE/node-was-modified"; }; env_setup single-single')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("unreachable from", result.stdout)
        self.assertFalse((self.path / "node-was-modified").exists())
        self.assertIn("Connection timed out", next((self.path / "artifacts/logs").glob("mqtt-network-*.log")).read_text())

    def test_module_failure_diagnostics_are_retained_and_filtered(self):
        self.command("ssh", """
print('mqtt target state target_id=primary:mqtt state=connect_timeout')
print('Local notification runtime failed: primary:mqtt: initialization failed')
print('Local notification runtime failed secret=do-not-copy-this')
print('unrelated startup configuration do-not-copy-that')
""")
        result = self.run_shell("CURRENT_TOPOLOGY=single-single; capture_module_startup_diagnostics")
        self.assertEqual(result.returncode, 0, result.stdout)
        files = list((self.path / "artifacts/logs").glob("module-startup-*.log"))
        self.assertEqual(len(files), 2)
        for path in files:
            text = path.read_text()
            self.assertIn("state=connect_timeout", text)
            self.assertIn("primary:mqtt: initialization failed", text)
            self.assertNotIn("do-not-copy", text)


if __name__ == "__main__":
    unittest.main()
