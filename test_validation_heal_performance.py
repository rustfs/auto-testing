import copy
import hashlib
import json
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import patch

from scripts.validation_heal import performance as perf
from scripts.validation_heal import fleet_node as node
from scripts.validation_heal.matrix import select, AUTOMATIC_CASES


class PerformanceTests(unittest.TestCase):
    def metrics(self, epoch=1):
        return {name: {"identity": {field: name + field for field in node.PROCESS_FIELDS},
                "clockDomain": name + "bootId", "monotonicNs": epoch * 10**9,
                "cpuTicks": epoch * 10, "clockTicksPerSecond": 100, "rssBytes": epoch * 100,
                "fileDescriptors": 100, "io": {key: epoch * 100 for key in
                    ("rchar", "wchar", "syscr", "syscw", "read_bytes", "write_bytes")}, "readOnly": True}
                for name in ("n0", "n1", "n2", "n3")}

    def rows(self):
        return [{"sequence": index, "operation": "get_object" if index % 2 == 0 else "put_object",
                 "latencyMs": 10, "verdict": "PASS"} for index in range(120)]

    def phases(self):
        return [{**spec, "statistics": perf.summarize(self.rows()),
                 "resources": perf.resource_delta(self.metrics(), self.metrics(2)),
                 "repair": {"physicalVerified": True, "MiBPerSecond": 1} if spec["mode"] == "B" else None,
                 "overlap": {"bothOperationsDuringRealRepair": True}, "noHeal": spec["mode"] == "A"}
                for spec in perf.schedule()]

    def test_frozen_order_contains_every_source_repetition(self):
        self.assertEqual(select(perf.CASE_ID)[0]["variants"], list(perf.BRANCHES))
        self.assertIn(perf.CASE_ID, AUTOMATIC_CASES)
        self.assertEqual([phase["mode"] for phase in perf.schedule()], list("ABBAABBAABBA"))
        self.assertGreaterEqual(perf.PROTOCOL["faultLeaseMinutes"], 120)
        with self.assertRaises(ValueError): perf.schedule({**perf.PROTOCOL, "maxErrorRate": 1})

    def test_nearest_rank_percentiles_keep_the_slowest_request(self):
        samples = list(range(1, 101))
        self.assertEqual(perf.percentile(samples, .95), 95)
        self.assertEqual(perf.percentile(samples, .99), 99)
        for bad in ([], [float("nan")], [-1], [float("inf")]):
            with self.assertRaises(ValueError): perf.percentile(bad, .95)

    def test_failed_samples_stay_in_latency_and_error_denominators(self):
        rows = self.rows()
        for index in (1, 3): rows[index].update(verdict="FAIL", latencyMs=30000)
        value = perf.summarize(rows)
        self.assertEqual(value["put"]["requests"], 60)
        self.assertEqual(value["failedRequests"], 2)
        self.assertEqual(value["errorRate"], 2 / 120)
        self.assertEqual(value["put"]["p99Ms"], 30000)
        for bad in (rows[:-1], rows + [rows[0]], rows[:-1] + [rows[0]]):
            with self.assertRaises(ValueError): perf.summarize(bad)

    def test_transport_unknown_is_durable_before_summary_and_cannot_pass(self):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            probe = types.SimpleNamespace(s3=lambda *args: {"acknowledged": False, "resultUnknown": True,
                                          "clientCallInvoked": True, "error": {"type": "Timeout"}})
            with patch.object(perf.time, "monotonic", return_value=1), patch.object(perf.time, "monotonic_ns", return_value=10**9):
                row = perf.request(probe, "rv-heal-abcdef-load", 0, 1, root)
            self.assertEqual(row["verdict"], "FAIL")
            self.assertEqual(row["requestSha256"], hashlib.sha256((root / "00000-request.json").read_bytes()).hexdigest())
            self.assertTrue(json.loads((root / "00000-ack.json").read_text())["observation"]["resultUnknown"])

    def test_missed_admission_is_a_failed_sample_without_a_late_network_write(self):
        probe = types.SimpleNamespace(s3=lambda *args: self.fail("late request was sent"))
        with tempfile.TemporaryDirectory() as name, patch.object(perf.time, "monotonic", return_value=33), \
                patch.object(perf.time, "monotonic_ns", return_value=33 * 10**9):
            value = perf.request(probe, "rv-heal-abcdef-load", 1, 1, Path(name))
        self.assertEqual(value["verdict"], "FAIL")
        self.assertFalse(value["observation"]["clientCallInvoked"])

    def test_get_success_with_the_wrong_bytes_is_a_failure(self):
        observed = {"acknowledged": True, "resultUnknown": False, "response": {"ResponseMetadata": {"HTTPStatusCode": 200}},
                    "payload": {"size": 65536, "sha256": "wrong"}}
        with tempfile.TemporaryDirectory() as name, patch.object(perf.time, "monotonic", return_value=1), \
                patch.object(perf.time, "monotonic_ns", return_value=10**9):
            value = perf.request(types.SimpleNamespace(s3=lambda *args: observed), "rv-heal-abcdef-load", 0, 1, Path(name))
        self.assertEqual(value["verdict"], "FAIL")

    def test_resource_counters_require_all_peers_and_the_same_process_birth(self):
        start, end = self.metrics(), self.metrics(3)
        delta = perf.resource_delta(start, end)
        self.assertEqual(delta["n0"]["cpuCores"], .1)
        self.assertEqual(delta["n0"]["ioDelta"]["read_bytes"], 200)
        for kind in ("missing", "pid", "boot", "binary", "mount", "clock", "backwards", "io", "mutable"):
            with self.subTest(kind=kind):
                bad = copy.deepcopy(end)
                if kind == "missing": del bad["n1"]
                elif kind in {"pid", "binary", "mount"}:
                    key = {"pid": "pid", "binary": "binarySha256", "mount": "mountNamespace"}[kind]
                    bad["n1"]["identity"][key] = "other"
                elif kind == "boot": bad["n1"]["clockDomain"] = "another-boot"
                elif kind == "clock": bad["n1"]["clockTicksPerSecond"] = 1000
                elif kind == "backwards": bad["n1"]["monotonicNs"] = 0
                elif kind == "io": bad["n1"]["io"]["write_bytes"] = 99
                else: bad["n1"]["readOnly"] = False
                with self.assertRaises(ValueError): perf.resource_delta(start, bad)

    def windows(self):
        return [{"callStartedMonotonicNs": index * 3 * 10**9,
                 "callFinishedMonotonicNs": (index * 3 + .1) * 10**9,
                 "response": {"summary": "running", "outcome": {"execution": {"state": "running"},
                    "counters": {"processed": (index + 1) * 10, "healed": index, "unchanged": 10,
                    "skipped": 0, "failed": 0, "unknown": 0, "attemptFailures": 0, "overflowed": False}}}}
                for index in range(3)]

    def bracketed(self):
        return [{"operation": "get_object" if index % 2 == 0 else "put_object", "observation": {
                 "clientCallInvoked": True, "callStartedMonotonicNs": (index * .3 + .2) * 10**9,
                 "callFinishedMonotonicNs": (index * .3 + .3) * 10**9}} for index in range(4)]

    def test_true_partial_running_progress_must_bracket_both_workload_operations(self):
        self.assertEqual(perf.overlap(self.bracketed(), self.windows())["bracketedCalls"], 4)
        for kind in ("finished", "zero", "stalled", "regressed", "unknown", "put-only", "outside", "not-invoked", "few"):
            with self.subTest(kind=kind):
                rows, windows = self.bracketed(), self.windows()
                if kind == "finished":
                    for row in windows: row["response"]["summary"] = "finished"
                elif kind in {"zero", "stalled"}:
                    for row in windows: row["response"]["outcome"]["counters"]["processed"] = 0 if kind == "zero" else 10
                elif kind == "regressed": windows[-1]["response"]["outcome"]["counters"]["processed"] = 1
                elif kind == "unknown": windows[0]["response"]["outcome"]["counters"]["unknown"] = 1
                elif kind == "put-only":
                    for row in rows: row["operation"] = "put_object"
                elif kind == "outside":
                    for row in rows: row["observation"]["callFinishedMonotonicNs"] = 99 * 10**9
                elif kind == "not-invoked":
                    for row in rows: row["observation"]["clientCallInvoked"] = False
                else: rows.pop()
                with self.assertRaises(ValueError): perf.overlap(rows, windows)

    def test_independent_performance_gates_reject_functional_success_with_slow_or_failed_calls(self):
        self.assertEqual(len(perf.acceptance(self.phases())["pairedComparisons"]), 12)
        for kind in ("omit", "reorder", "error", "absolute", "paired", "no-physical", "no-overlap", "cpu", "rss", "fd", "baseline-heal"):
            with self.subTest(kind=kind):
                phases = self.phases()
                if kind == "omit": phases.pop()
                elif kind == "reorder": phases[0], phases[1] = phases[1], phases[0]
                elif kind == "error": phases[1]["statistics"]["errorRate"] = 1 / 120
                elif kind == "absolute": phases[1]["statistics"]["get"]["p99Ms"] = 11000
                elif kind == "paired": phases[1]["statistics"]["get"]["p99Ms"] = 100
                elif kind == "no-physical": phases[1]["repair"]["physicalVerified"] = False
                elif kind == "no-overlap": phases[1]["overlap"]["bothOperationsDuringRealRepair"] = False
                elif kind in {"cpu", "rss", "fd"}:
                    key = {"cpu": "cpuCores", "rss": "maxSampledRssBytes", "fd": "maxSampledFDs"}[kind]
                    phases[1]["resources"]["n2"][key] = 10**12
                else: phases[0]["noHeal"] = False
                with self.assertRaises(ValueError): perf.acceptance(phases)

    def test_phase_hardware_cannot_drift(self):
        snapshot = {node: {"formats": [{"value": "disk-" + node}], "hostname": node, "binarySha256": "candidate"}
                    for node in ("n0", "n1", "n2", "n3")}
        self.assertTrue(perf.same_hardware(snapshot, copy.deepcopy(snapshot)))
        for key in ("formats", "hostname", "binarySha256"):
            changed = copy.deepcopy(snapshot)
            changed["n2"][key] = "new"
            with self.assertRaises(ValueError): perf.same_hardware(snapshot, changed)

    def test_node_metrics_reject_path_or_pid_injection_before_reading_process(self):
        for field in ("pid", "path", "command", "expected"):
            with patch.object(node, "process", side_effect=AssertionError("must reject before reading")):
                with self.assertRaises(ValueError): node.process_metrics({"runId": "gh-1-1-h2", "action": "process-metrics", field: "/etc/shadow"})

    def test_node_metrics_attest_resources_and_reject_a_recycled_pid(self):
        identity = {name: name for name in node.PROCESS_FIELDS}
        identity.update(pid=123, startTicks=900)
        fields = ["0"] * 22
        fields[0], fields[11], fields[12], fields[17], fields[19], fields[20], fields[21] = "S", "100", "50", "12", "900", "9000", "5"
        proc = {"stat": "123 (rustfs worker) " + " ".join(fields),
                "io": "\n".join(name + ": 100" for name in ("rchar", "wchar", "syscr", "syscw", "read_bytes", "write_bytes", "cancelled_write_bytes"))}
        class Proc:
            def __init__(self, name=""): self.name = name
            def __truediv__(self, name): return Proc(name)
            def read_text(self): return proc[self.name]
            def iterdir(self): return iter(range(7))
        with patch.object(node, "Path", return_value=Proc()), patch.object(node, "process", return_value=identity), \
                patch.object(node, "owned", return_value=(None, None, {"runtimeProfile": "manual"})), \
                patch.object(node.os, "sysconf", side_effect=lambda name: 100 if name == "SC_CLK_TCK" else 4096):
            value = node.process_metrics({"runId": "gh-1-1-h2", "action": "process-metrics"})
            self.assertEqual(value["cpuTicks"], 150)
            self.assertEqual(value["rssBytes"], 5 * 4096)
            self.assertEqual(value["fileDescriptors"], 7)
            with patch.object(node, "process", side_effect=[identity, {**identity, "startTicks": 901}]):
                with self.assertRaises(ValueError): node.process_metrics({"runId": "gh-1-1-h2", "action": "process-metrics"})

    def test_exit_race_refreshes_systemd_and_never_infers_an_exit_from_proc_absence(self):
        request = {"runId": "gh-1-1-h2", "action": "process"}
        missing = FileNotFoundError(2, "process exited", "/proc/123/stat")
        terminal = {"pid": 0, "service": {"ExecMainCode": "1", "ExecMainStatus": "0", "Result": "success"}}
        with patch.object(node, "process_once", side_effect=[missing, terminal]) as reader, patch.object(node.time, "sleep"):
            self.assertEqual(node.process(request), terminal)
            self.assertEqual(reader.call_count, 2)
        with patch.object(node, "process_once", side_effect=missing), patch.object(node.time, "sleep"):
            with self.assertRaisesRegex(ValueError, "did not settle"): node.process(request)
        wrong_path = FileNotFoundError(2, "candidate missing", "/var/lib/rustfs-auto-testing/heal/rustfs")
        with patch.object(node, "process_once", side_effect=wrong_path) as reader:
            with self.assertRaises(FileNotFoundError): node.process(request)
            self.assertEqual(reader.call_count, 1)


if __name__ == "__main__": unittest.main()
