#!/usr/bin/env python3
"""Check that table-suite privileged commands are decided on the remote host."""

from pathlib import Path
import json
import os
import subprocess
import tempfile
import unittest

from scripts.prepare_table_smoke import prepare, validate


ROOT = Path(__file__).resolve().parent


class TableRemotePrivilegeTests(unittest.TestCase):
    def test_package_download_uses_data_backed_staging_path(self):
        script = (ROOT / "rustfs-table-test.sh").read_text()
        self.assertIn('pkg_dir="/data/rustfs1"', script)
        self.assertIn('pkg_tmp="\\${pkg_dir}/.rustfs-table-${RUN_ID}.deb"', script)
        self.assertIn("--retry-all-errors", script)

    def test_generated_config_does_not_escape_quotes(self):
        script = (
            f'source "{ROOT / "rustfs-table-test.sh"}"; '
            'ACCESS_KEY=test-access; SECRET_KEY=test-secret; '
            'rustfs_table_config'
        )
        result = subprocess.run(
            ["bash", "-c", script], capture_output=True, text=True, timeout=10,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('RUSTFS_ADDRESS=":9000"', result.stdout)
        self.assertIn(
            'RUSTFS_VOLUMES="http://rustfs-node{1...4}:9000/data/rustfs{1...4}/mnmd"',
            result.stdout,
        )
        self.assertNotIn('\\"', result.stdout)

    def test_remote_root_checks_uid_after_ssh(self):
        script = (
            f'source "{ROOT / "rustfs-table-test.sh"}"; '
            'ssh() { printf "%s" "$*"; }; '
            'remote_root vm000 </dev/null'
        )
        result = subprocess.run(
            ["bash", "-c", script], capture_output=True, text=True, timeout=10,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('if [ "$(id -u)" -eq 0 ]', result.stdout)
        self.assertIn("sudo -n bash -s", result.stdout)


class TableSmokePreparationTests(unittest.TestCase):
    def test_complete_product_tree_is_staged_and_bound_to_its_sha(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source"
            tree = source / "scripts/table-catalog"
            tree.mkdir(parents=True)
            (tree / "pyiceberg_smoke.py").write_text("import engine_compatibility\nimport failure_coverage\n")
            (tree / "engine_compatibility.py").write_text("VALUE = 1\n")
            (tree / "failure_coverage.py").write_text("VALUE = 2\n")
            (tree / "README.md").write_text("Supporting source documentation\n")
            for command in (["init", "-q"], ["add", "."],
                            ["-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm", "fixture"]):
                subprocess.run(["git", "-C", str(source), *command], check=True, capture_output=True)
            sha = subprocess.check_output(["git", "-C", str(source), "rev-parse", "HEAD"], text=True).strip()
            destination = root / "staged"
            entry = prepare(source, destination, sha)
            self.assertEqual(len(validate(entry)), 3)
            self.assertTrue((destination / "README.md").is_file())
            manifest = json.loads((destination / "source-manifest.json").read_text())
            self.assertEqual(manifest["source_sha"], sha)
            self.assertEqual(len(manifest["files"]), 4)
            with self.assertRaises(ValueError):
                prepare(source, root / "wrong", "f" * 40)
            self.assertFalse((root / "wrong").exists())
            (tree / "engine_compatibility.py").write_text("VALUE = 99\n")
            with self.assertRaisesRegex(ValueError, "uncommitted"):
                prepare(source, root / "dirty", sha)
            self.assertFalse((root / "dirty").exists())

    def test_missing_module_fails_before_venv_or_s3_actions(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            entry = root / "pyiceberg_smoke.py"
            entry.write_text("import engine_compatibility\n")
            command = f'''source "{ROOT / 'rustfs-table-test.sh'}"
SMOKE_SCRIPT="$SMOKE_FIXTURE"
python3() {{
  if [ "${{1:-}}" = -m ]; then echo "unexpected-venv"; return 99; fi
  command python3 "$@"
}}
test_tbl101
'''
            result = subprocess.run(["bash", "-c", command], capture_output=True, text=True,
                                    env={**os.environ, "SMOKE_FIXTURE": str(entry)}, timeout=10)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("[HARNESS-ERROR]", result.stderr)
            self.assertIn("engine_compatibility.py", result.stderr)
            self.assertNotIn("unexpected-venv", result.stdout)


if __name__ == "__main__":
    unittest.main()
