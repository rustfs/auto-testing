#!/usr/bin/env python3
"""Check that table-suite privileged commands are decided on the remote host."""

from pathlib import Path
import subprocess
import unittest


ROOT = Path(__file__).resolve().parent


class TableRemotePrivilegeTests(unittest.TestCase):
    def test_package_download_uses_data_backed_staging_path(self):
        script = (ROOT / "rustfs-table-test.sh").read_text()
        self.assertIn('pkg_dir="/data/rustfs1"', script)
        self.assertIn(".rustfs-table-", script)
        self.assertIn("${RUN_ID}.deb", script)
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


if __name__ == "__main__":
    unittest.main()
