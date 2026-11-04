#!/usr/bin/env python3
"""Check that table-suite privileged commands are decided on the remote host."""

from pathlib import Path
import subprocess
import unittest


ROOT = Path(__file__).resolve().parent


class TableRemotePrivilegeTests(unittest.TestCase):
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
