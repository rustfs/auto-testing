#!/usr/bin/env python3
"""Exercise the heal disk-usage monitor against bind-mounted data paths."""

import os
from pathlib import Path
import re
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parent


def shell_function(name):
    source = (ROOT / "rustfs_heal_test.sh").read_text()
    return re.search(rf"(?ms)^{name}\(\) \{{.*?^\}}", source).group(0)


class HealNodeUsageTests(unittest.TestCase):
    def test_sums_explicit_bind_mount_usage_and_propagates_probe_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            calls = Path(directory) / "calls"
            harness = r'''
set -Eeuo pipefail
DRY_RUN=0
SSH_OPTS=(-o BatchMode=yes)
SSH_USER=fixture
ssh() {
  printf '%s\n' "$*" > "$CALLS"
  if [ "${FAIL_PROBE:-0}" -eq 1 ]; then return 255; fi
  printf '34\n'
}
'''
            function = shell_function("node_used_gb")
            result = subprocess.run(
                ["bash", "-c", harness + function + '\nnode_used_gb fixture-node'],
                env={**os.environ, "CALLS": str(calls)},
                capture_output=True, text=True, timeout=10,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout.strip(), "34")
            remote_command = calls.read_text()
            for drive in range(1, 5):
                self.assertIn(f"/data/rustfs{drive}", remote_command)
            self.assertIn("--total", remote_command)

            failed = subprocess.run(
                ["bash", "-c", harness + function + '\nnode_used_gb fixture-node'],
                env={**os.environ, "CALLS": str(calls), "FAIL_PROBE": "1"},
                capture_output=True, text=True, timeout=10,
            )
            self.assertNotEqual(failed.returncode, 0)
            self.assertEqual(failed.stdout, "")


if __name__ == "__main__":
    unittest.main()
