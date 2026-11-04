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
            root = Path(directory)
            calls = root / "calls"
            tools = root / "bin"
            tools.mkdir()
            fake_df = tools / "df"
            fake_df.write_text(r'''#!/usr/bin/env bash
printf 'Used\n'
printf '1342177280\n1610612736\n2147483648\n268435456\n5368709120\n'
[ "${FAIL_PROBE:-0}" -eq 0 ]
''')
            fake_df.chmod(0o755)
            harness = r'''
set -Eeuo pipefail
DRY_RUN=0
SSH_OPTS=(-o BatchMode=yes)
SSH_USER=fixture
PATH="$BIN:$PATH"
ssh() {
  local remote="${*: -1}"
  printf '%s\n' "${remote}" > "$CALLS"
  bash -c "${remote}"
}
'''
            function = shell_function("node_used_gb")
            result = subprocess.run(
                ["bash", "-c", harness + function + '\nnode_used_gb fixture-node'],
                env={**os.environ, "CALLS": str(calls), "BIN": str(tools)},
                capture_output=True, text=True, timeout=10,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout.strip(), "5")
            remote_command = calls.read_text()
            for drive in range(1, 5):
                self.assertIn(f"/data/rustfs{drive}", remote_command)
            self.assertIn("--total", remote_command)

            failed = subprocess.run(
                ["bash", "-c", harness + function + '\nnode_used_gb fixture-node'],
                env={**os.environ, "CALLS": str(calls), "BIN": str(tools), "FAIL_PROBE": "1"},
                capture_output=True, text=True, timeout=10,
            )
            self.assertNotEqual(failed.returncode, 0)
            self.assertEqual(failed.stdout, "")


if __name__ == "__main__":
    unittest.main()
