#!/usr/bin/env python3
"""Contract tests for the read-only execution-environment preflight."""

import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location(
    "execution_environment", ROOT / "scripts/check_execution_environment.py",
)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class ExecutionEnvironmentTests(unittest.TestCase):
    def test_bash_major_version_is_enforced(self):
        version = mock.Mock(returncode=0, stdout="GNU bash, version 5.2.21(1)-release\n", stderr="")
        with mock.patch.object(MODULE.shutil, "which", return_value="/bin/bash"), \
             mock.patch.object(MODULE.subprocess, "run", return_value=version):
            self.assertEqual(MODULE.bash_version_check(4)["status"], "pass")
            self.assertEqual(MODULE.bash_version_check(6)["status"], "blocked")

    def test_aws_cli_parser_failure_is_reported_as_blocked(self):
        failed = mock.Mock(returncode=255, stdout="", stderr="badly formed help string\n")
        with mock.patch.object(MODULE.shutil, "which", return_value="/usr/bin/aws"), \
             mock.patch.object(MODULE.subprocess, "run", return_value=failed):
            check = MODULE.aws_list_objects_check()
        self.assertEqual(check["status"], "blocked")
        self.assertIn("compatible AWS CLI v2", check["detail"])

    def test_aws_cli_parser_success_is_accepted(self):
        passed = mock.Mock(returncode=0, stdout="{}", stderr="")
        with mock.patch.object(MODULE.shutil, "which", return_value="/usr/bin/aws"), \
             mock.patch.object(MODULE.subprocess, "run", return_value=passed):
            check = MODULE.aws_list_objects_check()
        self.assertEqual(check["status"], "pass")

    def test_drive_layout_reports_missing_and_shared_devices(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "rustfs"
            for index in range(1, 3):
                (Path(temporary) / f"rustfs{index}").mkdir()
            checks = MODULE.drive_layout_check(root, 3, 0, True)
        self.assertEqual([item["status"] for item in checks], ["pass", "blocked", "blocked"])
        self.assertIn("shares device", checks[1]["detail"])
        self.assertIn("missing", checks[2]["detail"])

    def test_port_check_detects_a_bound_port(self):
        with MODULE.socket.socket(MODULE.socket.AF_INET, MODULE.socket.SOCK_STREAM) as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
            check = MODULE.port_check(port, "127.0.0.1")
        self.assertEqual(check["status"], "blocked")


if __name__ == "__main__":
    unittest.main()
