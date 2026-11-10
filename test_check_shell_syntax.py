import json
from pathlib import Path
import subprocess
import tempfile
import unittest

from scripts.check_shell_syntax import check


class ShellSyntaxTests(unittest.TestCase):
    def test_sidecars_are_excluded_and_each_real_result_is_retained(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "._good.sh").write_bytes(b"\x00AppleDouble")
            (root / "a-bad.sh").write_text("if then\n")
            (root / "z-good.sh").write_text("echo good\n")
            report = root / "report.json"
            run = subprocess.run(["python3", str(Path(__file__).parent / "scripts/check_shell_syntax.py"),
                                  "--root", directory, "--report", str(report)], capture_output=True, text=True)
            self.assertNotEqual(run.returncode, 0)
            results = json.loads(report.read_text())
            self.assertFalse(results["passed"])
            self.assertTrue(results["complete"])
            self.assertEqual([r["file"] for r in results["results"]], ["a-bad.sh", "z-good.sh"])
            self.assertNotEqual(results["results"][0]["exit_code"], 0)
            self.assertEqual(results["results"][1]["exit_code"], 0)
            self.assertIn("syntax error", results["results"][0]["stderr"])

    def test_no_actual_scripts_cannot_certify_a_syntax_pass(self):
        with tempfile.TemporaryDirectory() as directory:
            self.assertFalse(check(Path(directory))["passed"])


if __name__ == "__main__":
    unittest.main()
