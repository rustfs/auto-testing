#!/usr/bin/env python3
"""Benchmark result contracts exercised with a local fake warp executable."""

import csv
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parent


class PerformanceResultTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.results = self.root / "results"
        self.shims = self.root / "fake-tools.sh"
        self.shims.write_text(r'''warp() {
  local method="$1" size="" objects=0 mode="${FAKE_WARP_MODE:-success}" omit="${FAKE_WARP_OMIT:-}"
  shift
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --obj.size) size="$2"; shift;;
      --objects) objects=1; shift;;
    esac
    shift
  done
  printf '%s %s\n' "$method" "$size" >> "$FAKE_WARP_CALLS"
  [[ "$method" == put && "$objects" == 0 || "$method" != put && "$objects" == 1 ]] || return 21
  printf 'diagnostic %s %s\n' "$method" "$size" >&2
  if [[ "$mode" == fail || "$mode" == mixed-failure && "$method" == put ]]; then return 42; fi
  if [[ "$mode" == hang ]]; then sleep 30; fi
  local sections=() section text
  case "$method" in
    get) sections=(GET);;
    put) sections=(PUT);;
    mixed) sections=(Total GET PUT);;
    *) return 2;;
  esac
  for section in "${sections[@]}"; do
    printf 'Report: %s. Concurrency: 64. Ran: 1s\n' "$section"
    if [[ "$mode" != missing-throughput ]]; then
      text=' * Average: 1.25 MiB/s, 1280.0 obj/s'
      printf '%s\n' "${text//"$omit"/}"
    fi
    if [[ "$mode" == missing-requests || "$method" == mixed && ( "$mode" == missing-mixed-put && "$section" == PUT || "$mode" == missing-mixed-get && "$section" == GET ) ]]; then continue; fi
    text=' * Reqs: Avg: 1.2ms, 50%: 1.1ms, 90%: 1.5ms, 99%: 2.1ms, Fastest: 1ms, Slowest: 3ms'
    printf '%s\n' "${text//"$omit"/}"
  done
  [[ "$mode" != failure-with-metrics ]] || return 42
  if [[ "$mode" == errors-with-success ]]; then
    printf '%s\n' 'warp: <ERROR> upload error: The service is unavailable. Please retry.'
  fi
  if [[ "$mode" == hang-after-report ]]; then sleep 30; fi
}
''')
        self.calls = self.root / "calls"
        self.env = dict(os.environ, BASH_ENV=str(self.shims), FAKE_WARP_CALLS=str(self.calls), WARP_RESULT_DIR=str(self.results), WARP_HOST="127.0.0.1:1", WARP_ACCESS_KEY="test-access", WARP_SECRET_KEY="test-secret", WARP_METHODS="get,put,mixed", WARP_SIZES="1KiB,4KiB", WARP_SLEEP_BETWEEN_ROUNDS="0", WARP_DURATION="1s")
        self.script = Path(__file__).with_name("rustfs_performance_testing.sh")

    def run_benchmark(self, mode="success", **overrides):
        result = subprocess.run(["bash", str(self.script)], cwd=self.root, env=dict(self.env, FAKE_WARP_MODE=mode, **overrides), capture_output=True, text=True)
        with (self.results / "summary.tsv").open() as stream:
            rows = list(csv.DictReader(stream, delimiter="\t"))
        return result, rows

    def assert_diagnostics(self, expected):
        self.assertEqual(len(self.calls.read_text().splitlines()), expected)
        outputs = list(self.results.glob("*.txt"))
        self.assertEqual(len(outputs), expected)
        for output in outputs:
            self.assertIn("diagnostic", output.read_text())
        self.assertTrue((self.results / "summary.md").is_file())

    def test_success_keeps_selected_matrix_and_optional_ttfb(self):
        result, rows = self.run_benchmark()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(rows), 6)
        self.assert_diagnostics(6)
        self.assertIn("parsed=6", (self.results / "master.log").read_text())

    def test_all_failed_rounds_return_failure_after_collecting_every_round(self):
        result, rows = self.run_benchmark("fail")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(rows, [])
        self.assert_diagnostics(6)
        self.assertEqual((self.results / "master.log").read_text().count("rc=42"), 6)
        self.assertIn("failed=6", (self.results / "summary.md").read_text())

    def test_failure_between_successes_remains_failed(self):
        result, rows = self.run_benchmark("mixed-failure")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual([row["method"] for row in rows], ["get", "get", "mixed", "mixed"])
        self.assert_diagnostics(6)
        self.assertIn("failed=2", (self.results / "summary.md").read_text())

    def test_failed_output_capture_is_not_a_successful_round(self):
        with self.shims.open("a") as shims:
            shims.write(f'\ntee() {{ "{shutil.which("tee")}" "$@" || return 1; case "$*" in *.txt) return 23;; esac; }}\n')
        result, rows = self.run_benchmark()
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(rows, [])
        self.assert_diagnostics(6)
        self.assertEqual((self.results / "master.log").read_text().count("capture_rc=23"), 6)

    def test_zero_exit_with_missing_metrics_is_not_a_result(self):
        for mode in ("missing-throughput", "missing-requests", "missing-mixed-get", "missing-mixed-put"):
            with self.subTest(mode=mode):
                self.calls.unlink(missing_ok=True)
                result, rows = self.run_benchmark(mode)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(len(rows), 4 if mode.startswith("missing-mixed-") else 0)
                self.assert_diagnostics(6)

    def test_stuck_round_is_hard_timed_out_and_fails_the_run(self):
        result, rows = self.run_benchmark(
            "hang", WARP_METHODS="get", WARP_SIZES="1KiB",
            WARP_ROUND_TIMEOUT="1", WARP_MAX_CONSECUTIVE_FAILURES="1",
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(rows, [])
        master = (self.results / "master.log").read_text()
        self.assertIn("rc=124", master)
        self.assertIn("ABORT consecutive_failures=1 limit=1", master)

    def round_evidence(self, filename="rounds.tsv"):
        with (self.results / filename).open() as stream:
            return list(csv.DictReader(stream, delimiter="\t"))

    def test_zero_exit_with_service_errors_retains_metrics_but_is_not_a_pass(self):
        result, rows = self.run_benchmark("errors-with-success", WARP_METHODS="get", WARP_SIZES="1KiB")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(rows, [])
        evidence = self.round_evidence()[0]
        self.assertEqual(evidence["warp_exit"], "0")
        self.assertEqual(evidence["verdict"], "FAIL")
        self.assertEqual(evidence["reason"], "logged_errors")
        self.assertEqual(evidence["error_lines"], "1")
        self.assertIn("Average:", (self.results / "get_1KiB.txt").read_text())

    def test_timeout_before_and_after_report_preserve_distinct_evidence(self):
        for mode, reason in (("hang", "timeout_before_report"), ("hang-after-report", "timeout_after_report")):
            with self.subTest(mode=mode):
                result, rows = self.run_benchmark(mode, WARP_METHODS="get", WARP_SIZES="1KiB,4KiB",
                                                  WARP_ROUND_TIMEOUT="1", WARP_MAX_CONSECUTIVE_FAILURES="1")
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(rows, [])
                evidence = self.round_evidence()
                self.assertEqual(evidence[0]["reason"], reason)
                self.assertEqual(evidence[0]["warp_exit"], "124")
                self.assertEqual(evidence[1]["verdict"], "NOT_RUN")

    def test_each_required_single_method_metric_is_checked(self):
        for value in ("1.25 MiB/s", "1280.0", "1.2ms", "1.1ms", "1.5ms", "2.1ms"):
            with self.subTest(missing=value):
                result, rows = self.run_benchmark(FAKE_WARP_OMIT=value, WARP_METHODS="get", WARP_SIZES="1KiB")
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(rows, [])

    def test_zero_selection_is_not_success(self):
        for selection in ({"WARP_METHODS": ", ,"}, {"WARP_SIZES": " , "}):
            with self.subTest(selection=selection):
                result, rows = self.run_benchmark(**selection)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(rows, [])
                self.assertFalse(self.calls.exists())
                self.assertIn("selected=0", (self.results / "summary.md").read_text())

    def test_one_round_and_default_full_matrix_are_valid_selections(self):
        result, rows = self.run_benchmark(WARP_METHODS="get", WARP_SIZES="1KiB")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(rows), 1)
        self.assertIn("- Methods: get; sizes: 1KiB\n", (self.results / "summary.md").read_text())
        self.calls.unlink()
        result, rows = self.run_benchmark(WARP_METHODS="all", WARP_SIZES="all")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(rows), 30)
        self.assert_diagnostics(30)
        self.assertIn("- Methods: get put mixed; sizes: ", (self.results / "summary.md").read_text())

    def test_parse_only_rejects_missing_or_incomplete_selected_results(self):
        result, _ = self.run_benchmark()
        self.assertEqual(result.returncode, 0, result.stderr)
        original = {path.name: path.read_bytes() for path in self.results.iterdir()}
        calls = self.calls.read_bytes()
        command = ["bash", str(self.script), "--parse-only", str(self.results)]
        parsed = subprocess.run(command, cwd=self.root, env=self.env, capture_output=True, text=True)
        self.assertEqual(parsed.returncode, 0, parsed.stderr)
        self.assertEqual(original, {name: (self.results / name).read_bytes() for name in original})
        self.assertEqual(self.calls.read_bytes(), calls)
        self.assertEqual(len(self.round_evidence("rounds.reparsed.tsv")), 6)
        self.assertIn("parsed=6", (self.results / "summary.reparsed.md").read_text())
        (self.results / "get_1KiB.txt").unlink()
        parsed = subprocess.run(command, cwd=self.root, env=self.env, capture_output=True, text=True)
        self.assertNotEqual(parsed.returncode, 0)
        self.assertIn("failed=1", (self.results / "summary.reparsed.md").read_text())

    def test_parse_only_preserves_aborted_execution_and_writes_separate_evidence(self):
        result, _ = self.run_benchmark("fail", WARP_MAX_CONSECUTIVE_FAILURES="1")
        self.assertNotEqual(result.returncode, 0)
        execution = self.round_evidence()
        self.assertEqual(execution[0]["warp_exit"], "42")
        self.assertTrue(execution[0]["elapsed_seconds"].isdigit())
        self.assertEqual(execution[1]["verdict"], "NOT_RUN")
        self.assertEqual(execution[1]["reason"], "aborted")
        original = {path.name: path.read_bytes() for path in self.results.iterdir()}
        calls = self.calls.read_bytes()

        parsed = subprocess.run(["bash", str(self.script), "--parse-only", str(self.results)],
                                cwd=self.root, env=self.env, capture_output=True, text=True)
        self.assertNotEqual(parsed.returncode, 0)
        self.assertEqual(original, {name: (self.results / name).read_bytes() for name in original})
        self.assertEqual(self.calls.read_bytes(), calls)
        reparsed = self.round_evidence("rounds.reparsed.tsv")
        self.assertEqual(len(reparsed), 6)
        self.assertEqual(reparsed[0]["warp_exit"], "42")
        self.assertEqual(reparsed[0]["elapsed_seconds"], "unknown")
        self.assertEqual(reparsed[1]["reason"], "missing_output")
        self.assertIn("failed=6", (self.results / "summary.reparsed.md").read_text())
        self.assertIn("PARSE_FAILED", (self.results / "master.reparsed.log").read_text())
        with (self.results / "summary.reparsed.tsv").open() as stream:
            self.assertEqual(list(csv.DictReader(stream, delimiter="\t")), [])

    def test_analysis_without_original_summary_uses_reparsed_report_and_propagates_failure(self):
        for mode, expected in (("success", 0), ("failure-with-metrics", 1)):
            with self.subTest(mode=mode):
                self.run_benchmark(mode)
                (self.results / "summary.md").unlink()
                original = {path.name: path.read_bytes() for path in self.results.iterdir()
                            if ".reparsed." not in path.name}
                calls = self.calls.read_bytes()
                environment = {**self.env, "RUSTFS_RESULT_DIR": str(self.results),
                               "RUSTFS_BENCH_SCRIPT": str(self.script),
                               "RUSTFS_PERF_SCRIPT_SOURCE_ONLY": "1",
                               "RUSTFS_WARP_METHODS": self.env["WARP_METHODS"],
                               "RUSTFS_WARP_SIZES": self.env["WARP_SIZES"]}
                environment.pop("WARP_METHODS")
                environment.pop("WARP_SIZES")
                parsed = subprocess.run(
                    ["bash", "-c", 'source "$1"; step6_analyze', "bash",
                     str(ROOT / "rustfs_performance_test.sh")],
                    cwd=self.root, env=environment,
                    capture_output=True, text=True, timeout=10,
                )
                self.assertEqual(parsed.returncode, expected, parsed.stdout + parsed.stderr)
                self.assertFalse((self.results / "summary.md").exists())
                self.assertEqual(original, {name: (self.results / name).read_bytes() for name in original})
                self.assertEqual(self.calls.read_bytes(), calls)
                if expected == 0:
                    self.assertIn("step 6 complete", parsed.stdout)
                else:
                    self.assertNotIn("step 6 complete", parsed.stdout)
                self.assertTrue((self.results / "summary.reparsed.md").is_file())

    def test_parse_only_cannot_certify_failed_or_unverified_metrics(self):
        result, rows = self.run_benchmark("failure-with-metrics")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(rows, [])
        command = ["bash", str(self.script), "--parse-only", str(self.results)]
        parsed = subprocess.run(command, cwd=self.root, env=self.env, capture_output=True, text=True)
        self.assertNotEqual(parsed.returncode, 0)
        self.assertIn("failed=6", (self.results / "summary.reparsed.md").read_text())
        result, _ = self.run_benchmark()
        self.assertEqual(result.returncode, 0, result.stderr)
        output = self.results / "get_1KiB.txt"
        original = output.read_text()
        for terminal in ("", "RustFS round status: warp=0 capture=", "RustFS round status: warp=0 capture=0\ntruncated"):
            with self.subTest(terminal=terminal):
                output.write_text(original.replace("RustFS round status: warp=0 capture=0\n", terminal))
                parsed = subprocess.run(command, cwd=self.root, env=self.env, capture_output=True, text=True)
                self.assertNotEqual(parsed.returncode, 0)
                self.assertIn("failed=1", (self.results / "summary.reparsed.md").read_text())


class PerformanceEnvironmentTests(unittest.TestCase):
    def test_help_does_not_require_home(self):
        environment = dict(os.environ)
        environment.pop("HOME", None)
        result = subprocess.run(
            ["bash", str(ROOT / "rustfs_performance_test.sh"), "--help"],
            env=environment, capture_output=True, text=True, timeout=10,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
