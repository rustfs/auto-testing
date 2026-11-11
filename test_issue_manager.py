"""Exercise automatic issue resolution without accessing GitHub."""
from argparse import Namespace
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from scripts import issue_manager as manager


class ResolutionTests(unittest.TestCase):
    def setUp(self):
        self.args = Namespace(category="s3", suite="s3", run_id="123", run_url="https://example.test/run/123", dry_run=False)

    def test_empty_or_nonpassing_evidence_never_queries_or_closes_issues(self):
        values = [{}] + [{"S3C-101": {"statuses": statuses}} for statuses in
                       ({"PASS", "UNSUPPORTED"}, {"PASS", "SKIP"}, {"PASS", "RUNNING"}, {"PASS", "FAIL"}, set())]
        for cases in values:
            with self.subTest(cases=cases), mock.patch.object(manager, "search_open") as search, mock.patch.object(manager, "close_issue") as close:
                manager.handle_success(self.args, cases)
                search.assert_not_called()
                close.assert_not_called()

    def test_category_alone_does_not_authorize_closing_manual_issues(self):
        with mock.patch.object(manager, "search_open", return_value=[{"number": 1}]), \
             mock.patch.object(manager, "issue_labels", return_value={"s3"}), \
             mock.patch.object(manager, "close_issue") as close:
            manager.handle_success(self.args, {"S3C-101": {"statuses": {"PASS"}}})
        close.assert_not_called()

    def test_missing_digit_bearing_case_keeps_issue_open(self):
        with mock.patch.object(manager, "search_open", return_value=[{"number": 1}]), \
             mock.patch.object(manager, "issue_labels", return_value={"functional-test", "s3", "S3C-405"}), \
             mock.patch.object(manager, "close_issue") as close:
            manager.handle_success(self.args, {"S3C-101": {"statuses": {"PASS"}}})
        close.assert_not_called()

    def test_all_tracked_cases_pass_closes_only_managed_issue(self):
        with mock.patch.object(manager, "search_open", return_value=[{"number": 1}]), \
             mock.patch.object(manager, "issue_labels", return_value={"functional-test", "s3", "S3C-405"}), \
             mock.patch.object(manager, "close_issue") as close:
            manager.handle_success(self.args, {"S3C-405": {"statuses": {"PASS"}}})
        self.assertEqual(close.call_args.args[0], 1)
        self.assertIn("S3C-405", close.call_args.args[1])

    def test_real_parser_preserves_mixed_topology_statuses(self):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "cases.md"
            report.write_text("| S3C-405 | case (single) | PASS |\n| S3C-405 | case (multi) | UNSUPPORTED |\n")
            cases = manager.parse_cases(report, None)
        self.assertEqual(cases["S3C-405"]["statuses"], {"PASS", "UNSUPPORTED"})
        with mock.patch.object(manager, "close_issue") as close, mock.patch.object(manager, "search_open") as search:
            manager.handle_success(self.args, cases)
        search.assert_not_called()
        close.assert_not_called()

    def test_cancelled_or_skipped_runs_are_noop(self):
        for outcome in ("cancelled", "skipped"):
            self.assertEqual(manager.classify(outcome, {}, None)[0], "noop")

    def test_failed_run_with_only_passing_cases_is_harness_failure(self):
        for statuses in ({"PASS"}, {"PASS", "NOT_RUN"}, {"RUNNING"}):
            with self.subTest(statuses=statuses):
                cases = {"UPG-101": {"statuses": statuses}}
                self.assertEqual(manager.classify("failure", cases, None)[0], "env")


if __name__ == "__main__":
    unittest.main()
