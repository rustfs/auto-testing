from pathlib import Path
from subprocess import CompletedProcess
import tempfile
import unittest
from unittest.mock import call, patch

from scripts import musl_cpu_affinity as probe


class MuslCpuAffinityTests(unittest.TestCase):
    def test_selects_high_only_and_mixed_masks_of_equal_width(self):
        allowed = {10, 11, 12, 127, 128, 192, 193, 198, 199, 1024}
        self.assertEqual(probe.select_masks(allowed), {
            "high-only": {128, 192, 193, 198},
            "mixed": {10, 11, 128, 192},
        })

    def test_missing_high_or_low_cpus_fails_preflight(self):
        for allowed in ({0, 1, 128, 129, 130, 1024}, {127, 128, 129, 130, 131}):
            with self.subTest(allowed=allowed), self.assertRaisesRegex(RuntimeError, "requires at least two"):
                probe.select_masks(allowed)

    @patch.object(probe.subprocess, "run")
    def test_rejects_zero_matching_tests_and_nonzero_exit(self, run):
        for status, count in ((0, 0), (0, 11), (101, 1)):
            summary = f"test result: ok. {count} passed; 0 failed; 0 ignored;"
            with self.subTest(status=status, summary=summary):
                run.return_value = CompletedProcess([], status, stdout=summary)
                with self.assertRaisesRegex(RuntimeError, "did not pass exactly once"):
                    probe.run_ecstore_tests(Path("/fixture/ecstore-tests"), Path("/fixture"))

    @patch.object(probe.subprocess, "run")
    def test_runs_each_named_test_in_a_separate_process(self, run):
        run.return_value = CompletedProcess([], 0, stdout="test result: ok. 1 passed; 0 failed; 0 ignored;")
        probe.run_ecstore_tests(Path("/fixture/ecstore-tests"), Path("/fixture"))
        self.assertEqual([call.args[0][2] for call in run.call_args_list], list(probe.ECSTORE_TESTS))
        self.assertEqual(run.call_args.kwargs["env"]["RUST_MIN_STACK"], "4194304")

    @patch.object(probe.os, "sched_setaffinity", create=True)
    @patch.object(probe.os, "sched_getaffinity", create=True)
    @patch.object(probe, "local_rustfs", side_effect=RuntimeError("startup failed"))
    def test_restores_original_affinity_when_startup_fails(self, _local_rustfs, get_affinity, set_affinity):
        allowed = {4, 5, 128, 129, 130, 131}
        mask = {128, 129, 130, 131}
        get_affinity.side_effect = [allowed, mask]
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(RuntimeError, "high-only.*startup failed"):
                probe.exercise(Path("/fixture/rustfs"), Path("/fixture/ecstore-tests"), Path(directory))
        self.assertEqual(set_affinity.call_args_list, [call(0, mask), call(0, allowed)])


if __name__ == "__main__":
    unittest.main()
