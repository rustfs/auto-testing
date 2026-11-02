#!/usr/bin/env python3
"""Object Lock oracles for S3C-602..605. No cluster and no live AWS calls."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parent
SCRIPT = ROOT / "rustfs-s3-compat-test.sh"
CAMPAIGN = ROOT / "scripts" / "campaign" / "run-worm-object-lock.sh"

HARNESS = r'''
source "$RUNNER"
HOLD=OFF
PUTS=0
V1_DELETED=0
STORED_V1=""
STORED_V2=""
aws_run() {
  printf '%s\n' "$*" >> "$CALL_LOG"
  local method="${2:-}" vid="" body="" bypass=0 arg prev="" dest="" src="" deny=0
  prev=""
  for arg in "$@"; do
    if [ "$prev" = --version-id ]; then vid="$arg"; fi
    if [ "$prev" = --body ]; then body="$arg"; fi
    if [ "$arg" = --bypass-governance-retention ]; then bypass=1; fi
    prev="$arg"
    dest="$arg"
  done
  AWS_CODE=0
  AWS_OUT='{}'
  case "$method" in
    create-bucket|put-object-lock-configuration|put-object-retention|delete-bucket) ;;
    put-object)
      [ -n "$body" ] && [ -f "$body" ] || { echo "put-object missing body" >&2; return 1; }
      PUTS=$((PUTS+1))
      if [ "$PUTS" -eq 1 ]; then
        STORED_V1="${TEST_TMP}/stored-v1"
        cp "$body" "$STORED_V1"
        if [ "$SCENARIO" = missing-version ]; then AWS_OUT='{}'; else AWS_OUT='{"VersionId":"v1"}'; fi
      else
        STORED_V2="${TEST_TMP}/stored-v2"
        cp "$body" "$STORED_V2"
        AWS_OUT='{"VersionId":"v2"}'
      fi
      ;;
    head-object)
      if [ "$SCENARIO" = retention-missing ]; then AWS_OUT='{}'
      else AWS_OUT='{"ObjectLockMode":"GOVERNANCE","ObjectLockRetainUntilDate":"2099-01-01T00:00:00Z"}'
      fi
      ;;
    get-object)
      [ "$vid" = v1 ] || { AWS_CODE=255; AWS_OUT='NoSuchVersion'; return 0; }
      src="$STORED_V1"
      if [ "$SCENARIO" = overwrite-replaces ] && [ "$PUTS" -ge 2 ]; then src="$STORED_V2"; fi
      [ -n "$src" ] && [ -f "$src" ] || { echo "stored body missing" >&2; return 1; }
      cp "$src" "$dest"
      ;;
    list-object-versions)
      if [ "$SCENARIO" = version-lost ] || [ "$V1_DELETED" -eq 1 ]; then
        AWS_OUT='{"Versions":[],"DeleteMarkers":[]}'
      else
        AWS_OUT='{"Versions":[{"Key":"f","VersionId":"v1","Size":512}],"DeleteMarkers":[{"Key":"f","VersionId":"marker"}]}'
      fi
      ;;
    delete-object)
      if [ -z "$vid" ]; then
        AWS_OUT='{"DeleteMarker":true,"VersionId":"marker"}'
      elif [ "$vid" = v1 ] && [ "$bypass" -eq 1 ]; then
        if [ "$CASE" = 604 ] && [ "$SCENARIO" != compliance-bypass-allowed ]; then
          AWS_CODE=255; AWS_OUT='AccessDenied'
        else
          V1_DELETED=1
        fi
      elif [ "$vid" = v1 ]; then
        deny=1
        if [ "$SCENARIO" = version-delete-allowed ] || [ "$SCENARIO" = hold-delete-allowed ]; then deny=0; fi
        if [ "$CASE" = 605 ] && [ "$HOLD" = OFF ]; then deny=0; fi
        if [ "$deny" -eq 1 ]; then AWS_CODE=255; AWS_OUT='AccessDenied'; else V1_DELETED=1; fi
      fi
      ;;
    put-object-legal-hold)
      HOLD=ON
      [[ "$*" == *Status=OFF* ]] && HOLD=OFF
      ;;
    get-object-legal-hold)
      if [ "$SCENARIO" = hold-not-on ]; then AWS_OUT='{"LegalHold":{"Status":"OFF"}}'
      else AWS_OUT="$(printf '{"LegalHold":{"Status":"%s"}}' "$HOLD")"
      fi
      ;;
    *) echo "unexpected AWS operation $method" >&2; return 1 ;;
  esac
}
if "test_s3c${CASE}"; then exit 0; else exit 1; fi
'''


def aws_lines(calls):
    return [line for line in calls.splitlines() if line.startswith("s3api ")]


def operation(line):
    return line.split()[1]


class WormObjectLockOracleTest(unittest.TestCase):
    def run_case(self, case, scenario):
        with tempfile.TemporaryDirectory() as directory:
            call_log = Path(directory, "calls")
            result = subprocess.run(
                ["bash", "-c", HARNESS],
                env={**os.environ, "RUNNER": str(SCRIPT), "CASE": str(case),
                     "SCENARIO": scenario, "CALL_LOG": str(call_log)},
                capture_output=True, text=True, timeout=60,
            )
            calls = call_log.read_text() if call_log.exists() else ""
            return result, calls

    def assert_case(self, case, scenario, success):
        result, calls = self.run_case(case, scenario)
        detail = result.stdout + result.stderr
        if success:
            self.assertEqual(result.returncode, 0, detail)
        else:
            self.assertNotEqual(result.returncode, 0, detail)
        return calls

    def test_protected_versions_pass(self):
        for case in (602, 603, 604, 605):
            with self.subTest(case=case):
                self.assert_case(case, "success", True)

    def test_metadata_alone_is_not_enough(self):
        for case in (602, 603, 604, 605):
            for scenario in ("version-delete-allowed", "overwrite-replaces"):
                with self.subTest(case=case, scenario=scenario):
                    self.assert_case(case, scenario, False)
        self.assert_case(602, "retention-missing", False)
        self.assert_case(602, "missing-version", False)
        self.assert_case(603, "version-lost", False)
        self.assert_case(604, "compliance-bypass-allowed", False)
        self.assert_case(605, "hold-delete-allowed", False)
        self.assert_case(605, "hold-not-on", False)

    def test_default_retention_is_set_before_the_object_is_written(self):
        calls = self.assert_case(602, "success", True)
        lines = aws_lines(calls)
        names = [operation(line) for line in lines]
        config = names.index("put-object-lock-configuration")
        puts = [index for index, name in enumerate(names) if name == "put-object"]
        self.assertGreaterEqual(len(puts), 2)
        self.assertLess(config, puts[0])
        bare = [index for index, line in enumerate(lines)
                if operation(line) == "delete-object" and "--version-id v1" in line
                and "--bypass-governance-retention" not in line]
        bypass = [index for index, line in enumerate(lines)
                  if operation(line) == "delete-object" and "--version-id v1" in line
                  and "--bypass-governance-retention" in line]
        self.assertTrue(bare)
        self.assertTrue(bypass)
        self.assertLess(bare[0], bypass[0])

    def test_governance_retention_is_attached_before_probes(self):
        calls = self.assert_case(603, "success", True)
        lines = aws_lines(calls)
        retention = next(index for index, line in enumerate(lines)
                         if operation(line) == "put-object-retention")
        puts = [index for index, line in enumerate(lines) if operation(line) == "put-object"]
        deletes = [index for index, line in enumerate(lines)
                   if operation(line) == "delete-object" and "--version-id v1" in line]
        self.assertIn("--version-id v1", lines[retention])
        self.assertIn("GOVERNANCE", lines[retention])
        self.assertLess(puts[0], retention)
        self.assertLess(retention, puts[1])
        self.assertLess(retention, deletes[0])

    def test_compliance_bypass_is_attempted_and_not_a_cleanup_path(self):
        calls = self.assert_case(604, "success", True)
        lines = aws_lines(calls)
        bypass = [line for line in lines if "--bypass-governance-retention" in line and "--version-id v1" in line]
        self.assertEqual(len(bypass), 1)
        self.assertIn("COMPLIANCE", next(line for line in lines if operation(line) == "put-object-retention"))

    def test_legal_hold_blocks_delete_until_it_is_cleared(self):
        calls = self.assert_case(605, "success", True)
        lines = aws_lines(calls)
        holds = [index for index, line in enumerate(lines) if operation(line) == "put-object-legal-hold"]
        self.assertGreaterEqual(len(holds), 2)
        self.assertIn("Status=ON", lines[holds[0]])
        self.assertIn("Status=OFF", lines[holds[1]])
        deletes = [index for index, line in enumerate(lines)
                   if operation(line) == "delete-object" and "--version-id v1" in line]
        self.assertTrue(any(holds[0] < index < holds[1] for index in deletes))
        self.assertTrue(any(index > holds[1] for index in deletes))


class WormCampaignEntryTest(unittest.TestCase):
    def run_suite(self, *args):
        return subprocess.run(
            ["bash", str(SCRIPT), *args],
            cwd=ROOT, capture_output=True, text=True, timeout=60,
        )

    def dry_run_ids(self, output):
        return [line.strip() for line in output.splitlines() if "[DRY-RUN]" in line]

    def test_suite_flag_schedules_only_the_deep_cases(self):
        result = self.run_suite("--suite", "worm", "--topology", "single-single", "--dry-run", "-y")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        scheduled = self.dry_run_ids(result.stdout)
        self.assertEqual(scheduled, [
            "[DRY-RUN] S3C-602 DefaultRetention",
            "[DRY-RUN] S3C-603 GovernanceProtection",
            "[DRY-RUN] S3C-604 ComplianceProtection",
            "[DRY-RUN] S3C-605 LegalHold",
        ])

    def test_case_filter_and_unknown_selection(self):
        selected = self.run_suite("--cases", "S3C-602,S3C-605", "--topology", "single-single", "--dry-run", "-y")
        self.assertEqual(selected.returncode, 0, selected.stdout + selected.stderr)
        self.assertEqual(self.dry_run_ids(selected.stdout), [
            "[DRY-RUN] S3C-602 DefaultRetention",
            "[DRY-RUN] S3C-605 LegalHold",
        ])
        unknown = self.run_suite("--cases", "S3C-999", "--topology", "single-single", "--dry-run", "-y")
        self.assertNotEqual(unknown.returncode, 0, unknown.stdout + unknown.stderr)
        self.assertIn("no tests matched", unknown.stdout + unknown.stderr)
        both = self.run_suite("--suite", "worm", "--cases", "S3C-602", "--topology", "single-single", "--dry-run", "-y")
        self.assertNotEqual(both.returncode, 0)
        bad_suite = self.run_suite("--suite", "shallow", "--topology", "single-single", "--dry-run", "-y")
        self.assertNotEqual(bad_suite.returncode, 0)
        mixed = self.run_suite(
            "--validation-case", "s3.basic-object-round-trip", "--suite", "worm", "--dry-run", "-y")
        self.assertNotEqual(mixed.returncode, 0)

    def test_campaign_harness_lists_the_same_four_cases(self):
        result = subprocess.run(
            ["bash", str(CAMPAIGN), "--dry-run", "--endpoint", "http://127.0.0.1:9"],
            cwd=ROOT, capture_output=True, text=True, timeout=60,
        )
        detail = result.stdout + result.stderr
        self.assertEqual(result.returncode, 0, detail)
        self.assertEqual(self.dry_run_ids(result.stdout), [
            "[DRY-RUN] S3C-602 DefaultRetention",
            "[DRY-RUN] S3C-603 GovernanceProtection",
            "[DRY-RUN] S3C-604 ComplianceProtection",
            "[DRY-RUN] S3C-605 LegalHold",
        ])
        self.assertNotIn("environment setup", detail)
        self.assertNotIn("ssh ", detail)
        self.assertIn("S3C-602 DefaultRetention", detail)
        missing = subprocess.run(["bash", str(CAMPAIGN)], cwd=ROOT, capture_output=True, text=True, timeout=30)
        self.assertNotEqual(missing.returncode, 0)
        self.assertIn("missing --endpoint", missing.stderr)


if __name__ == "__main__":
    unittest.main()
