import copy
from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts.validation_heal import nolock
from scripts.validation_heal.matrix import AUTOMATIC_CASES, select
import test_validation_heal_race_native as native_fixtures

HELPER = native_fixtures.HELPER


class NolockTests(unittest.TestCase):
    def test_source_specific_nolock_wire_true_and_effective_false_are_exact(self):
        for source in (nolock.REFERENCE['candidateCommit'],nolock.COPY_CANDIDATE):
            with self.subTest(source=source):
                ref = nolock.source_contract({'source_sha':source})
                self.assertEqual(ref['candidateCommit'],source)
                self.assertIs(ref['requested']['nolock'],True)
                self.assertIs(ref['effective']['nolock'],False)
                value = {'httpStatus':200,'response':{'summary':'running','settings':copy.deepcopy(ref['effective'])}}
                nolock.settings(value,ref)
                for field,wrong in [('nolock',True),('pool',0),('set',False),('recursive',1),('readRepair',True)]:
                    bad = copy.deepcopy(value);bad['response']['settings'][field] = wrong
                    with self.subTest(field=field),self.assertRaises(ValueError):nolock.settings(bad,ref)

    def test_exact_owner_group_admits_a09_only_and_never_executes_downloaded_native(self):
        candidate = {'source_sha':nolock.COPY_CANDIDATE,'package_sha256':'b'*64}
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            native_fixtures.RaceNativeTests().fixture(root,source=nolock.COPY_CANDIDATE)
            with patch('scripts.validation_heal.race_native.subprocess.run',side_effect=AssertionError('no executable')):
                proof = nolock.native_preflight(candidate,HELPER,root)
                self.assertEqual(proof['requiredRegressionLabels'],['default-heal-exclusive-owner'])
                self.assertTrue(proof['requiredNativeSubsetPassed'])
                self.assertFalse(proof['wholeCasePassClaimed'] or proof['canonicalCasePassClaimed'])
                self.assertIsNone(nolock.native_preflight({'source_sha':nolock.REFERENCE['candidateCommit']},HELPER,''))
                for source in ('e'*40,'bd0671ba781d8ebf509f4f03533228dfb44496f1'):
                    with self.subTest(source=source),self.assertRaises(ValueError):
                        nolock.native_preflight({**candidate,'source_sha':source},HELPER,root)
                with self.assertRaises(ValueError):nolock.native_preflight(candidate,HELPER,'')
                with self.assertRaises(ValueError):nolock.native_preflight(candidate,'f'*40,root)
            summary = root/'native-evidence/race-native-results.json'
            value = json.loads(summary.read_text())
            value['results'] = [r for r in value['results'] if r['label'] != 'default-heal-exclusive-owner']
            summary.write_text(json.dumps(value))
            with self.assertRaisesRegex(ValueError,'omit or duplicate'):nolock.native_preflight(candidate,HELPER,root)

    def test_real_preflight_cli_returns_before_fleet_creation(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp);native = root/'native';native.mkdir()
            native_fixtures.RaceNativeTests().fixture(native,source=nolock.COPY_CANDIDATE)
            candidate = root/'candidate.json'
            candidate.write_text(json.dumps({'source_sha':nolock.COPY_CANDIDATE,'package_sha256':'b'*64}))
            argv = ['nolock.py','preflight','--candidate',str(candidate),'--case-id',nolock.CASE_ID,'--testing-commit',HELPER]
            output = io.StringIO()
            with patch.object(nolock.sys,'argv',argv),patch.dict(nolock.os.environ,{'VALIDATION_HEAL_NATIVE_DIRECTORY':str(native)}),\
                 patch.object(nolock,'Fleet',side_effect=AssertionError('preflight must not create fleet')),redirect_stdout(output):
                nolock.main()
            proof = json.loads(output.getvalue())
            self.assertFalse(proof['fleetMutationRequested'] or proof['canonicalCasePassClaimed'])
            self.assertEqual(proof['nativeProof']['requiredRegressionLabels'],nolock.NATIVE_GROUP)

    def test_real_main_keeps_every_public_branch_and_stops_after_dirty_cleanup(self):
        for dirty in (False,True):
            with self.subTest(dirty=dirty),tempfile.TemporaryDirectory() as tmp:
                root=Path(tmp);native=root/'native';native.mkdir();evidence=root/'evidence';evidence.mkdir()
                native_fixtures.RaceNativeTests().fixture(native,source=nolock.COPY_CANDIDATE)
                candidate=root/'candidate.json'
                candidate.write_text(json.dumps({'source_sha':nolock.COPY_CANDIDATE,'package_sha256':'b'*64}))
                argv=['nolock.py','--candidate',str(candidate),'--rc-bin',str(root/'rc'),'--evidence',str(evidence),
                      '--tool-directory',str(root),'--case-id',nolock.CASE_ID,'--testing-commit',HELPER,'--binary-sha','c'*64]
                calls=[]
                def run(args,topology,nodes,branch):
                    calls.append((topology,len(nodes),branch))
                    return {'topology':topology,'branch':branch,'verdict':'FAIL' if dirty else 'PASS',
                            'cleanup':'FAIL' if dirty else 'PASS','canonicalCasePassClaimed':False}
                with patch.object(nolock.sys,'argv',argv),patch.object(nolock.signal,'signal'),\
                     patch.dict(nolock.os.environ,{'VALIDATION_HEAL_NATIVE_DIRECTORY':str(native),'GITHUB_RUN_ID':'12345',
                                                  'RUSTFS_NODES':'vm000 vm001 vm002 vm003'}),\
                     patch.object(nolock,'run_branch',side_effect=run),\
                     patch.object(nolock,'Fleet',side_effect=AssertionError('mock public branches do not create fleet')):
                    if dirty:
                        with self.assertRaisesRegex(ValueError,'coverage incomplete'):nolock.main()
                    else:nolock.main()
                summary=json.loads((evidence/'healthy-summary.json').read_text())
                self.assertIs(summary['complete'],not dirty)
                self.assertFalse(summary['canonicalCasePassClaimed'] or summary['nativePrerequisitesBlocked'])
                self.assertEqual(len(calls),1 if dirty else 4)
                if not dirty:self.assertEqual(calls,[(top,len(nodes),branch) for top,nodes in
                    [('H1',['vm000']),('H2',['vm000','vm001','vm002','vm003'])] for branch in nolock.BLACK_BOX])
                native_rows=[r for r in summary['results'] if r['branch']==nolock.BRANCHES[-1]]
                self.assertEqual(len(native_rows),2)
                self.assertTrue(all(r['nativeFenceProved'] and not r['fleetExecutionClaimed']
                                    and r['nativeProof']['requiredRegressionLabels']==nolock.NATIVE_GROUP for r in native_rows))

    def test_source_branches_and_fixture_mapping_are_preserved(self):
        recipe = select(nolock.CASE_ID)[0]
        self.assertEqual(recipe["variants"], list(nolock.BRANCHES))
        self.assertIn(nolock.CASE_ID, AUTOMATIC_CASES)
        self.assertEqual(nolock.REFERENCE["requested"]["nolock"], True)
        self.assertEqual(nolock.REFERENCE["effective"]["nolock"], False)
        self.assertIsNone(nolock.REFERENCE["requested"]["pool"])
        self.assertIsNone(nolock.REFERENCE["effective"]["set"])

    def test_settings_require_exact_lock_safe_normalization(self):
        status = {"httpStatus": 200, "response": {"summary": "running", "settings": nolock.REFERENCE["effective"]}}
        self.assertEqual(nolock.settings(status), nolock.REFERENCE["effective"])
        for changed in (
            {**nolock.REFERENCE["effective"], "nolock": True},
            {**nolock.REFERENCE["effective"], "recursive": False},
            {**nolock.REFERENCE["effective"], "pool": 0},
            {**nolock.REFERENCE["effective"], "set": False},
        ):
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                nolock.settings({"httpStatus": 200, "response": {"summary": "finished", "settings": changed}})

    def test_blocked_native_fence_is_kept_in_the_denominator(self):
        native = nolock.native_blocked({"source_sha": nolock.REFERENCE["candidateCommit"]}, "H1")
        rows = [
            {"topology": topology, "branch": branch, "verdict": "PASS", "cleanup": "PASS"}
            for topology in ("H1", "H2") for branch in nolock.BLACK_BOX
        ]
        self.assertFalse(nolock.complete_results([*rows, native]))
        self.assertFalse(nolock.complete_results(rows))
        self.assertFalse(nolock.complete_results([*rows[:-1], native]))
        failed = copy.deepcopy(rows)
        failed[0]["verdict"] = "FAIL"
        self.assertFalse(nolock.complete_results([*failed, native]))
        self.assertFalse(native["canonicalCasePassClaimed"])

    def test_run_case_dispatch_and_source_contract(self):
        dispatch = Path("scripts/validation_heal/run_case.sh").read_text()
        self.assertIn("heal.admin-nolock-safety) driver=nolock.py", dispatch)
        catalog = json.loads(Path("validation-cases.json").read_text())
        source = {case["id"]: case for module in catalog["modules"] for case in module["cases"]}[nolock.CASE_ID]
        self.assertEqual(source["probes"]["designCode"], "A09")
        self.assertEqual(source["probes"]["topology"], "H1,H2")

    def test_foreground_plan_keeps_three_overwrites_or_one_exact_delete(self):
        expected = [{"key": nolock.TARGET_KEY, "versionId": value, "isLatest": value == "v2",
                     "deleteMarker": False} for value in ("v0", "v1", "v2")]
        overwrites = nolock.operation_plan("rv-heal-0123456789abcdef", "overwrite-nolock", expected)
        self.assertEqual(len(overwrites), 3)
        self.assertEqual([step["params"]["Metadata"]["nonce"] for step in overwrites], ["0", "1", "2"])
        deletion = nolock.operation_plan("rv-heal-0123456789abcdef", "delete-nolock", expected)
        self.assertEqual(deletion[0]["params"]["VersionId"], "v0")

    def test_native_blocked_rows_cannot_become_a_canonical_pass(self):
        row = nolock.native_blocked({"source_sha": nolock.REFERENCE["candidateCommit"]}, "H2")
        self.assertEqual(row["verdict"], "BLOCKED")
        self.assertFalse(row["canonicalCasePassClaimed"])
        self.assertFalse(row["nativeFenceProved"])


if __name__ == "__main__":
    unittest.main()
