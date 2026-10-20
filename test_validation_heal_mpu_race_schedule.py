"""Original G05 operation order and halt boundaries through the migrated model."""
import copy
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import test_validation_heal_mpu_race_fixture as model_fixtures
from scripts.validation_heal import mpu_race_fixture as model, mpu_race_schedule as schedule


class MultipartScheduleTests(unittest.TestCase):
    def test_each_branch_prepares_the_original_parts_before_foreground_and_keeps_both_final_versions(self):
        for branch in schedule.SELECTED:
            with self.subTest(branch=branch),tempfile.TemporaryDirectory() as tmp,patch.object(model,'PART_SIZES',(12,13,14)):
                recorder=model_fixtures.Recorder(Path(tmp)/'seed');recorder.output.mkdir()
                fixture=model.MpuRaceFixture(recorder,'rv-g05-owned');prepared=schedule.prepare(fixture,branch)
                self.assertEqual([len(prepared['manifest']['items'][r]['parts']) for r in model.ROLES],
                                 [3,2 if branch=='upload-continue' else 3,3])
                fixture.verify_s3(recorder)
                # Preparation and foreground use separate actual request sequences.
                recorder.sequence=0;recorder.output=Path(tmp)/'foreground';recorder.output.mkdir();recorder.calls=[]
                seen=[]
                def checkpoint(label,manifest):
                    seen.append(label)
                    self.assertEqual(manifest,fixture.manifest())
                    return {'checkpointLabel':label,'readOnlyMock':True}
                result=schedule.execute(fixture,branch,prepared,checkpoint)
                wanted=(['upload_part','complete_multipart_upload','abort_multipart_upload'] if branch=='upload-continue'
                        else ['complete_multipart_upload','abort_multipart_upload'] if branch=='complete'
                        else ['abort_multipart_upload','complete_multipart_upload'])
                self.assertEqual([c[0] for c in recorder.calls],wanted)
                self.assertEqual(seen,['continued','completed','aborted'] if branch=='upload-continue'
                                 else ['completed','aborted'] if branch=='complete' else ['aborted','completed'])
                self.assertEqual(result['selectedAckSequence'],1)
                self.assertEqual(result['selectedOperation'],wanted[0])
                self.assertFalse(result['ioCapRequested'] or result['physicalRepairProved'] or result['publicOverlapProved']
                                 or result['canonicalCasePassClaimed'])
                proof=fixture.verify_s3(recorder)
                self.assertEqual((proof['committedVersions'],proof['pendingUploads']),(2,0))

    def test_missing_or_unknown_mutation_checkpoint_stops_every_later_companion(self):
        for branch in schedule.SELECTED:
            for failure in ('checkpoint','sdk'):
                with self.subTest(branch=branch,failure=failure),tempfile.TemporaryDirectory() as tmp,patch.object(model,'PART_SIZES',(12,13,14)):
                    recorder=model_fixtures.Recorder(tmp);fixture=model.MpuRaceFixture(recorder,'rv-g05-owned')
                    prepared=schedule.prepare(fixture,branch);count=len(recorder.calls)
                    if failure=='sdk':recorder.fail=schedule.SELECTED[branch]
                    def failed_checkpoint(*args):raise ValueError('original physical witness unavailable')
                    with self.assertRaises(ValueError):schedule.execute(fixture,branch,prepared,failed_checkpoint)
                    self.assertEqual(len(recorder.calls),count+1)
                    self.assertIsNotNone(fixture.poisoned)
                    with self.assertRaises(ValueError):fixture.abort_b()
                    self.assertEqual(len(recorder.calls),count+1)

    def test_foreign_preparation_branch_or_changed_model_is_refused_before_foreground_sdk(self):
        for fault in ('branch','manifest','operation','callback'):
            with self.subTest(fault=fault),tempfile.TemporaryDirectory() as tmp,patch.object(model,'PART_SIZES',(12,13,14)):
                recorder=model_fixtures.Recorder(tmp);fixture=model.MpuRaceFixture(recorder,'rv-g05-owned')
                prepared=schedule.prepare(fixture,'complete');count=len(recorder.calls);callback=lambda *args:{}
                if fault=='branch':prepared['branch']='abort'
                elif fault=='manifest':prepared=copy.deepcopy(prepared);prepared['manifest']['items']['a']['uploadId']='foreign'
                elif fault=='operation':prepared['selectedOperation']='abort_multipart_upload'
                else:callback=None
                with self.assertRaises(ValueError):schedule.execute(fixture,'complete',prepared,callback)
                self.assertEqual(len(recorder.calls),count)
