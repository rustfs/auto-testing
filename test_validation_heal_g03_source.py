"""Frozen serial-ACK deletion order and physical latest authority; no live proof."""
import copy
import unittest
import uuid

from scripts.validation_heal import race
import test_validation_heal_race_physical as physical_fixtures


class VersionDeleteSourceTests(unittest.TestCase):
    bucket = 'rv-heal-0123456789abcdef'

    def scene(self, branch):
        before, _, _, _ = physical_fixtures.MarkerPhysicalTests().scene('batch-version-delete')
        order = [str(uuid.UUID(int=i)) for i in (900, 700, 800)]
        replacement = {str(uuid.UUID(int=i+1)): version for i, version in enumerate(order)}
        for value in before.values():
            for row in value['files']:
                for version in row.get('metadata', {}).get('versions', []):
                    version['versionId'] = replacement[version['versionId']]
        seed = [{'key':race.TARGET_KEY,'versionId':version,'isLatest':i==2,'deleteMarker':False}
                for i,version in enumerate(order)]
        mapped = race.map_fixture(before,self.bucket,seed)
        plan = race.operation_plan(self.bucket,branch,seed)
        selected = plan[0]['params']['VersionId']
        final = [copy.deepcopy(row) for row in seed if row['versionId']!=selected]
        for row in final:row['isLatest'] = row is final[-1]
        after = copy.deepcopy(before)
        selected_dir = mapped[race.TARGET_KEY,selected]['dataDir']
        for value in after.values():
            value['files'] = [row for row in value['files'] if selected_dir not in row['path']]
            for row in value['files']:
                metadata=row.get('metadata')
                if metadata:
                    metadata['versions']=[version for version in metadata['versions'] if version['versionId']!=selected]
                    for version in metadata['versions']:
                        version['isLatest'] = version['versionId']==final[-1]['versionId']
        baseline={'entries':sorted(copy.deepcopy(seed),key=lambda row:row['versionId'])}
        return before,after,mapped,baseline,plan,{'entries':sorted(final,key=lambda row:row['versionId'])}

    def test_deleted_current_promotes_last_retained_seed_ack_even_when_uuid_sort_disagrees(self):
        for branch in race.G03_BRANCHES:
            with self.subTest(branch=branch):
                before,after,mapped,baseline,plan,wanted=self.scene(branch)
                original=copy.deepcopy(baseline)
                actual=race.final_inventory(baseline,branch,plan,[{'VersionId':plan[0]['params']['VersionId']}])
                self.assertEqual(actual,wanted)
                self.assertEqual(baseline,original)
                self.assertEqual(plan[0]['seedVersionOrder'],[str(uuid.UUID(int=i)) for i in (900,700,800)])

    def test_real_physical_oracle_allows_only_ack_defined_latest_promotion(self):
        for branch in race.G03_BRANCHES:
            with self.subTest(branch=branch):
                before,after,mapped,baseline,plan,wanted=self.scene(branch)
                survivors,proof=race.physical_transition(before,after,self.bucket,mapped,wanted,branch)
                self.assertTrue(proof['physicalProofBeforeGETOrRescue'])
                self.assertEqual(len(survivors),2)

    def test_delete_manifest_rejects_missing_duplicate_or_foreign_ack_order(self):
        for fault in ('missing','duplicate','foreign','wrong-delete','wrong-ack'):
            with self.subTest(fault=fault):
                _,_,_,baseline,plan,_=self.scene(race.G03_BRANCHES[1])
                plan[0]['seedVersionOrder']=[str(uuid.UUID(int=i)) for i in (900,700,800)]
                response={'VersionId':plan[0]['params']['VersionId']}
                if fault=='missing':plan[0].pop('seedVersionOrder')
                elif fault=='duplicate':plan[0]['seedVersionOrder'][1]=plan[0]['seedVersionOrder'][0]
                elif fault=='foreign':plan[0]['seedVersionOrder'][1]=str(uuid.UUID(int=999))
                elif fault=='wrong-delete':plan[0]['params']['VersionId']=str(uuid.UUID(int=700));response['VersionId']=str(uuid.UUID(int=700))
                else:response['VersionId']=str(uuid.UUID(int=700))
                with self.assertRaises(ValueError):race.final_inventory(baseline,race.G03_BRANCHES[1],plan,[response])

    def test_retained_generation_payload_unknown_marker_or_latest_drift_is_refused(self):
        for fault in ('bytes','directory','latest','marker','duplicate'):
            with self.subTest(fault=fault):
                before,after,mapped,_,_,wanted=self.scene(race.G03_BRANCHES[1])
                value=after['node1']
                meta=next(row['metadata'] for row in value['files'] if row['path'].endswith('/xl.meta'))
                if fault=='bytes':next(row for row in value['files'] if row['path'].endswith('part.1'))['sha256']='changed'
                elif fault=='directory':meta['versions'][0]['dataDir']=str(uuid.UUID(int=999))
                elif fault=='latest':meta['versions'][0]['isLatest']=not meta['versions'][0]['isLatest']
                elif fault=='marker':meta['versions'].append({'versionId':str(uuid.UUID(int=999)),'isLatest':False,'deleted':True})
                else:meta['versions'].append(copy.deepcopy(meta['versions'][0]))
                with self.assertRaises(ValueError):race.physical_transition(before,after,self.bucket,mapped,wanted,race.G03_BRANCHES[1])
