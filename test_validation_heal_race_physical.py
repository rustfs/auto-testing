"""Exercise the real G04 physical transition oracle with decoded disk scenes."""
import copy
import unittest
import uuid

import test_validation_heal as physical_fixtures
from scripts.validation_heal import race


class MarkerPhysicalTests(unittest.TestCase):
    bucket = "rv-heal-0123456789abcdef"

    def scene(self, branch):
        before = physical_fixtures.HealShardOracleTest().snapshot()
        count = 1 if branch == "unversioned-delete" else 3
        expected = [{"key": race.TARGET_KEY, "versionId": "null" if count == 1 else str(uuid.UUID(int=i + 1)),
                     "isLatest": i == count - 1, "deleteMarker": False} for i in range(count)]
        for value in before.values():
            original = value["files"]
            value["files"] = []
            for meta, part in zip(original[::3], original[1::3], strict=True):
                meta = copy.deepcopy(meta)
                meta["path"] = f"{self.bucket}/{race.TARGET_KEY}/xl.meta"
                metadata = meta["metadata"]
                metadata["key"] = race.TARGET_KEY
                template = metadata["versions"][0]
                metadata["versions"] = []
                for i, row in enumerate(expected):
                    version = copy.deepcopy(template)
                    version.update(versionId=None if count == 1 else row["versionId"],
                                   dataDir=str(uuid.UUID(int=100 + i)), isLatest=row["isLatest"])
                    metadata["versions"].append(version)
                    value["files"].append({**part, "path": f"{self.bucket}/{race.TARGET_KEY}/{version['dataDir']}/part.1",
                                           "sha256": part["sha256"] + f"-version-{i}"})
                value["files"].append(meta)
        marker = {"key": race.TARGET_KEY, "versionId": str(uuid.UUID(int=99)),
                  "isLatest": True, "deleteMarker": True}
        if branch == "delete-marker":
            for row in expected: row["isLatest"] = False
            expected.append(marker)
            for value in before.values():
                for f in value["files"]:
                    if not f["path"].endswith("xl.meta"): continue
                    for v in f["metadata"]["versions"]: v["isLatest"] = False
                    f["metadata"]["versions"].append({"versionId": marker["versionId"], "deleted": True, "isLatest": True})
        mapped = race.map_fixture(before, self.bucket, expected)
        final = copy.deepcopy(expected)
        if branch == "create-marker":
            for row in final: row["isLatest"] = False
            final.append(marker)
        elif branch == "delete-marker":
            final.pop()
            final[-1]["isLatest"] = True
        elif branch == "batch-version-delete": final = final[-1:]
        else: final = []
        after = copy.deepcopy(before)
        retained = {r["versionId"] for r in final if not r["deleteMarker"]}
        keep_dirs = {v["dataDir"] for v in mapped.values() if v["versionId"] in retained}
        for value in after.values():
            if not final:
                value["files"] = []
                continue
            value["files"] = [f for f in value["files"] if f["path"].endswith("xl.meta")
                              or f["path"].rsplit("/", 2)[1] in keep_dirs]
            for f in value["files"]:
                if not f["path"].endswith("xl.meta"): continue
                old = {v["versionId"] or "null": v for v in f["metadata"]["versions"]}
                f["metadata"]["versions"] = [
                    {**old[r["versionId"]], "isLatest": r["isLatest"]} if not r["deleteMarker"] else
                    {"versionId": r["versionId"], "isLatest": r["isLatest"], "deleted": True}
                    for r in final]
        return before, after, mapped, {"entries": final}

    def test_real_g04_transition_accepts_markers_batch_delete_and_unversioned_delete(self):
        for branch in race.G04_BRANCHES:
            with self.subTest(branch=branch):
                before, after, mapped, final = self.scene(branch)
                survivors, proof = race.physical_transition(before, after, self.bucket, mapped, final, branch)
                self.assertEqual(len(survivors), {"create-marker": 3, "delete-marker": 3,
                                                 "batch-version-delete": 1, "unversioned-delete": 0}[branch])
                self.assertTrue(proof["physicalProofBeforeGETOrRescue"])

    def test_g04_rejects_retained_part_drift_marker_drift_and_deleted_generation_resurrection(self):
        for branch in race.G04_BRANCHES[:3]:
            for fault in ("part", "data-dir", "duplicate-version", "unknown-marker", "marker-latest", "old-directory"):
                with self.subTest(branch=branch, fault=fault):
                    before, after, mapped, final = self.scene(branch)
                    value = after["node1"]
                    meta = next(f for f in value["files"] if f["path"].endswith("xl.meta"))
                    part = next(f for f in value["files"] if f["path"].endswith("part.1"))
                    if fault == "part": part["sha256"] = "changed"
                    elif fault == "data-dir": meta["metadata"]["versions"][0]["dataDir"] = str(uuid.UUID(int=999))
                    elif fault == "duplicate-version": meta["metadata"]["versions"].append(copy.deepcopy(meta["metadata"]["versions"][0]))
                    elif fault == "unknown-marker":
                        meta["metadata"]["versions"].append({"versionId": str(uuid.UUID(int=888)), "deleted": True, "isLatest": False})
                    elif fault == "marker-latest": meta["metadata"]["versions"][0]["isLatest"] = not meta["metadata"]["versions"][0]["isLatest"]
                    else:
                        value["files"].append({**part, "path": f"{self.bucket}/{race.TARGET_KEY}/{uuid.UUID(int=999)}/part.1"})
                    with self.assertRaises(ValueError): race.physical_transition(before, after, self.bucket, mapped, final, branch)


if __name__ == "__main__":
    unittest.main()
