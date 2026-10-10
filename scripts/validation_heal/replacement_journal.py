"""Read the exact candidate's run-owned replacement generation records."""

import hashlib
import json
import os
import stat
import uuid


REFERENCE = {"candidateCommit": "072b57b37559fbe626aeda8abf61617fdcbdd926", "sourceFiles": {
    "crates/heal/src/heal/resume.rs": "ed55baa03fafa79bf662393eafcf53b4f7752ca8b2f77824cc6deff085a0cf1d",
    "crates/heal/src/heal/resume/replacement.rs": "d44d4b688a12fd09c4c9c48067e0f3dd024daf429ad4d4eeb63d4783bb6a143a"},
    "resumeSchema": 7, "commitSeal": "sealed", "role": "read-only generation witness; no replacement record synthesis"}
FILES = {"intent": ("ahm_replacement_intent.json", 1024**2),
         "seal": ("ahm_replacement_intent_seal", 32),
         "proof": ("ahm_replacement_completion_proof.json", 1024**2)}


def require(value, message):
    if not value: raise ValueError(message)


def read_records(roots, token, checked):
    require(isinstance(token, str) and str(uuid.UUID(token)) == token and uuid.UUID(token).int != 0,
            "replacement checkpoint requires a canonical nonzero generation")
    require(len(roots) == 4, "replacement checkpoint omitted an original physical root")

    def collect():
        rows = []
        for disk, root in enumerate(roots):
            data = root / "data"
            for kind, (suffix, limit) in FILES.items():
                relative = f".rustfs.sys/buckets/ahm-replacement/{token}_{suffix}"
                path = checked(data / relative, data)
                row = {"disk": disk, "kind": kind, "relativePath": relative, "present": False}
                try: fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
                except FileNotFoundError:
                    rows.append(row)
                    continue
                with os.fdopen(fd, "rb") as stream:
                    before = os.fstat(stream.fileno())
                    require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1 and before.st_size <= limit,
                            "replacement record exceeds its exact regular-file bound")
                    raw = stream.read(limit + 1)
                    after = os.fstat(stream.fileno())
                try: current = path.lstat()
                except FileNotFoundError: return None
                fields = ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns", "st_nlink")
                if len(raw) != before.st_size or any(getattr(before, field) != getattr(other, field)
                        for other in (after, current) for field in fields): return None
                row.update(present=True, size=len(raw), sha256=hashlib.sha256(raw).hexdigest())
                if kind == "seal":
                    require(raw == b"sealed", "replacement generation has no reviewed commit seal")
                    row["sealed"] = True
                else:
                    row["value"] = json.loads(raw)
                    require(isinstance(row["value"], dict), "replacement record is not a complete JSON object")
                rows.append(row)
        return rows

    for _ in range(3):
        first, second = collect(), collect()
        if first is not None and first == second:
            return {"taskId": token, "readOnly": True, "stable": True, "records": second}
    raise ValueError("replacement checkpoint did not stabilize inside its three-read budget")
