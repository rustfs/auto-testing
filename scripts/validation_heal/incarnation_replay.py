"""C11: archive and replay only original, candidate-owned recovery records.

The stopped adapter accepts identities and digests, never caller-supplied
replacement bytes. The archive survives every mutation and is exported before
the private namespace can be removed.
"""

import base64
import hashlib
import json
import os


LEGACY_MRF_PATHS = (".rustfs.sys/buckets/.heal/mrf/journal.bin",
                    ".rustfs.sys/buckets/.heal/mrf/journal-scoped.bin")


def require(condition, message):
    if not condition: raise ValueError(message)


def profile(value):
    return value in {"incarnation-checkpoint", "incarnation-mrf"}


def archive_name(relative):
    return "C11-replay-" + hashlib.sha256(relative.encode()).hexdigest() + ".original"


def identity(owner, request, journal):
    require(profile(owner.get("runtimeProfile")) and request.get("runId") == owner["runId"],
            "C11 replay requires its exact private recovery runtime")
    family = "checkpoint" if owner["runtimeProfile"] == "incarnation-checkpoint" else "mrf"
    require(request.get("family") == family, "C11 replay family differs from the bound runtime")
    incarnation = journal.task_id(request.get("bucketIncarnationId"))
    token = journal.task_id(request.get("taskId")) if family == "checkpoint" else None
    require(family == "checkpoint" or request.get("taskId") is None, "MRF cannot invent a root task binding")
    return {"runId": owner["runId"], "binarySha256": owner["binarySha256"], "bucket": owner["bucket"],
            "family": family, "taskId": token, "bucketIncarnationId": incarnation}


def paths(binding, journal):
    if binding["family"] == "mrf": return [*journal.names("mrf"), *LEGACY_MRF_PATHS]
    return [*journal.names("checkpoint", binding["taskId"]), *journal.names("root", binding["taskId"])]


def read_optional(fd, relative, fault, limit):
    try:
        with fault.parent(fd, ["data", *relative.split("/")[:-1]]) as directory:
            return fault.read_file(directory, relative.split("/")[-1], limit)
    except FileNotFoundError:
        return None


def census(roots, binding, journal, fault):
    rows = []
    for disk, root in enumerate(roots):
        with fault.directory(root) as fd:
            for relative in paths(binding, journal):
                raw = read_optional(fd, relative, fault, journal.LIMIT * 4)
                row = {"disk": disk, "relativePath": relative, "present": raw is not None}
                if raw is not None:
                    row.update(journal.entry(raw), rawBase64=base64.b64encode(raw).decode())
                rows.append(row)
    return rows


def capture(state, roots, owner, request, journal, fault, mrf, checked):
    require(owner.get("incarnationPhase", "capture") == "capture", "C11 cannot capture a later replay phase")
    binding = identity(owner, request, journal)
    rows = census(roots, binding, journal, fault)
    actual = [{name: row[name] for name in ("disk", "relativePath", "size", "sha256")}
              for row in rows if row["present"]]
    require(journal.same_json(actual, request.get("expectedRecords")),
            "C11 original stopped record census differs from its independently bound digests")
    if binding["family"] == "checkpoint":
        allowed = {journal.names("checkpoint", binding["taskId"])[0],
                   journal.names("checkpoint", binding["taskId"])[1],
                   journal.names("root", binding["taskId"])[0]}
        require(all(row["relativePath"] in allowed for row in actual),
                "C11 checkpoint capture includes a terminal, blocked or digest record")
        if actual:
            names = [row["relativePath"] for row in actual]
            require(len(names) == len(set(names)), "C11 checkpoint has duplicated physical record owners")
            require((journal.names("checkpoint", binding["taskId"])[0] in names)
                    == (journal.names("checkpoint", binding["taskId"])[1] in names),
                    "C11 original checkpoint/resume pair is incomplete")
        for row in rows:
            if not row["present"]: continue
            raw = base64.b64decode(row["rawBase64"], validate=True)
            value = journal.decode_json(raw)
            require(value.get("task_id") == binding["taskId"], "C11 captured a foreign original task")
            if row["relativePath"] == journal.names("checkpoint", binding["taskId"])[0]:
                value = journal.checkpoint_value(raw, binding["taskId"], binding["bucket"])
                require(value["admin"]["buckets"] == {binding["bucket"]: binding["bucketIncarnationId"]},
                        "C11 captured checkpoint belongs to another bucket generation")
            elif row["relativePath"] == journal.names("checkpoint", binding["taskId"])[1]:
                require(value.get("schema_version") == 7 and value.get("task_type") == "admin_erasure_set"
                        and value.get("set_disk_id") == "pool_0_set_0" and value.get("completed") is False,
                        "C11 captured companion is not a runnable Admin resume state")
            else:
                require(value.get("schema") == 3 and value.get("heal_type") == {
                    "type": "erasure_set", "buckets": [], "set_disk_id": "pool_0_set_0", "all_buckets": True},
                    "C11 captured root admission changed its original Admin erasure-set scope")
    else:
        committed = mrf.checkpoint(mrf.read_records(state, roots, owner, checked))
        require((not committed.get("present")) or committed.get("present") is True
                and all(item["bucket"] == binding["bucket"] and item["kind"] == 3
                        and item.get("lifecycle", {}).get("source_bucket_incarnation_id") == binding["bucketIncarnationId"]
                        and item["lifecycle"].get("state") == {"state": "active"}
                        and item["lifecycle"].get("last_operator_acceptance") is None for item in committed["intents"]),
                "C11 MRF archive lacks committed active responsibility for the exact old incarnation")
        digests = {item["intentDigest"] for item in committed["intents"]}
        for row in rows:
            if not row["present"] or not ("snapshot." in row["relativePath"] or row["relativePath"] in LEGACY_MRF_PATHS):
                continue
            raw = base64.b64decode(row["rawBase64"], validate=True)
            intents = mrf.decode_journal(raw)
            require(all(item["kind"] == 3 and item["intentDigest"] in digests for item in intents),
                    "C11 fallback or older snapshot contains an unbound original responsibility")
            require(row["relativePath"] != LEGACY_MRF_PATHS[0] or not intents,
                    "C11 scoped durable intents cannot appear in the v1 compatibility mirror")
    manifest = {**binding, "records": rows, "originalProductBytes": True, "replacementBytesAccepted": False}
    require(len(fault.encode(manifest)) <= journal.LIMIT * 32, "C11 original archive exceeds the bounded export limit")
    # Archive the entire local transaction before removing any runnable record.
    for row in rows:
        if not row["present"]: continue
        with fault.directory(roots[row["disk"]]) as fd, fault.parent(fd, ["quarantine"]) as directory:
            fault.new_file(directory, archive_name(row["relativePath"]), base64.b64decode(row["rawBase64"], validate=True))
    with fault.directory(state) as fd:
        fault.new_file(fd, "C11-replay.json", fault.encode(manifest))
    require(journal.same_json(census(roots, binding, journal, fault), rows),
            "C11 record changed after archiving; refusing partial replay detachment")
    for row in rows:
        if not row["present"]: continue
        with fault.directory(roots[row["disk"]]) as fd, fault.parent(
                fd, ["data", *row["relativePath"].split("/")[:-1]]) as directory:
            require(fault.read_file(directory, row["relativePath"].split("/")[-1], journal.LIMIT * 4)
                    == base64.b64decode(row["rawBase64"], validate=True), "C11 record drifted immediately before detachment")
            os.unlink(row["relativePath"].split("/")[-1], dir_fd=directory)
            os.fsync(directory)
    require(all(not row["present"] for row in census(roots, binding, journal, fault)),
            "C11 runnable record remains after complete detachment")
    with fault.directory(state) as fd:
        fault.new_file(fd, "C11-replay-detached.json", fault.encode(binding))
    return {**manifest, "detached": True, "checkpointCursorClaimed": False,
            "backupRescueUsed": False, "canonicalCasePassClaimed": False}


def archived(state, roots, owner, request, journal, fault):
    binding = identity(owner, request, journal)
    with fault.directory(state) as fd:
        manifest = journal.decode_json(fault.read_file(fd, "C11-replay.json", journal.LIMIT * 32))
        detached = journal.decode_json(fault.read_file(fd, "C11-replay-detached.json", 16384))
    require(journal.same_json(detached, binding) and all(journal.same_json(manifest.get(key), value)
            for key, value in binding.items()), "C11 archive changed its run/candidate/task/incarnation binding")
    require(set(manifest) == {*binding, "records", "originalProductBytes", "replacementBytesAccepted"}
            and manifest["originalProductBytes"] is True and manifest["replacementBytesAccepted"] is False,
            "C11 archive contains unknown or unproved provenance")
    expected = {(disk, relative) for disk in range(4) for relative in paths(binding, journal)}
    rows = manifest.get("records")
    require(isinstance(rows, list) and len(rows) == len(expected)
            and {(row.get("disk"), row.get("relativePath")) for row in rows} == expected,
            "C11 archive omitted or duplicated a physical record")
    for row in rows:
        require(type(row.get("present")) is bool and type(row.get("disk")) is int, "C11 archive record presence is unknown")
        require(set(row) == {"disk", "relativePath", "present", *(
            ("size", "sha256", "rawBase64") if row["present"] else ())}, "C11 archive record has unknown fields")
        if not row["present"]: continue
        with fault.directory(roots[row["disk"]]) as fd, fault.parent(fd, ["quarantine"]) as directory:
            raw = fault.read_file(directory, archive_name(row["relativePath"]), journal.LIMIT * 4)
        require(journal.entry(raw) == {key: row.get(key) for key in ("size", "sha256")}
                and raw == base64.b64decode(row["rawBase64"], validate=True), "C11 immutable original archive changed")
    return manifest


def install(state, roots, owner, request, journal, fault):
    manifest = archived(state, roots, owner, request, journal, fault)
    observed = census(roots, manifest, journal, fault)
    require(owner.get("incarnationPhase") == "transition", "C11 replay installation requires the transition phase")
    empty_mirrors = [row for row in observed if row["present"] and manifest["family"] == "mrf"
                     and row["relativePath"] in LEGACY_MRF_PATHS and row["size"] == 0]
    require(all(not row["present"] or row in empty_mirrors for row in observed),
            "C11 replay would overwrite product progress or an existing terminal/responsibility")
    # Healthy runtime flushes may publish empty fallback mirrors. They carry no
    # responsibility; preserve the read receipt and never discard nonempty work.
    for row in empty_mirrors:
        with fault.directory(roots[row["disk"]]) as fd, fault.parent(
                fd, ["data", *row["relativePath"].split("/")[:-1]]) as directory:
            require(fault.read_file(directory, row["relativePath"].split("/")[-1], journal.LIMIT) == b"",
                    "C11 transition mirror gained product responsibility before installation")
            os.unlink(row["relativePath"].split("/")[-1], dir_fd=directory)
            os.fsync(directory)
    for row in manifest["records"]:
        if not row["present"]: continue
        with fault.directory(roots[row["disk"]]) as fd, fault.parent(
                fd, ["data", *row["relativePath"].split("/")[:-1]]) as directory:
            fault.new_file(directory, row["relativePath"].split("/")[-1], base64.b64decode(row["rawBase64"], validate=True))
    require(journal.same_json(census(roots, manifest, journal, fault), manifest["records"]),
            "C11 replay installation differs from the original exact record set")
    with fault.directory(state) as fd:
        fault.new_file(fd, "C11-replay-installed.json", fault.encode({key: manifest[key] for key in
            ("runId", "binarySha256", "bucket", "family", "taskId", "bucketIncarnationId")}))
    return {**manifest, "installed": True, "originalArchiveRetained": True,
            "discardedEmptyTransitionJournals": empty_mirrors,
            "productRepairClaimed": False, "canonicalCasePassClaimed": False}


def finalize(state, roots, owner, request, journal, fault):
    manifest = archived(state, roots, owner, request, journal, fault)
    with fault.directory(state) as fd:
        installed = journal.decode_json(fault.read_file(fd, "C11-replay-installed.json", 16384))
    require(all(journal.same_json(installed.get(key), manifest[key]) for key in installed)
            and set(installed) == {"runId", "binarySha256", "bucket", "family", "taskId", "bucketIncarnationId"},
            "C11 replay installation is incomplete; preserve every private namespace")
    return {"originalArchive": manifest, "currentRecords": census(roots, manifest, journal, fault),
            "originalEvidenceExported": True, "namespaceFinalizationAllowed": True,
            "backupRescueUsed": False, "productRepairClaimed": False, "canonicalCasePassClaimed": False}
