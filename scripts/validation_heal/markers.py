"""D04: genuine marker ACKs, missing metadata and same-drive stale transitions."""

import copy
import hashlib

try:
    from .heal_probe import Probe, payload, require
    from .heal_workload import verify_seed
    from .healthy import physical_content
    from .shards import files
except ImportError:
    from heal_probe import Probe, payload, require
    from heal_workload import verify_seed
    from healthy import physical_content
    from shards import files


BRANCHES = ("current-marker", "historical-marker", "missing-marker-replica", "stale-disk-marker")
INSTANCES = (
    {"name": "current-marker", "sourceBranch": "current-marker", "mode": "current", "role": "data"},
    {"name": "historical-marker", "sourceBranch": "historical-marker", "mode": "historical", "role": "parity"},
    {"name": "missing-current-data", "sourceBranch": "missing-marker-replica", "mode": "current", "role": "data"},
    {"name": "missing-historical-parity", "sourceBranch": "missing-marker-replica", "mode": "historical", "role": "parity"},
    {"name": "stale-create-current-marker", "sourceBranch": "stale-disk-marker", "mode": "create", "role": "data"},
    {"name": "stale-delete-current-marker", "sourceBranch": "stale-disk-marker", "mode": "delete", "role": "parity"},
)


def key_for(instance):
    return "markers/" + instance["name"] + ".bin"


def add_marker(expected, key, response):
    marker = response.get("VersionId")
    require(response.get("DeleteMarker") is True and isinstance(marker, str) and marker not in {"", "null"}
            and response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 204
            and not any(entry["versionId"] == marker for entry in expected), "marker creation lacks a distinct exact DELETE ACK")
    result = copy.deepcopy(expected)
    require(any(entry["key"] == key for entry in result), "marker creation has no acknowledged data key")
    for entry in result:
        if entry["key"] == key: entry["isLatest"] = False
    result.append({"key": key, "versionId": marker, "deleteMarker": True, "isLatest": True})
    return result


def remove_marker(expected, key, response):
    selected = [entry for entry in expected if entry["key"] == key and entry["deleteMarker"] and entry["isLatest"]]
    require(len(selected) == 1 and response.get("VersionId") == selected[0]["versionId"]
            and response.get("DeleteMarker") is True and response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 204,
            "current marker removal lacks its exact DELETE ACK")
    result = [copy.deepcopy(entry) for entry in expected if entry != selected[0]]
    data = [entry for entry in result if entry["key"] == key and not entry["deleteMarker"]]
    require(len(data) == 3, "marker removal lost or invented a retained data version")
    for entry in result:
        if entry["key"] == key: entry["isLatest"] = entry is data[-1]
    return result


def seed_markers(recorder, bucket, size):
    recorder.call("create_bucket", {"Bucket": bucket})
    recorder.call("put_bucket_versioning", {"Bucket": bucket, "VersioningConfiguration": {"Status": "Enabled"}})
    expected = []
    for instance in INSTANCES:
        key = key_for(instance)
        for nonce in range(3):
            body = payload(f"{bucket}/{key}/{nonce}", size)
            response = recorder.call("put_object", {"Bucket": bucket, "Key": key,
                "Metadata": {"heal-fixture": "marker-semantics", "generation": str(nonce)},
                "Tagging": f"suite=heal&generation=v{nonce}"}, body)
            version = response.get("VersionId")
            require(isinstance(version, str) and version not in {"", "null"}
                    and not any(entry["versionId"] == version for entry in expected), "marker fixture lacks distinct versioned PUT ACKs")
            for entry in expected:
                if entry["key"] == key: entry["isLatest"] = False
            expected.append({"key": key, "versionId": version, "deleteMarker": False, "isLatest": True,
                             "payload": {"size": size, "sha256": hashlib.sha256(body).hexdigest()}})
            if instance["mode"] == "historical" and nonce == 1:
                expected = add_marker(expected, key, recorder.call("delete_object", {"Bucket": bucket, "Key": key}))
        if instance["mode"] in {"current", "delete"}:
            expected = add_marker(expected, key, recorder.call("delete_object", {"Bucket": bucket, "Key": key}))
    return expected


def marker_plan(instance, snapshot, bucket, expected):
    # Imported lazily: the shared version executor also uses this module.
    try:
        from .versions import choose_target, metadata_target
    except ImportError:
        from versions import choose_target, metadata_target
    key = key_for(instance)
    selected = [entry for entry in expected if entry["key"] == key]
    data = [entry for entry in selected if not entry["deleteMarker"]]
    require(len(data) == 3 and len({entry["versionId"] for entry in selected}) == len(selected), "marker plan lacks the three exact data versions")
    if instance["mode"] != "create":
        require(len([entry for entry in selected if entry["deleteMarker"]]) == 1, "marker plan lacks its acknowledged current/historical marker")
    target = choose_target(snapshot, bucket, data[0], instance["role"])
    proofs = [choose_target(snapshot, bucket, entry, "data") for entry in data]
    proofs = [{**item, **next(shard for shard in item["cohort"] if
               (shard["node"], shard["disk"]) == (target["node"], target["disk"]))} for item in proofs]
    return {"scope": {"bucket": bucket, "key": key}, "recursive": True, "expected": selected,
            "faults": [] if instance["mode"] in {"create", "delete"} else [metadata_target(snapshot, target)],
            "proofs": proofs, "requiredRepairs": set(), "metadataRepairs": {key}, "parameters": instance,
            "staleMarker": instance["mode"] if instance["mode"] in {"create", "delete"} else None}


def stale_expected_scene(original, authorities, bucket, key, expected, target):
    """Derive an oracle from real quorum metadata. This never writes metadata."""
    left, right = files(original), files(authorities)
    selected = (target["node"], target["disk"])
    require({node: value["formats"] for node, value in original.items()} ==
            {node: value["formats"] for node, value in authorities.items()}, "stale window changed original disk/set identities")
    require({identity: entry for identity, entry in left.items() if identity[:2] == selected} ==
            {identity: entry for identity, entry in right.items() if identity[:2] == selected}, "inaccessible member received a marker write")
    wanted = {(entry["versionId"], entry["deleteMarker"], entry["isLatest"])
              for entry in expected if entry["key"] == key}
    path = f"{bucket}/{key}/xl.meta"
    donors = [entry["metadata"] for identity, entry in right.items() if identity[2] == path and identity[:2] != selected]
    require(len(donors) == len(target["cohort"]) - 1 and all(
        {(version["versionId"], version["deleted"], version["isLatest"]) for version in donor["versions"]} == wanted
        for donor in donors), "online marker replicas lack a full same-version ACK authority quorum")
    for identity, entry in right.items():
        if identity[2] != path or identity[:2] == selected: continue
        previous = left[identity]["metadata"]
        actual = entry["metadata"]
        require({name: value for name, value in previous.items() if name != "versions"} ==
                {name: value for name, value in actual.items() if name != "versions"},
                "marker operation changed the metadata format or object identity")
        previous_data = {version["versionId"]: {name: value for name, value in version.items() if name != "isLatest"}
                         for version in previous["versions"] if not version["deleted"]}
        actual_data = {version["versionId"]: {name: value for name, value in version.items() if name != "isLatest"}
                       for version in actual["versions"] if not version["deleted"]}
        require(previous_data == actual_data, "marker operation changed a retained data-version reference")
    current_meta = left[(*selected, path)]["metadata"]
    require({(version["versionId"], version["deleted"], version["isLatest"]) for version in current_meta["versions"]} != wanted,
            "marker window did not create a genuinely stale replica")
    template = donors[0]
    old = {version["versionId"]: version for version in current_meta["versions"]}
    versions = []
    for authority in template["versions"]:
        if authority["deleted"]:
            require(authority.get("dataDir") is None and not authority.get("parts") and authority.get("inline") is None,
                    "marker authority unexpectedly has live data references")
            versions.append(copy.deepcopy(authority))
        else:
            require(authority["versionId"] in old and not old[authority["versionId"]]["deleted"], "stale marker changed a data generation")
            version = copy.deepcopy(old[authority["versionId"]])
            version["isLatest"] = authority["isLatest"]
            versions.append(version)
    oracle = copy.deepcopy(authorities)
    for entry in oracle[selected[0]]["files"]:
        if entry["disk"] == selected[1] and entry["path"] == path:
            entry["metadata"]["versions"] = versions
    # Data shards and all controls are unchanged throughout the legitimate
    # marker operation; only this key's xl.meta is allowed to change online.
    require({identity: entry for identity, entry in left.items() if identity[2] != path} ==
            {identity: entry for identity, entry in right.items() if identity[2] != path},
            "marker ACK changed a data shard, auxiliary file or control")
    return oracle


def prepare_stale(recorder, fleet, bucket, plan, before, expected):
    target = plan["proofs"][0]
    node, disk, key = target["node"], target["disk"], plan["scope"]["key"]
    fleet.drive_gates.add(node)
    fleet.record("marker-member-isolated", fleet.remote(node, "drive-gate", disk=disk, mode="permission-denied"))
    fleet.start()
    refusal = fleet.record("marker-service-denial", fleet.remote(node, "drive-gate-probe"))
    require(refusal.get("readProbe", {}).get("errno") == 13 and refusal.get("dacBypassRemoved") is True,
            "stale marker member lacks actual same-service read/write denial")
    params = {"Bucket": bucket, "Key": key}
    if plan["staleMarker"] == "delete":
        params["VersionId"] = next(entry["versionId"] for entry in plan["expected"] if entry["deleteMarker"] and entry["isLatest"])
    response = recorder.call("delete_object", params)
    manifest = (add_marker if plan["staleMarker"] == "create" else remove_marker)(expected, key, response)
    baseline = fleet.record("marker-online-ack-inventory", recorder.probe.inventory(bucket))
    fleet.record("marker-online-ack-proof", {"expected": manifest, **verify_seed(manifest, baseline)})
    fleet.stop()
    authority = fleet.snapshot("marker-stale-authorities", quiescent=True)
    oracle = stale_expected_scene(before, authority, bucket, key, manifest, target)
    fleet.record("marker-derived-physical-oracle", {"scene": oracle, "metadataWrittenByHarness": False,
        "derivation": "acknowledged marker quorum plus original same-drive data-version references"})
    fleet.record("marker-member-rejoined", fleet.remote(node, "drive-gate", mode="clear"))
    fleet.drive_gates.remove(node)
    plan["expected"] = [entry for entry in manifest if entry["key"] == key]
    return oracle, authority, manifest, baseline


def validate_marker_error(observed, status, version):
    response = observed.get("response") or {}
    metadata = response.get("ResponseMetadata", {})
    headers = {name.lower(): value for name, value in metadata.get("HTTPHeaders", {}).items()}
    require(observed.get("acknowledged") is False and observed.get("resultUnknown") is False and "payload" not in observed
            and metadata.get("HTTPStatusCode") == status and headers.get("x-amz-delete-marker") == "true"
            and headers.get("x-amz-version-id") == version, "marker refusal lost its exact HTTP/version/marker identity")


def verify_markers(probe, fleet, bucket, key, expected):
    selected = [entry for entry in expected if entry["key"] == key]
    probes = []
    for endpoint in probe.binding["endpoints"]:
        peer = Probe(probe.binding, endpoint)
        for entry in [entry for entry in selected if entry["deleteMarker"]]:
            requests = [(entry["versionId"], 405)] + ([(None, 404)] if entry["isLatest"] else [])
            for selector, status in requests:
                for method in ("get_object", "head_object"):
                    params = {"Bucket": bucket, "Key": key}
                    if selector: params["VersionId"] = selector
                    observed = fleet.record("marker-s3-refusal", peer.s3(method, params))
                    validate_marker_error(observed, status, entry["versionId"])
                    probes.append({"endpoint": endpoint, "method": method, "selector": selector,
                                   "status": status, "markerVersion": entry["versionId"]})
    return {"allPeerMarkerIdentityHeaders": True, "probes": probes}


def reveal_current(recorder, fleet, bucket, key, expected, baseline):
    current = next((entry for entry in expected if entry["key"] == key and entry["deleteMarker"] and entry["isLatest"]), None)
    if current is None: return expected, baseline, {"currentMarkerPresent": False}
    response = recorder.call("delete_object", {"Bucket": bucket, "Key": key, "VersionId": current["versionId"]})
    manifest = remove_marker(expected, key, response)
    revealed = next(entry for entry in manifest if entry["key"] == key and entry["isLatest"])
    for endpoint in recorder.probe.binding["endpoints"]:
        peer = Probe(recorder.probe.binding, endpoint)
        read = fleet.record("marker-revealed-data", peer.s3("get_object", {"Bucket": bucket, "Key": key}))
        require(read.get("acknowledged") and read.get("payload") == revealed["payload"]
                and read.get("response", {}).get("VersionId") == revealed["versionId"], "marker removal exposed another data generation")
        deleted = fleet.record("marker-removed-version", peer.s3("get_object", {
            "Bucket": bucket, "Key": key, "VersionId": current["versionId"]}))
        require(deleted.get("acknowledged") is False and deleted.get("resultUnknown") is False
                and "payload" not in deleted and deleted.get("response", {}).get("ResponseMetadata", {}).get("HTTPStatusCode") == 404,
                "explicitly removed marker remained addressable")
    inventory = fleet.record("marker-removal-inventory", recorder.probe.inventory(bucket))
    verify_seed(manifest, inventory)
    old = {(entry["key"], entry["versionId"]): entry for entry in baseline["entries"]}
    require(len(inventory["entries"]) == len(baseline["entries"]) - 1, "marker removal created or lost extra versions")
    for entry in inventory["entries"]:
        previous = copy.deepcopy(old[(entry["key"], entry["versionId"])])
        if entry["key"] == key: previous["isLatest"] = entry["isLatest"]
        require(previous == entry, "marker removal changed a retained version or public property")
    return manifest, inventory, {"currentMarkerRemoved": current["versionId"], "revealedVersion": revealed["versionId"],
                                 "noAdditionalDataVersions": True, "allPeerCurrentReadsMatch": True}
