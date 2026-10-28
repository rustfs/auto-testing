"""Supplement C02/C03 DB controls with actual default-class size boundaries."""

try:
    from .boundaries import boundary_sizes, seed_boundaries, verify_boundary_ranges, verify_size_contract
    from .heal_probe import require
except ImportError:
    from boundaries import boundary_sizes, seed_boundaries, verify_boundary_ranges, verify_size_contract
    from heal_probe import require


MPU_MINIMUM = 5 * 1024**2
# Exact reviewed GLOBAL_MIN_PART_SIZE and is_min_allowed_part_size source.
MPU_SOURCES = {
    "1c7fc818dd858247c45b39542abd8291b1e6e31b": "6975b05976e88667065dd4fd2d7649ff61ce0887504e2790bc68fc73c9ba048a",
    "85c706cac09d53d0649487b8f1865f18beb53225": "6975b05976e88667065dd4fd2d7649ff61ce0887504e2790bc68fc73c9ba048a",
}


def parameters(manifest, candidate, calibration, processes, recorder, original):
    contract = verify_size_contract(manifest, candidate)
    require(candidate["source_sha"] in MPU_SOURCES and
            contract["sourceFiles"]["crates/ecstore/src/set_disk/mod.rs"] == MPU_SOURCES[candidate["source_sha"]],
            "shard DB multipart size controls require an exact reviewed candidate source")
    require({row["key"] for row in original if row["key"].startswith("objects/")} ==
            {f"objects/{index:04d}.bin" for index in range(128)},
            "size controls must retain every original default DB object")
    plan = boundary_sizes(contract, calibration, processes)
    plan["groups"]["multipart-minimum-minus-exact-plus"] = [MPU_MINIMUM - 1, MPU_MINIMUM, MPU_MINIMUM + 1]
    sizes = [size for group in plan["groups"].values() for size in group]
    require(max(sizes) <= 64 * 1024**2 and recorder.bytes_written + sum(sizes) <= recorder.probe.binding["maxInventoryBytes"]
            and len(original) + len(sizes) <= recorder.probe.binding["maxInventoryObjects"],
            "supplemental DB controls exceed the original complete fixture budget")
    return {**plan, "originalDefaultDBCount": 128, "supplementalObjectCount": len(sizes),
            "supplementalPayloadBytes": sum(sizes), "multipartMinimumPartBytes": MPU_MINIMUM,
            "multipartBoundarySourceSha256": MPU_SOURCES[candidate["source_sha"]],
            "multipartBoundaryIsObjectSizeControl": True, "completedMultipartProtocolClaimed": False,
            "faultBranchMatrixChanged": False}


def seed_controls(recorder, bucket, plan):
    expected, _ = seed_boundaries(recorder, bucket, plan)
    return expected


def map_controls(snapshot, bucket, expected, plan):
    # Lazy imports avoid the existing versions -> shards dependency at startup.
    try:
        from .shards import map_target
        from .versions import map_inline_target
    except ImportError:
        from shards import map_target
        from versions import map_inline_target
    sizes = [size for group in plan["groups"].values() for size in group]
    require(len(expected) == len(sizes) == plan["supplementalObjectCount"],
            "supplemental DB manifest omitted a required boundary")
    targets = []
    for index, (entry, size) in enumerate(zip(expected, sizes)):
        require(entry["key"] == f"boundaries/{index:04d}-{size}.bin" and entry["versionId"] == "null"
                and entry["deleteMarker"] is False and entry["isLatest"] is True and entry["payload"]["size"] == size,
                "supplemental DB ACK identity or size differs from the frozen plan")
        shape = "zero-metadata" if size == 0 else "inline" if size <= plan["actualLogicalInlineLimit"] else "external"
        target = (map_target(snapshot, bucket, entry["key"], "data", 1) if shape == "external" else
                  map_inline_target(snapshot, bucket, entry, allow_empty=size == 0))
        geometry = target["logical"]["erasure"]
        require(target["logical"]["size"] == size and target["logical"]["usesLegacyChecksum"] is False
                and geometry["data_blocks"] == plan["actualDataBlocks"]
                and geometry["block_size"] == plan["actualECBlockSize"]
                and len(target["cohort"]) == geometry["data_blocks"] + geometry["parity_blocks"],
                "supplemental DB control differs from the actual calibrated full-set geometry")
        targets.append({"expected": entry, "shape": shape, "target": target})
    return targets


def prove_controls(fleet, targets, label):
    try:
        from .versions import verify_proof
    except ImportError:
        from versions import verify_proof
    return [{"key": row["expected"]["key"], "size": row["expected"]["payload"]["size"], "shape": row["shape"],
             "physical": verify_proof(fleet, row["target"], label)} for row in targets]


def verify_ranges(probe, fleet, bucket, expected, plan):
    return verify_boundary_ranges(probe, fleet, bucket, expected, plan["actualECBlockSize"])
