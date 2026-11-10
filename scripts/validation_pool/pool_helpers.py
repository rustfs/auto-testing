"""Pool topology and fixture rules used by the source behavior checks."""

MIN_ACTIVE_FIXTURE_BYTES = 32 * 1024 * 1024
MIN_ACTIVE_REBALANCE_FIXTURE_BYTES = 128 * 1024 * 1024


def active_endpoint_indices(endpoint_count: int, layout: str) -> tuple[int, ...]:
    if layout not in {"initial", "expanded", "retired"}:
        raise ValueError(f"unknown pool topology layout: {layout}")
    if endpoint_count == 2:
        return {
            "initial": (0,),
            "expanded": (0, 1),
            "retired": (1,),
        }[layout]
    if endpoint_count == 4:
        return {
            "initial": (0, 1),
            "expanded": (0, 1, 2, 3),
            "retired": (2, 3),
        }[layout]
    raise ValueError("pool lifecycle requires exactly two or four node endpoints")


def active_fixture_bytes(configured_bytes: int) -> int:
    return max(configured_bytes, MIN_ACTIVE_FIXTURE_BYTES)


def active_rebalance_fixture_bytes(configured_bytes: int, minimum_bytes: int = MIN_ACTIVE_REBALANCE_FIXTURE_BYTES) -> int:
    if minimum_bytes < MIN_ACTIVE_FIXTURE_BYTES:
        raise ValueError("active rebalance fixtures must be at least 32 MiB per object")
    return max(configured_bytes, minimum_bytes)
