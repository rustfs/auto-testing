"""Strict scanner cycle and epoch observations for public probes."""

from __future__ import annotations

from typing import Any

from .client import ProbeError, integer


class CycleTracker:
    """Validate the monotonic identity carried by Admin usage snapshots."""

    def __init__(self) -> None:
        self.last_cycle: dict[int, int] = {}
        self.epoch: int | None = None

    def observe(self, snapshot: dict[str, Any], endpoint_index: int) -> dict[str, Any]:
        cycle = snapshot.get("scanner_cycle")
        epoch = snapshot.get("scanner_epoch")
        # Before a snapshot covers any current bucket, the Admin endpoint can
        # return an empty, explicitly incomplete view without a scan identity.
        unpublished = ("scanner_cycle" not in snapshot and epoch is None
                       and snapshot.get("usage_snapshot_complete") is False
                       and snapshot.get("last_update") is None
                       and snapshot.get("buckets_usage") == {})
        if not integer(cycle) and not unpublished:
            raise ProbeError("Admin usage response has no non-negative scanner cycle", "FAIL")
        if epoch is not None and not integer(epoch):
            raise ProbeError("Admin usage response has an invalid scanner epoch", "FAIL")
        if self.epoch is None:
            self.epoch = epoch
        elif epoch is not None and epoch != self.epoch:
            raise ProbeError("Admin usage response changed scanner epoch during a probe", "FAIL")
        previous = self.last_cycle.get(endpoint_index)
        if cycle is not None:
            if previous is not None and cycle < previous:
                raise ProbeError("Admin usage response regressed scanner cycle", "FAIL")
            self.last_cycle[endpoint_index] = cycle
        return {
            "endpoint_index": endpoint_index,
            "scanner_cycle": cycle,
            "scanner_epoch": epoch,
            "previous_cycle": previous,
            "advanced": cycle is not None and previous is not None and cycle > previous,
        }
