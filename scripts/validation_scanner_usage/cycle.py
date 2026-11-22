"""Strict scanner cycle and epoch observations for public probes."""

from __future__ import annotations

from typing import Any

from .client import ProbeError, integer


class CycleTracker:
    """Validate the monotonic identity carried by Admin usage snapshots."""

    def __init__(self) -> None:
        self.last_cycle: dict[int, int] = {}
        self.epoch: str | None = None

    def observe(self, snapshot: dict[str, Any], endpoint_index: int) -> dict[str, Any]:
        cycle = snapshot.get("scanner_cycle")
        if not integer(cycle):
            raise ProbeError("Admin usage response has no non-negative scanner cycle", "FAIL")
        epoch = snapshot.get("scanner_epoch")
        if epoch is not None and (not isinstance(epoch, str) or not epoch):
            raise ProbeError("Admin usage response has an invalid scanner epoch", "FAIL")
        if self.epoch is None:
            self.epoch = epoch
        elif epoch is not None and epoch != self.epoch:
            raise ProbeError("Admin usage response changed scanner epoch during a probe", "FAIL")
        previous = self.last_cycle.get(endpoint_index)
        if previous is not None and cycle < previous:
            raise ProbeError("Admin usage response regressed scanner cycle", "FAIL")
        self.last_cycle[endpoint_index] = cycle
        return {
            "endpoint_index": endpoint_index,
            "scanner_cycle": cycle,
            "scanner_epoch": epoch,
            "previous_cycle": previous,
            "advanced": previous is not None and cycle > previous,
        }
