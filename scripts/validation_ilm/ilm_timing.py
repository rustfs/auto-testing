"""Independent time and progress oracles for serial, live ILM validation."""

from __future__ import annotations

import math
import signal
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from typing import Any, Callable, Iterator


class TimingPreconditionError(RuntimeError):
    """The driver cannot establish the required before/after observation window."""


class DeadlineExceeded(AssertionError):
    """An operation did not satisfy its contract within the original budget."""


def parse_expiry_date(value: str) -> datetime:
    """Read an absolute expiry instant, not certify its wire-format parity.

    AWS examples use HTTP dates; current RustFS also emits RFC 3339 here.
    Both retain their timezone and must satisfy the same independent due window.
    """
    try:
        try:
            parsed = parsedate_to_datetime(value)
        except (ValueError, TypeError, IndexError):
            parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError("expiry-date has no timezone")
        return parsed.astimezone(UTC)
    except (ValueError, TypeError, IndexError) as exc:
        raise AssertionError(f"invalid expiry-date: {value!r}") from exc


# Real elapsed time is deliberately independent of fake clocks in oracle tests.
_elapsed = time.monotonic
_budgets: list[tuple[float, str, type[Exception]]] = []


def _alarm(_signum: int, _frame: Any) -> None:
    _end, label, error = min(_budgets, key=lambda item: item[0])
    raise error(f"deadline exceeded: {label}")


def _arm() -> None:
    remaining = min(item[0] for item in _budgets) - _elapsed()
    # An expired parent is already unwinding. Re-arming it would interrupt its
    # exception handling repeatedly, possibly hiding the original failure.
    signal.setitimer(signal.ITIMER_REAL, remaining if remaining > 0 else 0)


def _check() -> None:
    if _budgets:
        end, label, error = min(_budgets, key=lambda item: item[0])
        if _elapsed() >= end:
            raise error(f"deadline exceeded: {label}")


@contextmanager
def time_budget(
    seconds: float, label: str, error: type[Exception] = DeadlineExceeded
) -> Iterator[None]:
    """Interrupt stalled I/O and body reads; nested budgets cannot extend parents.

    Live ILM cases run serially on the main thread on the Linux driver. A thread
    pool timeout would leave the timed-out request mutating the next fixture.
    """
    if not math.isfinite(seconds) or seconds <= 0:
        raise error(f"deadline exceeded: {label}")
    if threading.current_thread() is not threading.main_thread():
        raise RuntimeError("ILM time budgets require serial main-thread execution")
    _check()
    previous_handler = None
    if not _budgets:
        if signal.getitimer(signal.ITIMER_REAL)[0] > 0:
            raise RuntimeError("ILM cannot replace an existing process alarm")
        previous_handler = signal.signal(signal.SIGALRM, _alarm)
    end = _elapsed() + seconds
    entry = (end, label, error)
    try:
        _budgets.append(entry)
        _arm()
        yield
        _check()
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        if entry in _budgets:
            _budgets.remove(entry)
        if _budgets:
            _arm()
        else:
            signal.signal(signal.SIGALRM, previous_handler)


@dataclass(frozen=True)
class DueWindow:
    earliest: datetime
    latest: datetime

    def as_dict(self) -> dict[str, str]:
        return {"earliest": self.earliest.isoformat(), "latest": self.latest.isoformat()}


def expiry_window(
    modified: datetime, days: int, day_seconds: int, process_seconds: int = 1
) -> DueWindow:
    """Round independently, preserving uncertainty in second-resolution S3 time.

    LastModified may hide subsecond precision. The inclusive upper bound is
    deliberately conservative, including the next reported second.
    """
    if modified.tzinfo is None or days <= 0 or day_seconds <= 0 or process_seconds <= 0:
        raise TimingPreconditionError("expiry requires aware time and positive Days/intervals")
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    boundary = process_seconds * 1_000_000

    def rounded(value: datetime) -> datetime:
        delta = value.astimezone(UTC) - epoch + timedelta(seconds=days * day_seconds)
        micros = (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds
        return epoch + timedelta(microseconds=((micros + boundary - 1) // boundary) * boundary)

    return DueWindow(rounded(modified), rounded(modified + timedelta(seconds=1)))


class ServerClock:
    """Map server UTC intervals onto one monotonic driver timeline."""

    def __init__(
        self,
        samples: list[tuple[str, datetime, float, float]],
        *,
        max_skew: float = 5,
        wall: Callable[[], float] = time.time,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if not samples:
            raise TimingPreconditionError("no server clock samples")
        self.wall = wall
        self.monotonic = monotonic
        self.anchor_wall = wall()
        self.anchor_mono = monotonic()
        self.offset_lo = min(date.timestamp() - end for _, date, _, end in samples)
        self.offset_hi = max(date.timestamp() + 1 - start for _, date, start, _ in samples)
        if max(abs(self.offset_lo), abs(self.offset_hi)) > max_skew:
            raise TimingPreconditionError("server clock skew or sampling latency exceeds budget")
        self.samples = samples

    @staticmethod
    def response_date(response: dict[str, Any]) -> datetime:
        value = response.get("ResponseMetadata", {}).get("HTTPHeaders", {}).get("date")
        try:
            result = parsedate_to_datetime(value)
            if result.tzinfo is None:
                raise ValueError("Date has no timezone")
            return result.astimezone(UTC)
        except (ValueError, TypeError, IndexError) as exc:
            raise TimingPreconditionError("missing or invalid S3 response Date") from exc

    def now(self) -> DueWindow:
        projected = self.anchor_wall + self.monotonic() - self.anchor_mono
        if abs(self.wall() - projected) > 1:
            raise TimingPreconditionError("driver wall clock jumped during the case")
        return DueWindow(
            datetime.fromtimestamp(projected + self.offset_lo, UTC),
            datetime.fromtimestamp(projected + self.offset_hi, UTC),
        )

    def validate_response(self, response: dict[str, Any], start: float, end: float) -> None:
        date = self.response_date(response).timestamp()
        self.now()
        if date + 1 < start + self.offset_lo or date > end + self.offset_hi:
            raise TimingPreconditionError("server Date moved outside the calibrated clock interval")

    def require_before(self, due: DueWindow, *, margin: float = 0) -> None:
        if self.now().latest + timedelta(seconds=margin) >= due.earliest:
            raise TimingPreconditionError("missed the required before-expiry observation window")

    def passed(self, due: DueWindow) -> bool:
        return self.now().earliest > due.latest

    def timeout_after(self, due: DueWindow, allowance: float) -> float:
        return max(0, (due.latest - self.now().earliest).total_seconds()) + allowance

    def as_dict(self) -> dict[str, Any]:
        return {
            "anchorUtc": datetime.fromtimestamp(self.anchor_wall, UTC).isoformat(),
            "anchorMonotonic": self.anchor_mono,
            "offsetSeconds": [self.offset_lo, self.offset_hi],
            "samples": [
                {"endpoint": endpoint, "date": date.isoformat(), "sentUtc": start, "receivedUtc": end}
                for endpoint, date, start, end in self.samples
            ],
        }


def successful_cycle(cluster: dict[str, Any]) -> tuple[str, int, int | None] | None:
    """Accept only a single fresh healthy leader and a completed success."""
    leaders = []
    for node in cluster.get("nodes", []):
        data = node.get("status", {}).get("data", {})
        metrics = data.get("metrics", {})
        if metrics.get("leader_lock_held_by_this_process") is True:
            leaders.append((node.get("endpoint"), data, metrics))
    if len(leaders) != 1:
        return None
    endpoint, data, metrics = leaders[0]
    freshness = data.get("freshness", {})
    ended = freshness.get("last_cycle_end_unix_secs")
    if not (
        isinstance(endpoint, str)
        and data.get("enabled") is True
        and data.get("health") == "healthy"
        and freshness.get("state") == "fresh"
        and metrics.get("last_cycle_result") == "success"
        and metrics.get("leader_lock_state") == "acquired"
        and data.get("cycle_schedule", {}).get("execution_role") == "leader"
        and isinstance(ended, int) and not isinstance(ended, bool) and ended > 0
    ):
        return None
    lifetime_cycles = metrics.get("life_time_ops", {}).get("scan_cycle")
    if lifetime_cycles is not None and (
        not isinstance(lifetime_cycles, int) or isinstance(lifetime_cycles, bool) or lifetime_cycles < 0
    ):
        return None
    return endpoint, ended, lifetime_cycles


class CycleProgress:
    """Count distinct observed cycles after a baseline, never historical success."""

    def __init__(self) -> None:
        self.last: tuple[str, int, int | None] | None = None
        self.count = 0

    def observe(self, cluster: dict[str, Any]) -> int:
        current = successful_cycle(cluster)
        if current is None:
            # A gap/election may conceal a restart: re-establish the baseline.
            self.last = None
            self.count = 0
        elif (self.last is None or current[0] != self.last[0] or current[1] < self.last[1]
              or (current[2] is None) != (self.last[2] is None)
              or (current[2] is not None and self.last[2] is not None and current[2] < self.last[2])):
            self.last = current
            self.count = 0
        elif current[1] > self.last[1]:
            self.count += 1
            self.last = current
        else:
            # Preserve the latest lifetime counter between completions to detect
            # an observed same-node reset even when the UTC end keeps advancing.
            self.last = current
        return self.count


class BoundedBody:
    def __init__(self, body: Any, seconds: float) -> None:
        self.body = body
        self.end = _elapsed() + seconds

    def read(self, *args: Any, **kwargs: Any) -> bytes:
        try:
            with time_budget(self.end - _elapsed(), "S3 response body", TimeoutError):
                result = self.body.read(*args, **kwargs)
            amount = args[0] if args else kwargs.get("amt")
            if amount is None or amount < 0 or not result:
                self.body.close()
            return result
        except BaseException:
            self.body.close()
            raise

    def close(self) -> None:
        self.body.close()


class BoundedS3Client:
    """Bound complete SDK calls, including hidden retries and streamed bodies."""

    def __init__(self, client: Any, seconds: float) -> None:
        self.client = client
        self.seconds = seconds

    def __getattr__(self, name: str) -> Any:
        member = getattr(self.client, name)
        if not callable(member) or name == "close":
            return member

        def call(*args: Any, **kwargs: Any) -> Any:
            started = _elapsed()
            with time_budget(self.seconds, f"S3 {name}", TimeoutError):
                response = member(*args, **kwargs)
            if isinstance(response, dict) and "Body" in response:
                response["Body"] = BoundedBody(response["Body"], self.seconds - (_elapsed() - started))
            return response

        return call
