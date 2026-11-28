#!/usr/bin/env python3
"""Check measured Warp rates and tail latencies against an operator's budget."""
import argparse
import json
import math
import re
from pathlib import Path

UNITS_MS = {"ns": 1e-6, "us": 1e-3, "µs": 1e-3, "μs": 1e-3,
            "ms": 1, "s": 1000, "m": 60_000, "h": 3_600_000}
DURATION_PART = re.compile(r"([0-9]+(?:\.[0-9]+)?)(ns|us|µs|μs|ms|s|m|h)")
LATENCY_FIELDS = {"get": ("max_get_p99_ms",), "put": ("max_put_p99_ms",),
                  "mixed": ("max_get_p99_ms", "max_put_p99_ms")}


def duration_ms(value):
    parts = DURATION_PART.findall(value)
    if not parts or "".join(number + unit for number, unit in parts) != value:
        raise ValueError(f"invalid Warp duration: {value!r}")
    return sum(float(number) * UNITS_MS[unit] for number, unit in parts)


def limits_for(budgets, round_id):
    method, _, _ = round_id.partition("/")
    if method not in LATENCY_FIELDS:
        raise ValueError(f"unsupported budget round: {round_id}")
    fields = ("min_obj_per_s", *LATENCY_FIELDS[method])
    limits = budgets.get(round_id)
    if not isinstance(limits, dict) or set(limits) != set(fields):
        raise ValueError(f"{round_id} budget requires {', '.join(fields)}")
    for field, value in limits.items():
        if type(value) not in (float, int) or not math.isfinite(value) or value <= 0:
            raise ValueError(f"{round_id} {field} must be a finite positive number")
    return limits


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("validate", "check"))
    parser.add_argument("budget", type=Path)
    parser.add_argument("values", nargs="+")
    args = parser.parse_args()
    try:
        budgets = json.loads(args.budget.read_text(encoding="utf-8"))
        if not isinstance(budgets, dict):
            raise ValueError("budget must be an object keyed by method/size")
        if args.command == "validate":
            for round_id in args.values:
                limits_for(budgets, round_id)
            return 0
        round_id, rate, get_p99, put_p99 = args.values
        limits = limits_for(budgets, round_id)
        measured_rate = float(rate)
        if not math.isfinite(measured_rate) or measured_rate < limits["min_obj_per_s"]:
            raise ValueError(f"{round_id} throughput {rate} obj/s below {limits['min_obj_per_s']} obj/s")
        for field, measurement in (("max_get_p99_ms", get_p99), ("max_put_p99_ms", put_p99)):
            if field in limits:
                latency = duration_ms(measurement)
                if latency > limits[field]:
                    raise ValueError(f"{round_id} {field}: measured {latency:g} ms exceeds {limits[field]} ms")
        return 0
    except (OSError, ValueError, OverflowError) as exc:
        parser.exit(1, f"performance budget: {exc}\n")


if __name__ == "__main__":
    raise SystemExit(main())
