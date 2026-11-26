#!/usr/bin/env python3
"""Run the RustFS musl CPU-affinity regression on a high-CPU Linux runner."""

import argparse
import json
import os
from pathlib import Path
import platform
import secrets
import subprocess
import tempfile
from types import SimpleNamespace

if __package__:
    from .local_rustfs import local_rustfs
else:
    from local_rustfs import local_rustfs


CASE_ID = "runtime.musl-cpu-affinity"
LIFECYCLE_TESTS = "bucket::lifecycle::bucket_lifecycle_ops::tests::"
ECSTORE_TESTS = (
    "disk::os::tests::disk_blocking_queue_bounds_concurrency_and_recovers_from_panic",
    "store::object::tests::batch_delete_preserves_results_and_accounting_across_sets",
    LIFECYCLE_TESTS + "expiry_worker_count_resolves_all_env_states",
    LIFECYCLE_TESTS + "resolve_transition_worker_count_uses_fallback_when_env_missing",
    LIFECYCLE_TESTS + "resize_workers_wires_recovery_notify_to_worker_failures",
)


def select_masks(allowed):
    low = sorted(cpu for cpu in allowed if cpu < 128)
    high = sorted(cpu for cpu in allowed if 128 <= cpu < 1024)
    if len(low) < 2 or len(high) < 4:
        raise RuntimeError("requires at least two allowed CPUs below 128 and four in 128–1023")
    return {"high-only": set(high[:4]), "mixed": set(low[:2] + high[:2])}


def run_ecstore_tests(binary, directory):
    env = {"PATH": os.environ.get("PATH", os.defpath), "RUST_MIN_STACK": "4194304"}
    for name in ECSTORE_TESTS:
        result = subprocess.run(
            [str(binary), "--exact", name, "--test-threads=1", "--color=never"],
            cwd=directory, env=env, capture_output=True, text=True, timeout=90, check=False,
        )
        if result.returncode or not any(
            line.startswith("test result: ok. 1 passed; 0 failed; 0 ignored;")
            for line in result.stdout.splitlines()
        ):
            raise RuntimeError(f"{name} did not pass exactly once")


def exercise(server, ecstore_tests, root):
    allowed = os.sched_getaffinity(0)
    masks = select_masks(allowed)
    credentials = SimpleNamespace(access_key="affinity-regression", secret_key=secrets.token_hex(24))
    for label, mask in masks.items():
        directory = root / label
        directory.mkdir()
        try:
            os.sched_setaffinity(0, mask)
            if os.sched_getaffinity(0) != mask:
                raise RuntimeError("runner did not grant the full CPU mask")
            with local_rustfs(server, credentials, directory, {}) as _port:
                pass  # The helper waits for /health/ready.
            run_ecstore_tests(ecstore_tests, directory)
        except (OSError, RuntimeError, subprocess.TimeoutExpired) as error:
            raise RuntimeError(f"{label} {sorted(mask)}: {error}") from error
        finally:
            os.sched_setaffinity(0, allowed)
    return {"case_id": CASE_ID, "masks": {label: sorted(mask) for label, mask in masks.items()},
            "ecstore_tests_per_mask": len(ECSTORE_TESTS)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rustfs-bin", type=Path, required=True, help="static x86_64-musl RustFS binary")
    parser.add_argument("--ecstore-test-bin", type=Path, required=True, help="ECStore musl unit-test binary from the same source")
    args = parser.parse_args()
    if platform.system() != "Linux" or platform.machine() != "x86_64":
        parser.error("requires x86_64 Linux")
    binaries = (args.rustfs_bin.resolve(), args.ecstore_test_bin.resolve())
    for binary in binaries:
        if not binary.is_file() or not os.access(binary, os.X_OK):
            parser.error(f"expected an executable file: {binary}")

    with tempfile.TemporaryDirectory(prefix="rustfs-musl-affinity-") as temporary:
        try:
            print(json.dumps(exercise(*binaries, Path(temporary))))
        except RuntimeError as error:
            parser.exit(1, f"{CASE_ID}: {error}\n")


if __name__ == "__main__":
    main()
