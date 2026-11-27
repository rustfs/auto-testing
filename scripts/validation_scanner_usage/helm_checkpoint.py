"""Exercise #9672 in a RustFS pod rendered from the chart under test."""

import json
from pathlib import Path
import secrets
import tempfile
import time

from .client import ProbeError, bucket_view, integer, run_command, sha256


FIXTURE = Path(__file__).resolve().parents[2] / "testdata/scanner-usage/paged-checkpoint-1.0.7.tgz"
FIXTURE_SHA256 = "ed05a2399315ae1dfc0b27d93b412f61ebd0bcedf33e869dc8bf4bd40dc2dca6"


def command(args, timeout=30):
    result = run_command(args, timeout)
    if result["timed_out"]:
        raise ProbeError(f"{args[0]} timed out after {timeout}s")
    if result["exit_code"] != 0:
        raise ProbeError(f"{args[0]} failed: {result['stderr'][-1000:]}")
    return result["stdout"]


def check_snapshot(snapshot):
    if not isinstance(snapshot, dict):
        raise ProbeError("Admin returned an invalid data usage snapshot", "FAIL")
    bucket = bucket_view(snapshot, "bucket")
    totals = (snapshot.get("objects_total_count"), snapshot.get("objects_total_size"))
    return (bool(snapshot.get("last_update")) and integer(snapshot.get("scanner_cycle"))
            and bucket["converged"] and (bucket["count"], bucket["size"]) == (1, 16)
            and all(integer(value) for value in totals) and totals == (1, 16))


def run(options, provenance, evidence, cleanup):
    if options.execution == "native" or not options.helm_chart or not options.kube_context:
        raise ProbeError("USAGE-22 requires --helm-chart and --kube-context for a disposable Kubernetes cluster", "BLOCKED")
    chart = options.helm_chart.resolve(strict=True)
    if not chart.is_file():
        raise ProbeError("--helm-chart must name a packaged RustFS chart")
    if sha256(FIXTURE) != FIXTURE_SHA256:
        raise ProbeError("The seed archive differs from the verified 2,085,052-byte checkpoint fixture")
    namespace = "usage-22-" + secrets.token_hex(10)
    kube = ["kubectl", "--context", options.kube_context]
    namespaced = [*kube, "--namespace", namespace]
    evidence.save("fixture-inputs", {
        "namespace": namespace, "context": options.kube_context, "chart_sha256": sha256(chart),
        "seed_sha256": FIXTURE_SHA256, "checkpoint_bytes": 2085052,
    })
    cleanup.update(status="FAIL", namespace=namespace, context=options.kube_context, creation_confirmed=False)
    command([*kube, "create", "namespace", namespace])
    cleanup["creation_confirmed"] = True
    try:
        with tempfile.TemporaryDirectory() as scratch:
            install_pod(namespaced, chart, namespace, Path(scratch))
        command([*namespaced, "wait", "--for=condition=Ready", "pod/rustfs", "--timeout=180s"], 190)
        pod = json.loads(command([*namespaced, "get", "pod", "rustfs", "-o", "json"]))
        app = rustfs_container(pod["spec"])
        execute = [*namespaced, "exec", "rustfs", "-c", "rustfs", "--"]
        uid = int(command([*execute, "id", "-u"]).strip())
        if app.get("securityContext", {}).get("readOnlyRootFilesystem") is not True or uid <= 0:
            raise ProbeError("The RustFS container must run as non-root with a read-only root filesystem", "FAIL")
        binary = command([*execute, "sha256sum", "/usr/bin/rustfs"]).partition(" ")[0]
        if binary != provenance["binary_sha256"]:
            raise ProbeError("The running RustFS binary differs from the candidate manifest")
        request = ('exec curl --noproxy "*" -fsS --max-time 10 --aws-sigv4 aws:amz:us-east-1:s3 '
                   '--user "$RUSTFS_ACCESS_KEY:$RUSTFS_SECRET_KEY" http://127.0.0.1:9000/rustfs/admin/v3/datausageinfo')
        deadline = time.monotonic() + options.convergence_timeout
        while True:
            snapshot = json.loads(command([*execute, "sh", "-ec", request]))
            logs = command([*namespaced, "logs", "rustfs", "-c", "rustfs", "--tail=150"])
            for line in logs.splitlines():
                if "Read-only file system" in line and "cache" in line.lower() and "scanner" in line.lower():
                    evidence.save("restore-error", {"message": line, "snapshot": snapshot})
                    raise ProbeError("Scanner cache restore failed with a read-only filesystem", "FAIL")
            if check_snapshot(snapshot):
                return evidence.save("published-usage", {
                    "image": app["image"], "binary_sha256": binary, "uid": uid,
                    "read_only_root": True, "snapshot": snapshot,
                })
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                evidence.save("last-usage", snapshot)
                raise ProbeError("Scanner did not publish the fixture's exact object and byte totals", "FAIL")
            time.sleep(min(options.poll_interval, remaining))
    finally:
        try:
            command([*kube, "delete", "namespace", namespace, "--timeout=120s"], 130)
        except (OSError, ProbeError) as exc:
            raise ProbeError(f"Could not remove fixture namespace {namespace}: {exc}", cleanup_status="FAIL") from exc
        cleanup["status"] = "PASS"


def rustfs_container(spec):
    for container in spec.get("containers", []):
        if container.get("name") == "rustfs":
            return container
    raise ProbeError("The rendered workload is missing the RustFS container")


def install_pod(kube, chart, namespace, scratch):
    # Keep the chart's RustFS command, security context and /tmp wiring. Only
    # /data is seeded, before the server starts and before any usage snapshot exists.
    data_mount = {"name": "fixture-data", "mountPath": "/data"}
    values = {
        "mode": {"standalone": {"enabled": True}, "distributed": {"enabled": False}},
        "config": {"rustfs": {
            "obs_log_directory": "",
            "scanner": {"speed": "fastest", "cycle_secs": "2", "start_delay_secs": "1"},
        }},
        "storageclass": {"dataStorageSize": ""},
        "serviceAccount": {"create": False},
        "secret": {"rustfs": {"access_key": "usage-fixture", "secret_key": secrets.token_hex(24)}},
        "extraVolumes": [{"name": "fixture-data", "emptyDir": {}}],
        "extraVolumeMounts": [data_mount],
    }
    values_file = scratch / "values.json"
    values_file.write_text(json.dumps(values))
    render = ["helm", "template", "rustfs", str(chart), "--namespace", namespace, "-f", str(values_file)]
    manifest = scratch / "rendered.yaml"
    manifest.write_text(command([*render, "-s", "templates/configmap.yaml", "-s", "templates/secret.yaml"]))
    command([*kube, "create", "-f", str(manifest)])
    command([*kube, "create", "configmap", "checkpoint", f"--from-file=disk.tgz={FIXTURE}"])
    manifest.write_text(command([*render, "-s", "templates/deployment.yaml"]))
    deployment = json.loads(command([*kube, "create", "--dry-run=client", "-f", str(manifest), "-o", "json"]))
    spec = deployment["spec"]["template"]["spec"]
    app = rustfs_container(spec)
    spec["volumes"].append({"name": "checkpoint", "configMap": {"name": "checkpoint"}})
    spec.setdefault("initContainers", []).append({
        "name": "seed-checkpoint", "image": app["image"], "securityContext": app.get("securityContext", {}),
        "command": ["tar", "--no-same-owner", "-xzf", "/seed/disk.tgz", "-C", "/data"],
        "volumeMounts": [data_mount, {"name": "checkpoint", "mountPath": "/seed", "readOnly": True}]})
    manifest.write_text(json.dumps({"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "rustfs"}, "spec": spec}))
    command([*kube, "create", "-f", str(manifest)])
