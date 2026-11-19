#!/usr/bin/env python3
"""Replace one member of a Docker 4x1 pool and prove its reconstructed shards."""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import secrets
import signal
import subprocess
import tarfile
import time
from urllib.error import URLError
from urllib.request import ProxyHandler, build_opener

try:
    from .validation_heal.heal_probe import payload, require, save
except ImportError:
    from validation_heal.heal_probe import payload, require, save


LABEL = "org.rustfs.auto-testing.node-replacement"
SIZES = (1024 * 1024 + 47, 3 * 1024 * 1024 + 19, 8 * 1024 * 1024 + 7)
ARCHIVE_LIMIT = 128 * 1024 * 1024


def archive_files(raw):
    """Read bounded regular-file evidence without extracting archive paths."""
    require(len(raw) <= ARCHIVE_LIMIT, "physical archive exceeds the fixture budget")
    result = {}
    with tarfile.open(fileobj=io.BytesIO(raw)) as archive:
        for entry in archive:
            path = PurePosixPath(entry.name)
            require(not path.is_absolute() and ".." not in path.parts, "unsafe physical archive path")
            if entry.isdir():
                continue
            require(entry.isfile() and entry.size <= ARCHIVE_LIMIT, "unexpected physical file type or size")
            name = str(path)
            require(name not in result, "duplicate physical file")
            result[name] = archive.extractfile(entry).read()
    return result


def file_manifest(files):
    return {name: {"size": len(data), "sha256": hashlib.sha256(data).hexdigest()}
            for name, data in sorted(files.items())}


def topology(disks, *, original=None):
    require(len(disks) == 4, "expected four independent data volumes")
    formats = [json.loads(files[".rustfs.sys/format.json"]) for files in disks]
    sets = formats[0]["xl"]["sets"]
    require(len(sets) == 1 and len(sets[0]) == len(set(sets[0])) == 4,
            "expected one four-drive erasure set")
    require(all(item["format"] == "xl" and item["id"] == formats[0]["id"]
                and item["xl"]["sets"] == sets for item in formats), "pool or set membership differs")
    members = [item["xl"]["this"] for item in formats]
    require(len(set(members)) == 4 and set(members) == set(sets[0]), "incomplete physical membership")
    if original is not None:
        require(formats[0]["id"] == original["poolId"] and members[:3] == original["members"][:3],
                "replacement did not preserve the pool and surviving disk identities")
        old_slot = original["set"].index(original["members"][3])
        require(sets[0][old_slot] == members[3]
                and all(sets[0][slot] == identity for slot, identity in enumerate(original["set"]) if slot != old_slot),
                "replacement moved another disk's set slot")
        # Releases may reformat with a fresh UUID or inherit the original slot
        # UUID. New container/volume identity and the empty pre-start archive
        # establish replacement independently of this format policy.
    return {"poolId": formats[0]["id"], "set": sets[0], "members": members, "nodes": 4, "drivesPerNode": 1}


def shard_manifest(files, bucket, objects):
    """Require one external part and metadata per acknowledged single-PUT key."""
    result = {}
    for key in objects:
        prefix = f"{bucket}/{key}/"
        metadata = files.get(prefix + "xl.meta", b"")
        parts = [name for name in files if name.startswith(prefix) and name.endswith("/part.1")]
        require(metadata and len(parts) == 1, f"missing or ambiguous replacement metadata/part: {key}")
        part = parts[0]
        require(len(PurePosixPath(part).relative_to(prefix).parts) == 2 and len(files[part]) > 0,
                f"invalid external shard: {key}")
        result[key] = {"path": part, **file_manifest({part: files[part]})[part],
                       "metadataSha256": hashlib.sha256(metadata).hexdigest()}
    return result


class Case:
    def __init__(self, image, artifacts, timeout):
        self.image_ref, self.artifacts, self.timeout = image, artifacts, timeout
        self.run_id = "nr-" + secrets.token_hex(6)
        self.network = self.run_id
        self.containers, self.volumes = [], []
        self.access, self.secret = secrets.token_hex(12), secrets.token_hex(24)
        self.bucket = "node-replacement-" + self.run_id
        self.objects = {}
        self.sequence = 0
        self.image = None
        self.env_file = artifacts / ".runtime.env"

    def record(self, label, value):
        self.sequence += 1
        save(self.artifacts / f"{self.sequence:03d}-{label}.json", value)
        print(label, flush=True)
        return value

    def docker(self, *arguments, timeout=90, binary=False):
        result = subprocess.run(["docker", *arguments], capture_output=True, timeout=timeout)
        if result.returncode:
            message = result.stderr.decode(errors="replace")
            for value in (self.access, self.secret):
                message = message.replace(value, "[REDACTED]")
            raise RuntimeError(f"docker {arguments[0]} failed: {message[:2000]}")
        return result.stdout if binary else result.stdout.decode().strip()

    def inspect(self, kind, name):
        return json.loads(self.docker(kind, "inspect", name))[0]

    def volume(self, suffix):
        name = self.run_id + "-" + suffix
        # Register before creation so an interrupted Docker response remains recoverable.
        self.volumes.append(name)
        self.resources()
        self.docker("volume", "create", "--label", f"{LABEL}={self.run_id}", name)
        return name

    def resources(self):
        document = {"runId": self.run_id, "label": LABEL, "network": self.network,
                    "containers": self.containers, "volumes": self.volumes}
        temporary = self.artifacts / "resources.tmp"
        temporary.write_text(json.dumps(document, indent=2) + "\n")
        temporary.replace(self.artifacts / "resources.json")

    def create(self, suffix, volume, slot, *, repair=True, ip=None):
        name = self.run_id + "-" + suffix
        self.containers.append(name)
        self.resources()
        network_args = ["--ip", ip] if ip else []
        volumes = [f"http://node{number}:9000/data" for number in range(1, 5)]
        self.docker("create", "--name", name, "--label", f"{LABEL}={self.run_id}",
                    "--hostname", f"node{slot}", "--network", self.network, "--network-alias", f"node{slot}",
                    *network_args, "--publish", "127.0.0.1::9000", "--user", "0:0",
                    "--mount", f"type=volume,source={volume},target=/data,volume-nocopy",
                    "--env-file", str(self.env_file),
                    "--env", "RUSTFS_HEAL_ENABLED=" + str(repair).lower(),
                    "--env", "RUSTFS_HEAL_AUTO_HEAL_ENABLE=" + str(repair).lower(),
                    "--env", "RUSTFS_HEAL_MRF_ENABLE=" + str(repair).lower(),
                    "--entrypoint", "/usr/bin/rustfs", self.image, *volumes)
        info = self.inspect("container", name)
        require(info["Image"] == self.image, "candidate image changed")
        return name

    def endpoint(self, name):
        ports = self.inspect("container", name)["NetworkSettings"]["Ports"]["9000/tcp"]
        require(len(ports) == 1 and ports[0]["HostIp"] == "127.0.0.1", "unexpected published endpoint")
        return "http://127.0.0.1:" + ports[0]["HostPort"]

    def wait_ready(self, names, *, writable=False):
        opener = build_opener(ProxyHandler({}))
        deadline = time.monotonic() + self.timeout
        endpoints = [self.endpoint(name) for name in names]
        while True:
            try:
                for endpoint in endpoints:
                    with opener.open(endpoint + "/health/ready", timeout=3) as response:
                        require(response.status == 200, "node is not ready")
                    if writable:
                        with opener.open(endpoint + "/minio/health/cluster", timeout=3) as response:
                            require(response.status == 200 and json.load(response).get("ready") is True,
                                    "cluster has not admitted writes")
                return
            except (OSError, ValueError, URLError):
                require(time.monotonic() < deadline, "node readiness deadline exceeded")
                time.sleep(1)

    def s3(self, name):
        import boto3
        from botocore.config import Config
        return boto3.client("s3", endpoint_url=self.endpoint(name), region_name="us-east-1",
            aws_access_key_id=self.access, aws_secret_access_key=self.secret,
            config=Config(s3={"addressing_style": "path"}, proxies={}, connect_timeout=3,
                          read_timeout=15, retries={"total_max_attempts": 1},
                          request_checksum_calculation="when_required", response_checksum_validation="when_required"))

    def write(self, name, phase):
        client = self.s3(name)
        for index, size in enumerate(SIZES):
            key = f"{phase}-{index}.bin"
            body = payload(self.run_id + key, size)
            response = client.put_object(Bucket=self.bucket, Key=key, Body=body)
            require(response["ResponseMetadata"]["HTTPStatusCode"] == 200, "PUT was not acknowledged")
            self.objects[key] = {"size": size, "sha256": hashlib.sha256(body).hexdigest(), "phase": phase}
            self.record("put-ack", {"key": key, **self.objects[key],
                                   "requestId": response["ResponseMetadata"].get("RequestId")})

    def read(self, names, label):
        for name in names:
            client = self.s3(name)
            for key, expected in self.objects.items():
                response = client.get_object(Bucket=self.bucket, Key=key)
                with response["Body"] as stream:
                    body = stream.read()
                require(len(body) == expected["size"] and hashlib.sha256(body).hexdigest() == expected["sha256"],
                        f"acknowledged object content differs: {key}")
                self.record(label, {"node": name, "key": key, **expected, "contentVerified": True})

    def stop(self, names):
        for name in names:
            self.docker("stop", "--time", "20", name)
            require(self.inspect("container", name)["State"]["Running"] is False, "node did not stop")

    def read_without_replacement(self, name):
        client = self.s3(name)
        for key in self.objects:
            try:
                response = client.get_object(Bucket=self.bucket, Key=key)
            except Exception as exc:
                error = getattr(exc, "response", {})
                code = error.get("Error", {}).get("Code")
                require(error.get("ResponseMetadata", {}).get("HTTPStatusCode") == 503
                        and code in {"SlowDownRead", "InsufficientReadQuorum", "XMinioReadQuorum"},
                        "negative reconstruction control did not reach a read-quorum failure")
                self.record("without-replacement-read", {"key": key, "httpStatus": 503, "code": code})
            else:
                response["Body"].close()
                raise ValueError("GET succeeded with only one surviving shard; reconstruction oracle is invalid")

    def snapshot(self, name, label, *, quiescent=True):
        running = self.inspect("container", name)["State"]["Running"]
        require(not quiescent or running is False, "snapshot requires a stopped node")
        raw = self.docker("cp", name + ":/data/.", "-", binary=True)
        files = archive_files(raw)
        self.record(label, {"node": name, "quiescent": not running, "files": file_manifest(files)})
        # Retain the actual stopped physical scene as well as its file hashes.
        (self.artifacts / f"{self.sequence:03d}-{label}.tar").write_bytes(raw)
        return files

    def disconnect(self, name):
        self.docker("network", "disconnect", self.network, name)
        require(self.network not in self.inspect("container", name)["NetworkSettings"]["Networks"],
                "retired node still has the cluster network")

    def quarantine_shards(self, volume, manifest):
        name = self.run_id + "-quarantine-" + str(len(self.containers))
        self.containers.append(name)
        self.resources()
        self.docker("run", "--name", name, "--network", "none", "--user", "0:0",
                    "--label", f"{LABEL}={self.run_id}",
                    "--mount", f"type=volume,source={volume},target=/data,volume-nocopy",
                    "--entrypoint", "/bin/sh", self.image, "-ec",
                    'mkdir /data/.node-replacement-oracle\n'
                    'index=0\n'
                    'for relative in "$@"; do\n'
                    '  mv "/data/$relative" "/data/.node-replacement-oracle/$index"\n'
                    '  index=$((index + 1))\n'
                    'done', "quarantine",
                    *(str(PurePosixPath(shard["path"]).parent) for shard in manifest.values()))

    def run(self):
        info = self.inspect("image", self.image_ref)
        self.image = info["Id"]
        self.record("candidate", {"imageRef": self.image_ref, "imageId": self.image,
                                  "repoDigests": info.get("RepoDigests", []), "architecture": info["Architecture"],
                                  "version": (info.get("Config", {}).get("Labels") or {}).get("version")})
        # These credentials exist only in the private runtime file and are removed on exit.
        descriptor = os.open(self.env_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w") as output:
            config = {"RUSTFS_ACCESS_KEY": self.access, "RUSTFS_SECRET_KEY": self.secret,
                      "RUSTFS_ADDRESS": ":9000", "RUSTFS_CONSOLE_ENABLE": "false",
                      "RUSTFS_STORAGE_CLASS_STANDARD": "EC:2", "RUSTFS_ERASURE_SET_DRIVE_COUNT": "4",
                      "RUSTFS_SCANNER_ENABLED": "false", "RUSTFS_HEAL_ENABLED": "true",
                      "RUSTFS_HEAL_INTERVAL_SECS": "5", "RUSTFS_UNSAFE_BYPASS_DISK_CHECK": "true"}
            output.write("\n".join(f"{key}={value}" for key, value in config.items()) + "\n")
        self.resources()
        subnet = f"10.{secrets.randbelow(200) + 20}.{secrets.randbelow(256)}.0/24"
        self.docker("network", "create", "--subnet", subnet, "--label", f"{LABEL}={self.run_id}", self.network)
        volumes = [self.volume(f"disk{number}") for number in range(1, 5)]
        original = [self.create(f"original{number}", volume, number) for number, volume in enumerate(volumes, 1)]
        for name in original:
            self.docker("start", name)
        self.wait_ready(original, writable=True)
        self.s3(original[0]).create_bucket(Bucket=self.bucket)
        self.write(original[0], "before-outage")
        retired, survivors = original[-1], original[:3]
        address = self.inspect("container", retired)["NetworkSettings"]["Networks"][self.network]["IPAddress"]
        self.stop([retired])
        before = [self.snapshot(name, "before-outage-physical", quiescent=name == retired) for name in original]
        layout = self.record("initial-layout", topology(before))
        retired_id = self.inspect("container", retired)["Id"]
        self.wait_ready(survivors, writable=True)
        self.record("outage", {"retiredContainerId": retired_id, "running": False,
                               "originalDisk": volumes[-1], "endpointAddress": address})
        self.write(survivors[0], "during-outage")
        self.read(survivors, "degraded-read")
        source = shard_manifest(self.snapshot(survivors[0], "degraded-authority", quiescent=False),
                                self.bucket, self.objects)
        frozen = self.snapshot(retired, "retired-after-degraded-writes")
        require(file_manifest(frozen) == file_manifest(before[-1]), "retired disk changed during degraded writes")
        require(all(not any(path.startswith(f"{self.bucket}/{key}/") for path in frozen)
                    for key, item in self.objects.items() if item["phase"] == "during-outage"),
                "retired disk already contains the offline ACKs")
        self.disconnect(retired)
        fresh_volume = self.volume("replacement-disk")
        replacement = self.create("replacement", fresh_volume, 4, ip=address)
        require(not self.snapshot(replacement, "replacement-empty"), "replacement data volume is not empty")
        require(self.inspect("container", replacement)["Id"] != retired_id and fresh_volume not in volumes,
                "replacement reused the retired process or volume")
        self.docker("start", replacement)
        active = survivors + [replacement]
        self.wait_ready(active)
        # Poll physical files before any post-replacement GET can trigger read repair.
        deadline = time.monotonic() + self.timeout
        previous = None
        while True:
            raw = self.docker("cp", replacement + ":/data/.", "-", binary=True)
            try:
                observed = shard_manifest(archive_files(raw), self.bucket, self.objects)
                require(all(shard["path"] == source[key]["path"] and shard["size"] == source[key]["size"]
                            for key, shard in observed.items()), "replacement part geometry is still incomplete")
                if observed == previous:
                    break
                previous = observed
            except ValueError as exc:
                require(time.monotonic() < deadline, "automatic replacement rebuild deadline exceeded: " + str(exc))
            require(time.monotonic() < deadline, "automatic replacement rebuild did not stabilize")
            time.sleep(2)
        addresses = {name: self.inspect("container", name)["NetworkSettings"]["Networks"][self.network]["IPAddress"]
                     for name in active}
        self.stop(active)
        repaired = [self.snapshot(name, "repaired-physical") for name in active]
        self.record("replacement-layout", topology(repaired, original=layout))
        manifests = [shard_manifest(files, self.bucket, self.objects) for files in repaired]
        for key, shard in manifests[-1].items():
            require(all(other[key]["path"] == shard["path"] and other[key]["size"] == shard["size"]
                        for other in manifests[:3]), "replacement external shard geometry differs from survivors")
            if self.objects[key]["phase"] == "before-outage":
                old = shard_manifest(before[-1], self.bucket, [key])[key]
                require(old["sha256"] == shard["sha256"], "replacement changed the original same-slot shard bytes")
        self.record("replacement-shards", manifests[-1])
        require(file_manifest(self.snapshot(retired, "retired-still-offline")) == file_manifest(frozen),
                "retired disk changed during replacement repair")

        # Preserve all repaired disks first. Quarantine the parts on two old
        # members so every later GET needs the new member's external shard.
        for volume, manifest in zip(volumes[1:3], manifests[1:3], strict=True):
            self.quarantine_shards(volume, manifest)
        verifier = []
        for slot, (old, volume) in enumerate(zip(active, volumes[:3] + [fresh_volume], strict=True), 1):
            ip = addresses[old]
            self.disconnect(old)
            verifier.append(self.create(f"verify{slot}", volume, slot, repair=False, ip=ip))
        for name in verifier[:3]:
            self.docker("start", name)
        self.wait_ready(verifier[:3])
        self.read_without_replacement(verifier[0])
        self.docker("start", verifier[-1])
        self.wait_ready(verifier)
        self.read(verifier, "replacement-required-read")
        self.stop(verifier)
        for name, manifest in zip(verifier[1:3], manifests[1:3], strict=True):
            scene = self.snapshot(name, "quarantined-source-after-reads")
            require(all(shard["path"] not in scene for shard in manifest.values()),
                    "verification repaired a quarantined source and invalidated the forced reconstruction")
        final = self.snapshot(verifier[-1], "replacement-after-forced-reads")
        require(shard_manifest(final, self.bucket, self.objects) == manifests[-1],
                "replacement shards changed during reconstruction verification")
        return {"topology": "4x1", "poolCount": 1, "parity": 2,
                "objects": self.objects, "newContainerAndEmptyVolume": True,
                "retiredNodeStayedOffline": True, "allNodeContentVerified": True,
                "replacementShardsPhysicallyPresent": True, "replacementRequiredForReconstruction": True}

def cleanup(artifacts):
    """Recover only resources bearing the exact saved run label, including after cancellation."""
    path = artifacts / "resources.json"
    if not path.exists():
        return []
    resources = json.loads(path.read_text())
    failures = []
    for kind, names in (("container", reversed(resources["containers"])),
                        ("volume", reversed(resources["volumes"])), ("network", [resources["network"]])):
        for name in names:
            result = subprocess.run(["docker", kind, "inspect", name], capture_output=True, text=True, timeout=30)
            if result.returncode:
                # A missing resource is recoverable. Daemon/transport failures remain failures.
                missing = {"container": f"no such container: {name}",
                           "volume": f"get {name}: no such volume",
                           "network": f"network {name} not found"}
                if missing[kind] in result.stderr.lower():
                    continue
                failures.append({"kind": kind, "name": name, "error": "inspect failed"})
                continue
            info = json.loads(result.stdout)[0]
            labels = info.get("Labels") if kind != "container" else info.get("Config", {}).get("Labels")
            if (labels or {}).get(LABEL) != resources["runId"]:
                failures.append({"kind": kind, "name": name, "error": "ownership label differs"})
                continue
            arguments = ["docker", kind, "rm", *(["--force"] if kind == "container" else []), name]
            removed = subprocess.run(arguments, capture_output=True, text=True, timeout=90)
            if removed.returncode:
                failures.append({"kind": kind, "name": name, "error": "removal failed"})
    (artifacts / ".runtime.env").unlink(missing_ok=True)
    return failures


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", help="locally available RustFS image; resolved to one immutable image ID")
    parser.add_argument("--artifacts-dir", type=Path, required=True)
    parser.add_argument("--timeout", type=int, default=300, help="readiness/rebuild deadline in seconds")
    parser.add_argument("--cleanup", action="store_true", help="recover only resources in the saved resources.json")
    args = parser.parse_args()
    if args.cleanup:
        failures = cleanup(args.artifacts_dir)
        print(json.dumps({"cleanup": "FAIL" if failures else "PASS", "failures": failures}))
        return bool(failures)
    require(args.image and 10 <= args.timeout <= 1800, "image and a 10..1800 second timeout are required")
    args.artifacts_dir.mkdir(parents=True, exist_ok=False)
    case = Case(args.image, args.artifacts_dir.resolve(), args.timeout)
    result = {"caseId": "node-replacement-4x1", "verdict": "RUNNING", "cleanup": "PENDING"}
    def interrupted(signum, frame):
        raise KeyboardInterrupt(f"signal {signum}")
    signal.signal(signal.SIGTERM, interrupted)
    try:
        result.update(case.run(), verdict="PASS")
    except (Exception, KeyboardInterrupt) as exc:
        message = str(exc).replace(case.access, "[REDACTED]").replace(case.secret, "[REDACTED]")
        result.update(verdict="FAIL", error={"type": type(exc).__name__, "message": message[:2000]})
    finally:
        for name in case.containers:
            try:
                log_result = subprocess.run(["docker", "logs", "--tail", "500", name], capture_output=True, timeout=30)
                logs = (log_result.stdout + log_result.stderr).decode(errors="replace")
                logs = logs.replace(case.access, "[REDACTED]").replace(case.secret, "[REDACTED]")
                (case.artifacts / (name + ".log")).write_text(logs)
            except Exception:
                pass
        if result["verdict"] == "FAIL":
            for name in case.containers:
                if "-quarantine-" in name:
                    continue
                try:
                    case.stop([name])
                    case.snapshot(name, "failure-physical")
                except Exception as exc:
                    case.record("failure-capture-error", {"node": name, "error": type(exc).__name__})
        try:
            failures = cleanup(case.artifacts)
            result.update(cleanup="FAIL" if failures else "PASS", cleanupFailures=failures)
        except Exception as exc:
            result.update(cleanup="FAIL", cleanupError=type(exc).__name__)
        case.env_file.unlink(missing_ok=True)
        save(case.artifacts / "result.json", result)
    print(json.dumps(result, indent=2))
    return result["verdict"] != "PASS" or result["cleanup"] != "PASS"


if __name__ == "__main__":
    raise SystemExit(main())
