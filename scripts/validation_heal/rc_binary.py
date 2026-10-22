"""Prepare the released rc with truthful J01 runtime status rendering."""

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import tarfile


RELEASE = "v0.1.36"
SOURCE = "9131de5b0bcb3a28bb0d32b5dce2c86cbbf4724e"
# The official musl build avoids a dependency on the runner's glibc version.
URL = "https://github.com/rustfs/cli/releases/download/v0.1.36/rustfs-cli-linux-amd64-v0.1.36.tar.gz"
ARCHIVE_SHA256 = "4a8128911ccad4e7b481f26635a4cfd1ec064412210526e57ad2c748d356f3b7"
BINARY_SHA256 = "15490337489f5c1bb2d33f77e308be52ac29adaa27069d7c77f0d808b165f786"
MAX_BYTES = 64 * 1024**2


def unpack(archive, directory):
    if archive.stat().st_size > MAX_BYTES or hashlib.sha256(archive.read_bytes()).hexdigest() != ARCHIVE_SHA256:
        raise ValueError("released rc archive digest differs")
    with tarfile.open(archive) as incoming:
        members = incoming.getmembers()
        if (len(members) != 3 or {m.name for m in members} != {"rc", "LICENSE-MIT", "LICENSE-APACHE"}
                or any(not m.isfile() or not 0 < m.size <= MAX_BYTES for m in members)
                or sum(m.size for m in members) > MAX_BYTES):
            raise ValueError("released rc archive has unexpected members")
        binary = incoming.extractfile(next(m for m in members if m.name == "rc")).read()
    if hashlib.sha256(binary).hexdigest() != BINARY_SHA256:
        raise ValueError("released rc binary digest differs")
    if binary[:6] != b"\x7fELF\x02\x01" or binary[18:20] != b"\x3e\x00":
        raise ValueError("released rc is not Linux x86-64 ELF")
    target = directory / "rc"
    with target.open("xb") as output:
        output.write(binary)
    target.chmod(0o700)
    return target.resolve()


def prepare(directory):
    directory.mkdir(mode=0o700, parents=True, exist_ok=False)
    archive = directory / "rc.tar.gz"
    subprocess.run(["curl", "--fail", "--silent", "--show-error", "--location", "--proto", "=https",
                    "--proto-redir", "=https", "--connect-timeout", "15", "--max-time", "180",
                    "--max-filesize", str(MAX_BYTES), "--output", str(archive), URL], check=True, timeout=190)
    target = unpack(archive, directory)
    result = subprocess.run([str(target), "--version"], capture_output=True, text=True, check=True, timeout=10)
    manifest = {"release": RELEASE, "sourceCommit": SOURCE, "target": "x86_64-unknown-linux-musl", "archiveUrl": URL,
                "archiveSha256": ARCHIVE_SHA256, "binarySha256": BINARY_SHA256,
                "binaryPath": str(target), "versionOutput": result.stdout.strip(),
                "role": "validation client; not the RustFS service under test"}
    with (directory / "manifest.json").open("x") as output:
        json.dump(manifest, output, indent=2)
        output.write("\n")
    return target


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    print(prepare(parser.parse_args().directory))
