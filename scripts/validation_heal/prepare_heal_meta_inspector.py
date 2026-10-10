#!/usr/bin/env python3
"""Prepare (do not silently build) a read-only metadata inspector for one SHA."""

import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess


def quorum_contract(source: Path, commit: str) -> dict:
    """Freeze only recognized candidate rules; unfamiliar source stays unsupported."""
    paths = ("rustfs/src/error.rs", "crates/ecstore/src/set_disk/metadata.rs")
    contents = {path: (source / path).read_text() for path in paths}
    code_only = {path: re.sub(r'"(?:\\.|[^"\\])*"', '""',
                    re.sub(r"/\*.*?\*/|//[^\n]*", "", value, flags=re.S)) for path, value in contents.items()}
    compact = {path: re.sub(r"\s+", "", value) for path, value in code_only.items()}
    metadata = compact[paths[1]]
    signature = "pub(super)fnobject_quorum_from_meta("
    start = metadata.find(signature)
    function = ""
    if start >= 0:
        opening = metadata.find("{", start)
        if opening >= 0:
            depth = 0
            for index in range(opening, len(metadata)):
                depth += (metadata[index] == "{") - (metadata[index] == "}")
                if depth == 0:
                    function = metadata[opening:index + 1]
                    break
    errors = compact[paths[0]]
    known_read = ("letdata_blocks=parts_metadata.len()asi32-parity_blocks;" in function
                  and function.endswith("Ok((data_blocks,write_quorum))}"))
    known_error = ('StorageError::ErasureReadQuorum|StorageError::InsufficientReadQuorum(_,_)=>{'
                   'S3ErrorCode::Custom(SLOW_DOWN_READ_CODE.into())}' in errors
                   and 'S3ErrorCode::Custom(custom)if&**custom==SLOW_DOWN_READ_CODE=>Some(StatusCode::SERVICE_UNAVAILABLE)' in errors)
    code = re.search(r'const\s+SLOW_DOWN_READ_CODE\s*:\s*&str\s*=\s*"([A-Za-z0-9]+)"\s*;', contents[paths[0]])
    return {"candidateCommit": commit, "recognized": bool(known_read and known_error and code),
            "readQuorumRule": "decoded-data-blocks" if known_read else None,
            "unrecoverableGet": {"httpStatus": 503, "code": code[1]} if known_error and code else None,
            "sourceFiles": {path: hashlib.sha256(value.encode()).hexdigest() for path, value in contents.items()},
            "extractorSourceSha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}


def compact_rust(value: str) -> str:
    value = re.sub(r"/\*.*?\*/|//[^\n]*", "", value, flags=re.S)
    return re.sub(r"\s+", "", value)


def rust_body(value: str, signature: str) -> str:
    start = value.find(signature)
    if start < 0:
        return ""
    opening = value.find("{", start)
    if opening < 0:
        return ""
    depth = 0
    for index in range(opening, len(value)):
        depth += (value[index] == "{") - (value[index] == "}")
        if depth == 0:
            return value[opening:index + 1]
    return ""


def size_contract(source: Path, commit: str) -> dict:
    """Recognize the bounded untransformed PUT policy, preserving source digests."""
    paths = ("crates/ecstore/src/config/storageclass.rs", "crates/ecstore/src/set_disk/mod.rs",
             "crates/ecstore/src/set_disk/ops/object.rs", "crates/ecstore/src/erasure/coding/erasure.rs")
    contents = {path: (source / path).read_text() for path in paths}
    storage, admission, put, erasure = (compact_rust(contents[path]) for path in paths)
    default = re.search(r"pubstaticDEFAULT_INLINE_BLOCK:usize=([0-9]+)\*([0-9]+);", storage)
    should = rust_body(storage, "pubfnshould_inline(")
    budget = rust_body(storage, "pub(crate)fneffective_inline_block(")
    shard = rust_body(erasure, "pubfncalc_shard_size(")
    recognized = bool(default and
        "constDEFAULT_INLINE_OBJECT_BUDGET:usize=2*DEFAULT_INLINE_BLOCK;" in storage and
        should == "{ifshard_size<0||data_shards==0{returnfalse;}letshard_size=shard_sizeasusize;"
                  "letinline_block=self.effective_inline_block(data_shards);ifversioned{shard_size<=inline_block/8}"
                  "else{shard_size<=inline_block}}" and
        budget == "{ifdata_shards==0{return0;}ifself.initialized&&self.inline_block_explicit{self.inline_block}"
                  "else{DEFAULT_INLINE_OBJECT_BUDGET.div_ceil(data_shards).min(DEFAULT_INLINE_BLOCK)}}" and
        "ifstored_size>=0{returnerasure.shard_file_size(stored_size);}" in rust_body(admission, "fninline_admission_shard_size(") and
        "letis_inline_buffer=storage_class_config.should_inline(inline_admission_shard_size(&erasure,put_object_size,data.actual_size()),"
        "erasure.data_shards,opts.versioned,)&&usize::try_from(inline_admission_size).is_ok_and(|size|size<=erasure.block_size);" in put and
        shard == "{block_size.div_ceil(data_shards)}")
    return {"candidateCommit": commit, "recognized": recognized, "scope": "known-size untransformed unversioned PUT; current shard codec",
        "defaultInlineBlock": int(default[1]) * int(default[2]) if default else None,
        "defaultObjectBudgetFactor": 2, "inlineEnvironment": "RUSTFS_STORAGE_CLASS_INLINE_BLOCK",
        "inlineRule": "ceil(size/dataBlocks)<=effectiveInlineBlock AND size<=actualECBlock",
        "effectiveDefaultRule": "min(ceil(2*defaultInlineBlock/dataBlocks),defaultInlineBlock)",
        "sourceFiles": {path: hashlib.sha256(value.encode()).hexdigest() for path, value in contents.items()},
        "extractorSourceSha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}


def prepare(source: Path, commit: str, output: Path) -> None:
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise ValueError("full candidate commit required")
    source = source.resolve(strict=True)
    head = subprocess.run(["git", "-C", str(source), "rev-parse", "HEAD"], check=True,
                          text=True, capture_output=True).stdout.strip()
    dirty = subprocess.run(["git", "-C", str(source), "status", "--porcelain", "--untracked-files=no"],
                           check=True, text=True, capture_output=True).stdout
    if head != commit or dirty.strip():
        raise ValueError("inspector requires an unmodified checkout of the exact candidate SHA")
    crate = source / "crates/filemeta"
    if not (crate / "Cargo.toml").is_file():
        raise ValueError("candidate has no rustfs-filemeta crate")
    output.mkdir(parents=True, exist_ok=False)
    (output / "src").mkdir()
    helper = Path(__file__).resolve().parent / "tools/heal_meta_inspect.rs"
    text = helper.read_text()
    (output / "src/main.rs").write_text(text)
    manifest = ('[package]\nname = "heal-meta-inspect"\nversion = "0.1.0"\nedition = "2024"\n'
                '[workspace]\n[dependencies]\nserde_json = "1"\n'
                'rustfs-filemeta = { path = ' + json.dumps(str(crate)) + ' }\n'
                'rustfs-utils = { path = ' + json.dumps(str(source / "crates/utils"))
                + ', default-features = false, features = ["hash"] }\n')
    (output / "Cargo.toml").write_text(manifest)
    (output / "source.json").write_text(json.dumps({"candidateCommit": head,
        "candidateLockSha256": hashlib.sha256((source / "Cargo.lock").read_bytes()).hexdigest(),
        "helperSourceSha256": hashlib.sha256(text.encode()).hexdigest(),
        "quorumContract": quorum_contract(source, head),
        "sizeContract": size_contract(source, head),
        "role": "read-only decoder; not the RustFS service under test",
        "built": False}, indent=2) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    prepare(args.source, args.commit, args.out)
    print(f"Prepared {args.out}; build on an approved host, then record its Cargo.lock and binary SHA-256.")


if __name__ == "__main__":
    main()
