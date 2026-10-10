// Read-only inspector linked to the exact candidate's rustfs-filemeta crate.
// Do not print inline bytes, user metadata, encryption envelopes, or IAM data.
use rustfs_filemeta::FileMeta;
use serde_json::json;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 4 {
        return Err("usage: heal-meta-inspect XL_META_FILE BUCKET KEY".into());
    }
    let metadata = std::fs::symlink_metadata(&args[1])?;
    if !metadata.is_file() || metadata.len() > 64 * 1024 * 1024 {
        return Err("expected bounded, non-symlink xl.meta evidence copy".into());
    }
    let bytes = std::fs::read(&args[1])?;
    let meta = FileMeta::load(&bytes)?;
    if meta.versions.is_empty() {
        return Err("empty metadata is not an authoritative version manifest".into());
    }
    let decoded = meta.into_file_info_versions(&args[2], &args[3], true)?;
    meta.data.validate()?;
    let versions: Result<Vec<_>, Box<dyn std::error::Error>> = decoded
        .versions
        .iter()
        .map(|info| -> Result<_, Box<dyn std::error::Error>> {
            let role = if info.erasure.index == 0 {
                "not-an-erasure-shard"
            } else if info.erasure.index <= info.erasure.data_blocks {
                "data"
            } else {
                "parity"
            };
            // Locate only the exact decoded inline value. Exposing the bytes
            // would leak user content; offset/length plus an occurrence count
            // lets a bounded injector target the value instead of guessing at
            // an xl.meta position.
            let data_key = info
                .version_id
                .filter(|version_id| !version_id.is_nil())
                .map(|version_id| version_id.to_string())
                .unwrap_or_else(|| "null".to_string());
            let decoded_inline = if info.inline_data() {
                meta.data.find(&data_key)?
            } else {
                None
            };
            let inline = decoded_inline.as_ref().filter(|data| !data.is_empty()).map(|data| {
                let offsets: Vec<_> = bytes
                    .windows(data.len())
                    .enumerate()
                    .filter_map(|(offset, candidate)| (candidate == data.as_slice()).then_some(offset))
                    .collect();
                json!({
                    "length": data.len(),
                    "rawOccurrenceCount": offsets.len(),
                    "rawOffset": (offsets.len() == 1).then_some(offsets[0])
                })
            });
            Ok(json!({
                "versionId": info.version_id.map(|id| id.to_string()),
                "isLatest": info.is_latest, "deleted": info.deleted,
                "dataDir": info.data_dir.map(|id| id.to_string()), "size": info.size,
                "inline": inline,
                "erasure": info.erasure, "role": role,
                "parts": info.parts.iter().map(|part| json!({
                    "number": part.number, "size": part.size, "actualSize": part.actual_size,
                    "etag": part.etag
                })).collect::<Vec<_>>()
            }))
        })
        .collect();
    let versions = versions?;
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "schemaVersion": 1, "bucket": args[2], "key": args[3],
            "metadataVersion": meta.meta_ver, "versions": versions,
            "physicalBitrotVerified": false, "authorityQuorumProved": false
        }))?
    );
    Ok(())
}
