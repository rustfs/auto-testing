// Read-only inspector linked to the exact candidate's rustfs-filemeta crate.
// Do not print inline bytes, user metadata, encryption envelopes, or IAM data.
use rustfs_filemeta::FileMeta;
use rustfs_utils::HashAlgorithm;
use serde_json::json;

fn bounded_file(path: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.is_file() || metadata.len() > 64 * 1024 * 1024 {
        return Err("expected bounded, non-symlink evidence file".into());
    }
    Ok(std::fs::read(path)?)
}

fn sha256(bytes: &[u8]) -> String {
    HashAlgorithm::SHA256
        .hash_encode(bytes)
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn verify_framing(
    bytes: &[u8],
    logical_size: usize,
    block_size: usize,
    data_blocks: usize,
    legacy: bool,
    algorithm: &HashAlgorithm,
) -> Result<usize, Box<dyn std::error::Error>> {
    if logical_size == 0 || block_size == 0 || data_blocks == 0 {
        return Err("positive bitrot verification requires supported nonempty geometry".into());
    }
    if !matches!(
        algorithm,
        HashAlgorithm::HighwayHash256S | HashAlgorithm::HighwayHash256SLegacy
    ) {
        return Err("unsupported bitrot framing; no integrity claim".into());
    }
    let mut remaining = logical_size;
    let mut offset: usize = 0;
    let mut blocks: usize = 0;
    while remaining > 0 {
        let logical = remaining.min(block_size);
        let length = logical.div_ceil(data_blocks);
        let length = if legacy { (length + 1) & !1 } else { length };
        let end = offset
            .checked_add(32 + length)
            .ok_or("part size overflow")?;
        let block = bytes.get(offset..end).ok_or("short shard read")?;
        if algorithm.hash_encode(&block[32..]).as_ref() != &block[..32] {
            return Err(format!("bitrot hash mismatch in block {blocks}").into());
        }
        offset = end;
        remaining -= logical;
        blocks += 1;
    }
    if offset != bytes.len() {
        return Err("unexpected trailing shard bytes".into());
    }
    Ok(blocks)
}

fn verify_inline(args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    if args.len() != 6 {
        return Err("usage: --verify-inline XL_META BUCKET KEY VERSION_ID".into());
    }
    let bytes = bounded_file(&args[2])?;
    let meta = FileMeta::load(&bytes)?;
    meta.data.validate()?;
    let decoded = meta.into_file_info_versions(&args[3], &args[4], true)?;
    let matches: Vec<_> = decoded
        .versions
        .iter()
        .filter(|info| {
            info.version_id
                .filter(|id| !id.is_nil())
                .map(|id| id.to_string())
                .unwrap_or_else(|| "null".to_owned())
                == args[5]
        })
        .collect();
    if matches.len() != 1 {
        return Err("inline verification requires one exact metadata version".into());
    }
    let info = matches[0];
    if info.deleted
        || !info.inline_data()
        || info.parts.len() != 1
        || info.parts[0].size == 0
        || usize::try_from(info.parts[0].actual_size).ok() != Some(info.parts[0].size)
        || usize::try_from(info.size).ok() != Some(info.parts[0].size)
    {
        return Err("unsupported inline version or part identity".into());
    }
    let data = meta.data.find(&args[5])?.ok_or("inline value is missing")?;
    let algorithm = if info.uses_legacy_checksum {
        HashAlgorithm::HighwayHash256SLegacy
    } else {
        info.erasure
            .get_checksum_info(info.parts[0].number)
            .algorithm
    };
    let blocks = verify_framing(
        &data,
        usize::try_from(info.parts[0].size)?,
        info.erasure.block_size,
        info.erasure.data_blocks,
        info.uses_legacy_checksum,
        &algorithm,
    )?;
    println!(
        "{}",
        json!({"schemaVersion": 1, "bucket": args[3], "key": args[4],
        "versionId": args[5], "physicalBitrotVerified": true, "ecReconstructionVerified": false,
        "algorithm": algorithm, "blocksVerified": blocks, "inlineLength": data.len(), "inlineSha256": sha256(&data)})
    );
    Ok(())
}

// This path checks stored framing without invoking the service or repairing
// anything. EC reconstruction is checked separately against the frozen shard.
fn verify_part(args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    if args.len() != 8 {
        return Err(
            "usage: --verify-part XL_META BUCKET KEY PART_FILE VERSION_ID PART_NUMBER".into(),
        );
    }
    let meta = FileMeta::load(&bounded_file(&args[2])?)?;
    let versions = meta.into_file_info_versions(&args[3], &args[4], true)?;
    let matches: Vec<_> = versions
        .versions
        .iter()
        .filter(|info| {
            info.version_id
                .filter(|id| !id.is_nil())
                .map(|id| id.to_string())
                .unwrap_or_else(|| "null".to_owned())
                == args[6]
        })
        .collect();
    if matches.len() != 1 {
        return Err("part verification requires one exact metadata version".into());
    }
    let info = matches[0];
    let number: usize = args[7].parse()?;
    let part = info
        .parts
        .iter()
        .find(|part| part.number == number)
        .ok_or("part is absent from metadata")?;
    let path = std::path::Path::new(&args[5]);
    let data_dir = info
        .data_dir
        .ok_or("external part has no data directory")?
        .to_string();
    if info.deleted
        || info.inline_data()
        || info.erasure.data_blocks == 0
        || info.erasure.block_size == 0
        || part.size <= 0
        || path.file_name().and_then(|name| name.to_str())
            != Some(format!("part.{number}").as_str())
        || path
            .parent()
            .and_then(|parent| parent.file_name())
            .and_then(|name| name.to_str())
            != Some(data_dir.as_str())
    {
        return Err("unsupported external part identity or geometry".into());
    }
    let algorithm = if info.uses_legacy_checksum {
        HashAlgorithm::HighwayHash256SLegacy
    } else {
        info.erasure.get_checksum_info(number).algorithm
    };
    let bytes = bounded_file(&args[5])?;
    let blocks = verify_framing(
        &bytes,
        usize::try_from(part.size)?,
        info.erasure.block_size,
        info.erasure.data_blocks,
        info.uses_legacy_checksum,
        &algorithm,
    )?;
    println!(
        "{}",
        json!({"schemaVersion": 1, "bucket": args[3], "key": args[4],
        "versionId": args[6], "partNumber": number, "dataDir": data_dir,
        "algorithm": algorithm, "blocksVerified": blocks, "physicalSize": bytes.len(),
        "physicalBitrotVerified": true, "ecReconstructionVerified": false})
    );
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).is_some_and(|value| value == "--verify-part") {
        return verify_part(&args);
    }
    if args.get(1).is_some_and(|value| value == "--verify-inline") {
        return verify_inline(&args);
    }
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
            let inline = decoded_inline
                .as_ref()
                .filter(|data| !data.is_empty())
                .map(|data| {
                    let offsets: Vec<_> = bytes
                        .windows(data.len())
                        .enumerate()
                        .filter_map(|(offset, candidate)| {
                            (candidate == data.as_slice()).then_some(offset)
                        })
                        .collect();
                    json!({
                        "length": data.len(),
                        "sha256": sha256(data),
                        "rawOccurrenceCount": offsets.len(),
                        "rawOffset": (offsets.len() == 1).then_some(offsets[0])
                    })
                });
            Ok(json!({
                "versionId": info.version_id.map(|id| id.to_string()),
                "isLatest": info.is_latest, "deleted": info.deleted,
                "dataDir": info.data_dir.map(|id| id.to_string()), "size": info.size,
                "usesLegacyChecksum": info.uses_legacy_checksum,
                "inline": inline,
                "erasure": info.erasure, "role": role,
                "parts": info.parts.iter().map(|part| json!({
                    "number": part.number, "size": part.size, "actualSize": part.actual_size,
                    "etag": part.etag, "integrityDescriptorPresent": part.integrity.is_some()
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
