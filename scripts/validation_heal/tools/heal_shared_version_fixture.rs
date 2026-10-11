// Encode a controlled C10 shared-reference fixture with the exact candidate
// codec. The node verifies both original physical generations before calling
// this stdin/stdout encoder; this program has no filesystem mutation API.
use rustfs_filemeta::FileMeta;
use std::io::{Read, Write};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 5 {
        return Err(
            "usage: shared-fixture BUCKET KEY SOURCE_VERSION DEST_VERSION < XL_META".into(),
        );
    }
    let mut original = Vec::new();
    std::io::stdin()
        .take(64 * 1024 * 1024 + 1)
        .read_to_end(&mut original)?;
    let (encoded, receipt) = encode(&original, &args[1], &args[2], &args[3], &args[4])?;
    eprintln!("{}", receipt);
    std::io::stdout().write_all(&encoded)?;
    Ok(())
}

fn encode(
    original: &[u8],
    bucket: &str,
    key: &str,
    source_version: &str,
    destination_version: &str,
) -> Result<(Vec<u8>, serde_json::Value), Box<dyn std::error::Error>> {
    if original.is_empty() || original.len() > 64 * 1024 * 1024 {
        return Err("requires bounded original metadata".into());
    }
    let mut meta = FileMeta::load(original)?;
    meta.data.validate()?;
    let decoded = meta.into_file_info_versions(bucket, key, true)?;
    let lookup = |id: &str| {
        decoded
            .versions
            .iter()
            .filter(|version| {
                version
                    .version_id
                    .is_some_and(|value| value.to_string() == id)
            })
            .collect::<Vec<_>>()
    };
    let sources = lookup(source_version);
    let destinations = lookup(destination_version);
    if sources.len() != 1 || destinations.len() != 1 {
        return Err("requires exactly one existing version for each ACK identity".into());
    }
    let src = sources[0];
    let dst = destinations[0];
    if src.version_id == dst.version_id
        || src.deleted
        || dst.deleted
        || src.is_remote()
        || dst.is_remote()
        || src.inline_data()
        || dst.inline_data()
        || src.size <= 0
        || src.size != dst.size
        || src.parts.is_empty()
        || src.parts.len() != dst.parts.len()
        || src.erasure.algorithm != dst.erasure.algorithm
        || src.erasure.data_blocks != dst.erasure.data_blocks
        || src.erasure.parity_blocks != dst.erasure.parity_blocks
        || src.erasure.block_size != dst.erasure.block_size
        || src.erasure.index != dst.erasure.index
        || src.erasure.distribution != dst.erasure.distribution
        || src.erasure.checksums != dst.erasure.checksums
        || src.parts.iter().zip(&dst.parts).any(|(left, right)| {
            left.etag != right.etag
                || left.number != right.number
                || left.size != right.size
                || left.actual_size != right.actual_size
                || left.index != right.index
                || left.checksums != right.checksums
                || left.error != right.error
        })
    {
        return Err("requires compatible distinct external plaintext versions".into());
    }
    let src_dir = src.data_dir.ok_or("source data directory absent")?;
    let dst_dir = dst.data_dir.ok_or("destination data directory absent")?;
    if src_dir.is_nil() || dst_dir.is_nil() || src_dir == dst_dir {
        return Err("requires two distinct original physical generations".into());
    }
    let before = meta
        .versions
        .iter()
        .map(|version| version.parse_version_meta())
        .collect::<Result<Vec<_>, _>>()?;
    let mut changed = before
        .iter()
        .find(|version| version.get_version_id() == dst.version_id)
        .ok_or("shallow destination absent")?
        .clone();
    let source_object = before
        .iter()
        .find(|version| version.get_version_id() == src.version_id)
        .and_then(|version| version.object.as_ref())
        .ok_or("source is not an object")?;
    let changed_object = changed
        .object
        .as_mut()
        .ok_or("destination is not an object")?;
    changed_object.data_dir = Some(src_dir);
    // A shared directory must also share the source commitment descriptor.
    // Preserve all other destination metadata, including its ACK identity.
    let is_integrity = |key: &str| {
        key == "x-rustfs-internal-shard-integrity-v1"
            || key == "x-minio-internal-shard-integrity-v1"
    };
    changed_object.meta_sys.retain(|key, _| !is_integrity(key));
    for (key, value) in &source_object.meta_sys {
        if is_integrity(key) {
            changed_object.meta_sys.insert(key.clone(), value.clone());
        }
    }
    meta.add_version_filemata(changed.clone())?;
    let encoded = meta.marshal_msg()?;
    if encoded.len() > 64 * 1024 * 1024 {
        return Err("encoded fixture exceeds metadata budget".into());
    }
    let reloaded = FileMeta::load(&encoded)?;
    if reloaded.versions.len() != before.len() {
        return Err("fixture changed the original version set".into());
    }
    for version in &reloaded.versions {
        let value = version.parse_version_meta()?;
        let old = before
            .iter()
            .find(|item| item.get_version_id() == value.get_version_id())
            .ok_or("fixture invented a version")?;
        let expected = if value.get_version_id() == dst.version_id {
            &changed
        } else {
            old
        };
        if &value != expected {
            return Err("fixture changed unrelated version metadata".into());
        }
    }
    let receipt = serde_json::json!({"sourceVersion":source_version,"destinationVersion":destination_version,
        "sharedDataDir":src_dir.to_string(),"retiredDataDir":dst_dir.to_string(),
        "versionCount":before.len(),"filesystemMutationPerformed":false,"physicalByteEqualityClaimed":false});
    Ok((encoded, receipt))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_filemeta::{ChecksumInfo, ErasureInfo, FileInfo, ObjectPartInfo};
    use rustfs_utils::HashAlgorithm;

    const SOURCE: &str = "00000000-0000-0000-0000-000000000001";
    const DESTINATION: &str = "00000000-0000-0000-0000-000000000002";
    const SOURCE_DIR: &str = "00000000-0000-0000-0000-000000000003";
    const DESTINATION_DIR: &str = "00000000-0000-0000-0000-000000000004";

    fn fixture() -> FileMeta {
        let mut meta = FileMeta::new();
        for (version, directory) in [(SOURCE, SOURCE_DIR), (DESTINATION, DESTINATION_DIR)] {
            let info = FileInfo {
                volume: "bucket".to_owned(),
                name: "shared.bin".to_owned(),
                version_id: Some(version.parse().unwrap()),
                data_dir: Some(directory.parse().unwrap()),
                size: 1024,
                parts: vec![ObjectPartInfo {
                    number: 1,
                    size: 1024,
                    actual_size: 1024,
                    etag: "identical-ack-etag".to_owned(),
                    ..Default::default()
                }],
                erasure: ErasureInfo {
                    algorithm: "ReedSolomon".to_owned(),
                    data_blocks: 2,
                    parity_blocks: 2,
                    block_size: 1024 * 1024,
                    index: 1,
                    distribution: vec![1, 2, 3, 4],
                    checksums: vec![ChecksumInfo {
                        part_number: 1,
                        algorithm: HashAlgorithm::HighwayHash256S,
                        hash: Default::default(),
                    }],
                },
                ..Default::default()
            };
            meta.add_version(info).unwrap();
        }
        meta
    }

    #[test]
    fn exact_existing_versions_share_one_directory_without_changing_other_metadata() {
        let original = fixture().marshal_msg().unwrap();
        let source_before = FileMeta::load(&original)
            .unwrap()
            .into_file_info_versions("bucket", "shared.bin", true)
            .unwrap()
            .versions
            .into_iter()
            .find(|version| version.version_id == Some(SOURCE.parse().unwrap()))
            .unwrap();
        let (encoded, receipt) =
            encode(&original, "bucket", "shared.bin", SOURCE, DESTINATION).unwrap();
        let decoded = FileMeta::load(&encoded)
            .unwrap()
            .into_file_info_versions("bucket", "shared.bin", true)
            .unwrap();
        assert_eq!(decoded.versions.len(), 2);
        assert!(
            decoded
                .versions
                .iter()
                .all(|version| version.data_dir == Some(SOURCE_DIR.parse().unwrap()))
        );
        assert_eq!(receipt["retiredDataDir"], DESTINATION_DIR);
        assert_eq!(receipt["filesystemMutationPerformed"], false);
        assert_eq!(receipt["physicalByteEqualityClaimed"], false);
        assert_eq!(
            decoded
                .versions
                .into_iter()
                .find(|version| version.version_id == Some(SOURCE.parse().unwrap()))
                .unwrap(),
            source_before
        );
    }

    #[test]
    fn malformed_absent_same_and_already_shared_versions_are_refused() {
        assert!(encode(b"invalid", "bucket", "shared.bin", SOURCE, DESTINATION).is_err());
        let original = fixture().marshal_msg().unwrap();
        assert!(encode(&original, "bucket", "shared.bin", SOURCE, SOURCE).is_err());
        assert!(encode(&original, "bucket", "shared.bin", SOURCE, SOURCE_DIR).is_err());
        let (shared, _) = encode(&original, "bucket", "shared.bin", SOURCE, DESTINATION).unwrap();
        assert!(encode(&shared, "bucket", "shared.bin", SOURCE, DESTINATION).is_err());
    }

    #[test]
    fn incompatible_part_and_deleted_destination_are_refused() {
        for deleted in [false, true] {
            let mut meta = fixture();
            let mut info = meta
                .into_file_info_versions("bucket", "shared.bin", true)
                .unwrap()
                .versions
                .into_iter()
                .find(|version| version.version_id == Some(DESTINATION.parse().unwrap()))
                .unwrap();
            if deleted {
                info.deleted = true;
            } else {
                info.parts[0].etag = "different-payload-ack".to_owned();
            }
            meta.add_version(info).unwrap();
            assert!(
                encode(
                    &meta.marshal_msg().unwrap(),
                    "bucket",
                    "shared.bin",
                    SOURCE,
                    DESTINATION
                )
                .is_err()
            );
        }
    }
}
