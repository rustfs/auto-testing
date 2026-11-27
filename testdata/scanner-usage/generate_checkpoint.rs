#[tokio::test]
async fn seed_helm_checkpoint_restore() {
    let root =
        std::path::PathBuf::from(std::env::var_os("USAGE_22_OUTPUT").expect("fixture output"));
    assert!(!root.exists(), "fixture output must be new");
    let store = setup_scanner_cycle_store_at_path_with_layout(&root, false, 1, 1, 1).await;
    store
        .make_bucket("bucket", &Default::default())
        .await
        .unwrap();
    let mut object = PutObjReader::from_vec(vec![7; 16]);
    store
        .put_object("bucket", "item", &mut object, &ObjectOptions::default())
        .await
        .unwrap();
    crate::scanner_io::tests::wait_for_namespace_commit_tails(store.as_ref()).await;

    let mut checkpoint = empty_bucket_checkpoint();
    let suffix = "x".repeat(160);
    for i in 0..4096 {
        checkpoint.replace(
            &format!("bucket/{i:05}-{suffix}"),
            "bucket",
            DataUsageEntry {
                size: i + 1,
                objects: 1,
                versions: 1,
                ..Default::default()
            },
        );
    }
    let bytes = checkpoint.marshal_msg().unwrap().len();
    assert!(bytes > 1024 * 1024);
    let revisions = DataUsageCache::read_revisions(store.clone(), NAME)
        .await
        .unwrap();
    checkpoint
        .save_with_revisions_for_epoch(store, NAME, &revisions, 0)
        .await
        .unwrap();
    println!("paged checkpoint: {bytes} bytes; expected current bucket: 1 object, 16 bytes");
}
