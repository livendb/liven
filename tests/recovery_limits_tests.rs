//! Recovery and index-budget behavior.
//!
//! The index charge is `len("stream:key") + 64 + 64`. Recovery must not refuse
//! to open a directory that exceeds the configured budget: it loads the data,
//! warns, and rejects only new keys. `RecoveryMode::Strict` is the explicit
//! opt-in to the old refuse-to-open behavior.
use liven::Liven;
use liven::error::LivenError;
use liven::storage::{RecoveryMode, StorageEngine};
use liven::types::DataValue;
use tempfile::tempdir;

const MB: u64 = 1024 * 1024;
const SEG: u64 = 64 * MB;

fn open(
    dir: &std::path::Path,
    streams: usize,
    index_mb: u64,
) -> liven::error::Result<StorageEngine> {
    StorageEngine::new_with_limits(dir, SEG, RecoveryMode::Default, streams, index_mb * MB, 512)
}

fn open_tuning(
    dir: &std::path::Path,
    mode: RecoveryMode,
    index_mb: u64,
) -> liven::error::Result<StorageEngine> {
    StorageEngine::new_with_tuning(
        dir,
        SEG,
        mode,
        1024,
        index_mb * MB,
        512,
        10_000,
        4_096,
        5_000,
        2_048,
    )
}

#[test]
fn streams_above_default_reopen_with_configured_limit() {
    let dir = tempdir().unwrap();
    {
        let e = open(dir.path(), 1024, 256).unwrap();
        for s in 0..40 {
            e.append(&format!("s{s}"), "k", DataValue::String("v".into()), false)
                .unwrap();
        }
    }
    // Same configured limit -> recovery succeeds.
    let e = open(dir.path(), 1024, 256).unwrap();
    assert!(e.list_streams().len() >= 40, "all streams recovered");

    // Limit LOWER than the data -> the same clear error.
    let err = match open(dir.path(), 32, 256) {
        Ok(_) => panic!("40 streams must not recover under max_streams=32"),
        Err(e) => e,
    };
    let msg = err.to_string();
    assert!(
        msg.to_lowercase().contains("stream"),
        "unexpected error: {msg}"
    );
}

/// Build 120k keys with a 60-byte stream and 32-byte key: old accounting
/// charges 93+64 = 157 B/key (~18.8 MB), current accounting charges 93+128 =
/// 221 B/key (~26.5 MB).
fn build_old_accounting_dataset(dir: &std::path::Path, sname: &str) {
    let key = |i: u32| format!("k{i:031}");
    let e = open(dir, 1024, 256).unwrap();
    let batch: Vec<(String, String, DataValue)> = (0..120_000u32)
        .map(|i| (sname.to_string(), key(i), DataValue::String("x".into())))
        .collect();
    e.append_batch(batch).unwrap();
}

#[test]
fn index_above_limit_reopens_and_rejects_only_new_keys() {
    let dir = tempdir().unwrap();
    let sname = "b".repeat(60);
    let key = |i: u32| format!("k{i:031}");
    build_old_accounting_dataset(dir.path(), &sname);

    // 16 MiB fits the old charge but not the current 221 B/key charge.
    let e = open(dir.path(), 1024, 16).expect("over-limit recovery must succeed");
    assert!(
        e.index_usage().high_water_reached,
        "high-water must be reported when over budget"
    );
    assert_eq!(
        e.get(&sname, &key(500)).unwrap().unwrap().value,
        DataValue::String("x".into()),
        "reads must keep working"
    );

    // Overwrites and deletes are allowed.
    e.append(&sname, &key(500), DataValue::String("y".into()), false)
        .unwrap();
    e.append_tombstone_batch(&sname, &[key(501)]).unwrap();

    // A new key is rejected with the typed error.
    let err = e
        .append(&sname, "brand-new", DataValue::String("z".into()), false)
        .expect_err("new keys must be rejected over budget");
    assert!(
        matches!(err, LivenError::IndexRamLimitExceeded { .. }),
        "expected typed IndexRamLimitExceeded, got: {err:?}"
    );

    // Strict recovery is the explicit refuse-to-open opt-in.
    drop(e);
    assert!(
        open_tuning(dir.path(), RecoveryMode::Strict, 16).is_err(),
        "Strict mode must refuse an over-budget index"
    );
}

#[test]
fn over_limit_mixed_batch_rejects_only_the_new_key() {
    let dir = tempdir().unwrap();
    let sname = "b".repeat(60);
    let key = |i: u32| format!("k{i:031}");
    build_old_accounting_dataset(dir.path(), &sname);

    let e = open(dir.path(), 1024, 16).unwrap();

    // Replace an existing key.
    e.replace(&sname, &key(0), DataValue::String("replaced".into()))
        .unwrap();
    // Delete an existing key.
    e.append_tombstone_batch(&sname, &[key(1)]).unwrap();

    // Existing + one new key: only the new key is rejected; the existing
    // entry in the same batch is still applied.
    let err = e
        .append_batch(vec![
            (
                sname.clone(),
                key(2),
                DataValue::String("batch-value".into()),
            ),
            (
                sname.clone(),
                "brand-new".to_string(),
                DataValue::String("n".into()),
            ),
        ])
        .expect_err("the new key must be rejected");
    assert!(
        matches!(err, LivenError::IndexRamLimitExceeded { .. }),
        "expected typed IndexRamLimitExceeded, got: {err:?}"
    );
    assert_eq!(
        e.get(&sname, &key(2)).unwrap().unwrap().value,
        DataValue::String("batch-value".into()),
        "existing key in the mixed batch must be applied"
    );
    assert!(
        e.get(&sname, "brand-new").unwrap().is_none(),
        "new key must not be applied"
    );
}

#[test]
fn retention_and_deletes_clear_the_live_high_water_flag() {
    let dir = tempdir().unwrap();
    let sname = "b".repeat(60);
    let key = |i: u32| format!("k{i:031}");
    build_old_accounting_dataset(dir.path(), &sname);

    let e = open(dir.path(), 1024, 16).unwrap();
    assert!(e.index_usage().high_water_reached);

    let keys: Vec<String> = (0..100_000u32).map(key).collect();
    e.append_tombstone_batch(&sname, &keys).unwrap();

    let usage = e.index_usage();
    assert!(
        !usage.high_water_reached,
        "deletes must clear high-water: {usage:?}"
    );
    assert!(usage.used_bytes < usage.high_water_bytes);

    // Below the limit again, a new key is accepted.
    e.append(&sname, "after-delete", DataValue::String("z".into()), false)
        .unwrap();
}

#[test]
fn embedded_open_with_config_honours_configured_limits() {
    let dir = tempdir().unwrap();
    {
        let e = open(dir.path(), 1024, 256).unwrap();
        for s in 0..40 {
            e.append(&format!("s{s}"), "k", DataValue::String("v".into()), false)
                .unwrap();
        }
    }
    let db = Liven::open_with_config(
        dir.path(),
        liven::embed::LivenConfig {
            max_streams: 1024,
            ..Default::default()
        },
    )
    .expect("embedded reopen with raised max_streams");
    assert!(db.streams().unwrap().len() >= 40);
}
