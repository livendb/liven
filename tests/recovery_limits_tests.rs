//! Step 4a: recovery must honour configured limits, not the hardcoded defaults.
//!
//! Before the fix, `StorageEngine::new_with_recovery_mode()` ran recovery with
//! defaults (32 streams, 16 MB index) and the configured limits were applied only
//! afterwards, so a larger database failed to reopen with StreamLimitExceeded /
//! IndexRamLimitExceeded.
use liven::Liven;
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
    println!("streams limit error: {msg}");
}

#[test]
fn index_above_default_reopens_with_configured_limit() {
    let dir = tempdir().unwrap();
    // Long stream name raises per-key overhead to ~157 B, so ~120k keys > 16 MB.
    let sname = "b".repeat(60);
    let key = |i: u32| format!("k{i:031}");
    {
        let e = open(dir.path(), 1024, 256).unwrap();
        let batch: Vec<(String, String, DataValue)> = (0..120_000u32)
            .map(|i| (sname.clone(), key(i), DataValue::String("x".into())))
            .collect();
        e.append_batch(batch).unwrap();
    }
    // Configured limit above 16 MB -> recovery succeeds.
    let e = open(dir.path(), 1024, 256).unwrap();
    assert!(
        e.get(&sname, &key(500)).unwrap().is_some(),
        "data recovered"
    );

    // Limit of 16 MB -> recovery fails with the index RAM error.
    let err = match open(dir.path(), 1024, 16) {
        Ok(_) => panic!("index > 16 MB must not recover under 16 MB"),
        Err(e) => e,
    };
    let msg = err.to_string();
    assert!(
        msg.to_lowercase().contains("index"),
        "unexpected error: {msg}"
    );
    println!("index limit error: {msg}");
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
    // Embedded API with a raised limit must also recover the 40-stream DB.
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
