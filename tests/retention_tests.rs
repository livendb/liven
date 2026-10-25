//! Retention, high-water mark, and timestamp-index accounting tests.

use liven::storage::StorageEngine;
use liven::types::DataValue;
use std::path::Path;
use std::time::Duration;

fn segment_count(dir: &Path) -> usize {
    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("liven"))
        .count()
}

#[test]
fn timestamp_index_is_included_in_the_index_charge() {
    let dir = tempfile::tempdir().unwrap();
    let engine = StorageEngine::new(dir.path(), 1024 * 1024).unwrap();
    engine
        .append("s", "k0", DataValue::String("v0".to_string()), false)
        .unwrap();
    // "s:k0" = 4 bytes; primary node = 64; timestamp node = 64.
    assert_eq!(
        engine
            .index_ram_bytes
            .load(std::sync::atomic::Ordering::SeqCst),
        132
    );
}

#[test]
fn high_water_mark_flips_before_the_hard_limit() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = StorageEngine::new(dir.path(), 1024 * 1024).unwrap();
    engine.set_max_index_ram_bytes(1000);
    engine.set_index_high_water_percent(50);

    for i in 0..4 {
        engine
            .append(
                "s",
                &format!("k{i}"),
                DataValue::String("v".to_string()),
                false,
            )
            .unwrap();
    }
    let usage = engine.index_usage();
    assert!(
        usage.high_water_reached,
        "50% of 1000 should have been reached"
    );
    assert!(usage.used_bytes > usage.high_water_bytes);
    assert!(usage.used_bytes < usage.limit_bytes);

    let mut last_error = None;
    for i in 4..12 {
        if let Err(e) = engine.append(
            "s",
            &format!("k{i}"),
            DataValue::String("v".to_string()),
            false,
        ) {
            last_error = Some(e.to_string());
            break;
        }
    }
    let error = last_error.expect("the hard limit must reject a new key");
    assert!(
        error.contains("Index RAM limit"),
        "unexpected error: {error}"
    );
}

#[test]
fn retention_deletes_fully_expired_inactive_segments() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = StorageEngine::new(dir.path(), 120).unwrap();
        for i in 0..6 {
            engine
                .append(
                    "s",
                    &format!("k{i}"),
                    DataValue::String(format!("v{i}")),
                    false,
                )
                .unwrap();
        }
        assert!(segment_count(dir.path()) >= 2, "need a rollover");
        engine.set_stream_retention("s", Some(1));
        std::thread::sleep(Duration::from_millis(20));

        let report = engine.enforce_retention().unwrap();
        assert!(
            report.deleted_segments >= 1,
            "expected an expired inactive segment to be deleted: {report:?}"
        );
        assert!(report.deleted_keys >= 1);
        // The active segment is never deleted, so the newest keys remain.
        assert!(engine.get("s", "k0").unwrap().is_none());
        assert!(engine.get("s", "k5").unwrap().is_some());
        // The charge must match the live keys that remain (4 + 64 + 64 each).
        let remaining = (0..6)
            .filter(|i| engine.get("s", &format!("k{i}")).unwrap().is_some())
            .count();
        assert!(remaining > 0);
        assert_eq!(
            engine
                .index_ram_bytes
                .load(std::sync::atomic::Ordering::SeqCst),
            remaining as u64 * 132
        );
    }

    let engine = StorageEngine::new(dir.path(), 120).unwrap();
    assert!(engine.get("s", "k0").unwrap().is_none());
    assert!(engine.get("s", "k5").unwrap().is_some());
}

#[test]
fn retention_disabled_keeps_segments() {
    let dir = tempfile::tempdir().unwrap();
    let engine = StorageEngine::new(dir.path(), 120).unwrap();
    for i in 0..6 {
        engine
            .append(
                "s",
                &format!("k{i}"),
                DataValue::String(format!("v{i}")),
                false,
            )
            .unwrap();
    }
    std::thread::sleep(Duration::from_millis(10));
    let before = segment_count(dir.path());
    let report = engine.enforce_retention().unwrap();
    assert_eq!(report.deleted_segments, 0);
    assert_eq!(segment_count(dir.path()), before);
    assert!(engine.get("s", "k0").unwrap().is_some());
}
