#![cfg(feature = "test-hooks")]
//! Crash-injection tests for compaction.
//!
//! The child test is re-executed as a subprocess with `LIVEN_TEST_CRASH_POINT`
//! set. `StorageEngine::compact` calls `test_crash_point` at each durable step
//! and aborts the process without unwinding, so the parent can verify that
//! reopening the directory yields either the pre- or post-compaction state and
//! never loses a live record.

use liven::storage::StorageEngine;
use liven::types::DataValue;
use std::path::PathBuf;
use std::process::Command;

const SEG: u64 = 120;
const KEYS: [(&str, &str); 6] = [
    ("k0", "v0"),
    ("k1", "v1"),
    ("k2", "v2"),
    ("k3", "v3"),
    ("k4", "v4"),
    ("k5", "v5"),
];

fn setup(dir: &std::path::Path) {
    let engine = StorageEngine::new(dir, SEG).unwrap();
    for (key, value) in KEYS {
        engine
            .append("s", key, DataValue::String(value.to_string()), false)
            .unwrap();
    }
    // Force a rollover so k0..k2 are in an inactive segment.
    for i in 0..3 {
        engine
            .append(
                "s",
                &format!("pad{i}"),
                DataValue::String("padding".to_string()),
                false,
            )
            .unwrap();
    }
}

fn verify(dir: &std::path::Path) {
    let engine = StorageEngine::new(dir, SEG).unwrap();
    for (key, value) in KEYS {
        let got = engine.get("s", key).unwrap();
        assert!(
            got.is_some(),
            "key {key} lost after a crash during compaction in {}",
            dir.display()
        );
        assert_eq!(
            got.unwrap().value,
            DataValue::String(value.to_string()),
            "key {key} changed after a crash during compaction"
        );
    }
}

#[test]
fn crash_child_compacts_and_aborts() {
    let Some(dir) = std::env::var_os("LIVEN_CRASH_DIR") else {
        return;
    };
    let engine = StorageEngine::new(PathBuf::from(dir), SEG).unwrap();
    engine.compact().unwrap();
    // If the selected crash point was not reached, exit successfully so the
    // parent can distinguish "not reached" from a real abort.
    std::process::exit(0);
}

#[test]
fn compaction_is_crash_consistent_at_every_step() {
    let points = [
        "after_create",
        "after_write",
        "after_sync_data",
        "after_sync_dir",
        "after_pointer_swap",
        "before_unlink",
        "after_first_unlink",
        "after_unlinks",
        "after_final_sync",
    ];

    let exe = std::env::current_exe().expect("current test binary");
    for point in points {
        let dir = tempfile::tempdir().unwrap();
        setup(dir.path());

        let status = Command::new(&exe)
            .arg("--exact")
            .arg("crash_child_compacts_and_aborts")
            .env("LIVEN_CRASH_DIR", dir.path())
            .env("LIVEN_TEST_CRASH_POINT", point)
            .status()
            .expect("spawn crash child");

        assert!(
            !status.success(),
            "crash point '{point}' was not reached (child exited successfully)"
        );
        verify(dir.path());
    }
}
