#![cfg(feature = "test-hooks")]
//! Subprocess crash test for the one-frame upsert/replace path.
//!
//! The child is re-executed with `LIVEN_TEST_CRASH_POINT` set. The flusher
//! aborts after writing the replacement frame (before fsync) or after fsync.
//! The parent verifies that reopening never loses the previously acknowledged
//! value: before fsync it may see either v1 or v2, after fsync it must see v2.

use liven::storage::StorageEngine;
use liven::types::DataValue;
use std::path::{Path, PathBuf};
use std::process::Command;

const SEG: u64 = 1024 * 1024;

fn setup(dir: &Path) {
    let engine = StorageEngine::new(dir, SEG).unwrap();
    engine
        .append("s", "k", DataValue::String("v1".to_string()), false)
        .unwrap();
}

fn child() {
    let Some(dir) = std::env::var_os("LIVEN_REPLACE_CRASH_DIR") else {
        return;
    };
    let engine = StorageEngine::new(PathBuf::from(dir), SEG).unwrap();
    let _ = engine.replace("s", "k", DataValue::String("v2".to_string()));
    std::process::exit(0);
}

#[test]
fn replace_child_aborts_at_hook() {
    child();
}

#[test]
fn replace_is_crash_safe() {
    let exe = std::env::current_exe().expect("current test binary");
    for (point, allowed_v1) in [("replace_after_write", true), ("replace_after_sync", false)] {
        let dir = tempfile::tempdir().unwrap();
        setup(dir.path());

        let status = Command::new(&exe)
            .arg("--exact")
            .arg("replace_child_aborts_at_hook")
            .env("LIVEN_REPLACE_CRASH_DIR", dir.path())
            .env("LIVEN_TEST_CRASH_POINT", point)
            .status()
            .expect("spawn replace crash child");
        assert!(
            !status.success(),
            "crash point '{point}' was not reached (child exited successfully)"
        );

        let engine = StorageEngine::new(dir.path(), SEG).unwrap();
        let got = engine.get("s", "k").unwrap().expect("key must exist");
        match got.value {
            DataValue::String(s) if s == "v2" => {}
            DataValue::String(s) if s == "v1" && allowed_v1 => {}
            other => panic!("crash point '{point}' produced unexpected value {other:?}"),
        }
    }
}
