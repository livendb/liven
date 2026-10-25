#![cfg(feature = "test-hooks")]
//! Fault injection for the durable-write failure path.
//!
//! This binary intentionally runs a single test so the process-wide env hook
//! cannot affect unrelated tests.

use liven::executor::execute_query;
use liven::storage::StorageEngine;
use liven::types::{DataValue, Query};

#[test]
fn fsync_failure_fails_the_ack_and_marks_the_engine_failed() {
    let dir = tempfile::tempdir().unwrap();
    // SAFETY: this test binary contains a single test and no other threads
    // inspect the environment concurrently.
    unsafe {
        std::env::set_var("LIVEN_TEST_FAIL_SYNC", "1");
    }

    let engine = StorageEngine::new(dir.path(), 1024 * 1024).unwrap();
    let first = engine.append("s", "k", DataValue::String("v".to_string()), false);
    assert!(
        first.is_err(),
        "the injected fsync failure must fail the ack"
    );
    assert!(engine.is_failed(), "the engine must enter a failed state");

    let second = engine.append("s", "k2", DataValue::String("v".to_string()), false);
    let err = second.expect_err("writes after a failed fsync must be rejected");
    assert!(
        err.to_string().contains("failed state"),
        "unexpected error: {err}"
    );

    // Executor insert: the ack fails, the pending reservation is cleared, and a
    // retry reports the failed engine rather than a spurious duplicate.
    let dir2 = tempfile::tempdir().unwrap();
    let engine2 = StorageEngine::new(dir2.path(), 1024 * 1024).unwrap();
    let q = Query::insert("s", "k", serde_json::json!("v"));
    let first_exec = execute_query(&engine2, &q).expect_err("fsync failure must fail the ack");
    assert!(!first_exec.to_string().contains("already exists"));
    let retry = execute_query(&engine2, &q).expect_err("retry must fail the engine");
    assert!(
        retry.to_string().contains("failed state"),
        "pending insert leaked or wrong error: {retry}"
    );

    unsafe {
        std::env::remove_var("LIVEN_TEST_FAIL_SYNC");
    }
}
