//! Fault injection for the durable-write failure path.
//!
//! This binary intentionally runs a single test so the process-wide env hook
//! cannot affect unrelated tests.

use liven::storage::StorageEngine;
use liven::types::DataValue;

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

    unsafe {
        std::env::remove_var("LIVEN_TEST_FAIL_SYNC");
    }
}
