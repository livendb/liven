//! Probes for Step 4b verification: what the primary index actually holds.
use liven::storage::StorageEngine;
use liven::types::DataValue;
use tempfile::tempdir;

fn count_prefix(e: &StorageEngine, prefix: &str) -> usize {
    e.skipmap
        .iter()
        .filter(|(key, _)| key.starts_with(prefix))
        .count()
}

#[test]
fn skipmap_holds_only_the_latest_live_pointer_per_key() {
    let dir = tempdir().unwrap();
    let engine = StorageEngine::new(dir.path(), 1 << 20).unwrap();

    engine
        .append("s", "k", DataValue::String("v1".into()), false)
        .unwrap();
    assert_eq!(count_prefix(&engine, "s:"), 1);
    assert_eq!(
        engine.get("s", "k").unwrap().unwrap().value,
        DataValue::String("v1".into())
    );

    // upsert == tombstone + append
    engine.append("s", "k", DataValue::Null, true).unwrap();
    engine
        .append("s", "k", DataValue::String("v2".into()), false)
        .unwrap();
    assert_eq!(count_prefix(&engine, "s:"), 1, "still one live pointer");
    assert_eq!(
        engine.get("s", "k").unwrap().unwrap().value,
        DataValue::String("v2".into())
    );

    engine.append("s", "k", DataValue::Null, true).unwrap();
    engine
        .append("s", "k", DataValue::String("v3".into()), false)
        .unwrap();
    assert_eq!(count_prefix(&engine, "s:"), 1);
    assert_eq!(
        engine.get("s", "k").unwrap().unwrap().value,
        DataValue::String("v3".into())
    );

    // delete == tombstone only
    engine.append("s", "k", DataValue::Null, true).unwrap();
    assert_eq!(
        count_prefix(&engine, "s:"),
        0,
        "tombstone removes the entry"
    );
    assert!(engine.get("s", "k").unwrap().is_none());

    println!(
        "PROBE RESULT: skipmap keeps exactly one latest live pointer per key; tombstones remove it"
    );
}

#[test]
fn stream_prefix_is_ambiguous_when_names_contain_colon() {
    let dir = tempdir().unwrap();
    let engine = StorageEngine::new(dir.path(), 1 << 20).unwrap();
    engine
        .append("a", "k1", DataValue::String("x".into()), false)
        .unwrap();
    engine
        .append("a:b", "k1", DataValue::String("y".into()), false)
        .unwrap();

    let keys = engine.list_keys("a"); // uses prefix "a:"
    println!("PROBE RESULT: list_keys(\"a\") = {keys:?}");
    assert!(keys.iter().any(|k| k == "k1"));
    // Documents CURRENT behavior: the "a:" prefix also matches stream "a:b".
    assert!(
        keys.iter().any(|k| k == "b:k1"),
        "prefix 'a:' collides with stream 'a:b' (expected current behavior)"
    );
}
