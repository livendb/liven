//! Concurrency semantics tests.
use liven::executor::execute_query;
use liven::storage::StorageEngine;
use liven::types::Query;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[test]
fn concurrent_same_key_inserts_have_exactly_one_winner() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(StorageEngine::new(dir.path(), 1024 * 1024).unwrap());
    engine
        .append(
            "s",
            "seed",
            liven::types::DataValue::String("v".into()),
            false,
        )
        .unwrap();

    let winners = Arc::new(Mutex::new(Vec::new()));
    let mut handles = Vec::new();
    for i in 0..16u64 {
        let engine = engine.clone();
        let winners = winners.clone();
        handles.push(std::thread::spawn(move || {
            let q = Query::insert("s", "same", serde_json::json!(format!("t{i}")));
            match execute_query(&engine, &q) {
                Ok(_) => winners.lock().unwrap().push(i),
                Err(e) => assert!(
                    e.to_string().contains("already exists"),
                    "thread {i}: unexpected error: {e}"
                ),
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    let winners = winners.lock().unwrap();
    assert_eq!(winners.len(), 1, "exactly one insert must win: {winners:?}");
    let got = engine.get("s", "same").unwrap().unwrap();
    assert_eq!(
        got.value,
        liven::types::DataValue::String(format!("t{}", winners[0]))
    );
}

/// Each key's operations are serialized by a test-side per-key lock, so the
/// completion-order model is one valid linearization. Operations on different
/// keys run concurrently.
#[test]
fn concurrent_upsert_delete_matches_a_linearizable_model() {
    const KEYS: usize = 8;
    const THREADS: usize = 8;
    const OPS: usize = 20;

    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(StorageEngine::new(dir.path(), 1024 * 1024).unwrap());
    let mut model = HashMap::new();
    for k in 0..KEYS {
        engine
            .replace(
                "s",
                &format!("k{k}"),
                liven::types::DataValue::String("init".into()),
            )
            .unwrap();
        model.insert(format!("k{k}"), Some("init".to_string()));
    }

    let model = Arc::new(Mutex::new(model));
    let locks: Vec<Arc<Mutex<()>>> = (0..KEYS).map(|_| Arc::new(Mutex::new(()))).collect();
    let mut handles = Vec::new();
    for t in 0..THREADS {
        let engine = engine.clone();
        let model = model.clone();
        let locks = locks.clone();
        handles.push(std::thread::spawn(move || {
            for op in 0..OPS {
                let idx = (t + op) % KEYS;
                let key = format!("k{idx}");
                let _key_guard = locks[idx].lock().unwrap();
                if (t + op) % 3 == 0 {
                    let q = Query::delete("s", &key);
                    execute_query(&engine, &q).unwrap();
                    model.lock().unwrap().insert(key, None);
                } else {
                    let value = format!("t{t}-o{op}");
                    let q = Query::upsert("s", &key, serde_json::json!(value));
                    execute_query(&engine, &q).unwrap();
                    model.lock().unwrap().insert(key, Some(value));
                }
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    let model = model.lock().unwrap();
    for (key, expected) in model.iter() {
        let got = engine.get("s", key).unwrap();
        match (expected, got) {
            (Some(value), Some(rec)) => assert_eq!(
                rec.value,
                liven::types::DataValue::String(value.clone()),
                "key {key} changed"
            ),
            (None, None) => {}
            (Some(_), None) => panic!("key {key} lost"),
            (None, Some(_)) => panic!("deleted key {key} resurrected"),
        }
    }
}
