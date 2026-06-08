use criterion::{Criterion, black_box, criterion_group, criterion_main};
use liven::parser::parse_pipeline;
use liven::storage::StorageEngine;
use liven::types::DataValue;

fn bench_parser(c: &mut Criterion) {
    let query_str = "from(\"logs\") | filter(status == \"error\") | limit(10)";
    c.bench_function("parse_pipeline_simple", |b| {
        b.iter(|| {
            let res = parse_pipeline(black_box(query_str));
            let _ = black_box(res);
        })
    });

    let complex_query_str = "from(\"telemetry\") | filter(voltage > 12.5) | window(5000, average) | enrich(\"customers\", \"customer_id\") | export(csv)";
    c.bench_function("parse_pipeline_complex", |b| {
        b.iter(|| {
            let res = parse_pipeline(black_box(complex_query_str));
            let _ = black_box(res);
        })
    });
}

fn bench_storage(c: &mut Criterion) {
    // Generate a unique temp directory path under the OS temp folder
    let rand_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let temp_dir = std::env::temp_dir().join(format!("liven_bench_{}", rand_id));
    std::fs::create_dir_all(&temp_dir).unwrap();

    let engine = StorageEngine::new(&temp_dir, 50 * 1024 * 1024).unwrap(); // 50MB max segment size

    // Warm up storage with 100 sequential entries to index
    for i in 0..100 {
        let _ = engine.append("logs", &format!("key_{}", i), DataValue::Int(i), false);
    }

    // Benchmark point-lookup performance for existing keys
    c.bench_function("storage_get_existing", |b| {
        b.iter(|| {
            let res = engine.get("logs", black_box("key_50"));
            let _ = black_box(res);
        })
    });

    // Benchmark point-lookup performance for non-existing keys
    c.bench_function("storage_get_non_existent", |b| {
        b.iter(|| {
            let res = engine.get("logs", black_box("key_not_exists"));
            let _ = black_box(res);
        })
    });

    // Benchmark batch append ingestion performance (appends to log on disk + fdatasync + updates SkipMap index)
    let mut batch_idx = 0;
    c.bench_function("storage_append_batch", |b| {
        b.iter(|| {
            let batch = vec![
                (
                    "logs".to_string(),
                    format!("batch_key_{}", batch_idx),
                    DataValue::String("some log message".to_string()),
                ),
                (
                    "logs".to_string(),
                    format!("batch_key_{}", batch_idx + 1),
                    DataValue::Int(batch_idx as i64),
                ),
            ];
            batch_idx += 2;
            let res = engine.append_batch(black_box(batch));
            let _ = black_box(res);
        })
    });

    // Clean up files synchronously after finishing the benchmarks
    let _ = std::fs::remove_dir_all(&temp_dir);
}

criterion_group!(benches, bench_parser, bench_storage);
criterion_main!(benches);
