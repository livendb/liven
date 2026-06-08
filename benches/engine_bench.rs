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

fn bench_vector(c: &mut Criterion) {
    let rand_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let temp_dir = std::env::temp_dir().join(format!("liven_vector_bench_{}", rand_id));
    std::fs::create_dir_all(&temp_dir).unwrap();

    let engine = StorageEngine::new(&temp_dir, 50 * 1024 * 1024).unwrap();

    let vector_size = 512;
    let vector_val = DataValue::Vector(vec![127i8; vector_size]);
    let array_val = DataValue::Array(vec![DataValue::Int(127); vector_size]);

    // Benchmark appending Quantized Vector vs Standard MessagePack Array
    c.bench_function("vector_append_quantized", |b| {
        let mut idx = 0;
        b.iter(|| {
            let key = format!("v_{}", idx);
            let _ = engine.append("vectors", &key, black_box(vector_val.clone()), false);
            idx += 1;
        })
    });

    c.bench_function("vector_append_msgpack_array", |b| {
        let mut idx = 0;
        b.iter(|| {
            let key = format!("a_{}", idx);
            let _ = engine.append("arrays", &key, black_box(array_val.clone()), false);
            idx += 1;
        })
    });

    // Warm up for lookup
    let _ = engine.append("vectors_lookup", "v_look", vector_val, false);
    let _ = engine.append("arrays_lookup", "a_look", array_val, false);

    c.bench_function("vector_lookup_quantized_slice", |b| {
        b.iter(|| {
            let res = engine.get("vectors_lookup", black_box("v_look"));
            let _ = black_box(res);
        })
    });

    c.bench_function("vector_lookup_msgpack_array", |b| {
        b.iter(|| {
            let res = engine.get("arrays_lookup", black_box("a_look"));
            let _ = black_box(res);
        })
    });

    let _ = std::fs::remove_dir_all(&temp_dir);
}

criterion_group!(benches, bench_parser, bench_storage, bench_vector);
criterion_main!(benches);
