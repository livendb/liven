use criterion::{
    BatchSize, BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main,
};
use liven::executor::execute_query;
use liven::parser::parse_pipeline;
use liven::storage::StorageEngine;
use liven::types::{DataValue, PipelineStage, Query};
use std::sync::Arc;
use std::time::Duration;

// ── Helpers ───────────────────────────────────────────────────────────────────

fn make_engine(suffix: &str, segment_mb: u64) -> (StorageEngine, tempfile::TempDir) {
    let dir = tempfile::Builder::new()
        .prefix(&format!("liven_bench_{}_", suffix))
        .tempdir()
        .expect("tempdir");
    let engine =
        StorageEngine::new(dir.path(), segment_mb * 1024 * 1024).expect("StorageEngine::new");
    (engine, dir)
}

fn populate(engine: &StorageEngine, stream: &str, count: usize) {
    for i in 0..count {
        engine
            .append(
                stream,
                &format!("key_{:08}", i),
                DataValue::String(format!(r#"{{"index":{},"msg":"log {}" }}"#, i, i)),
                false,
            )
            .expect("populate");
    }
}

// Tuned criterion config for laptop — fewer samples, shorter measurement time
fn fast_config() -> Criterion {
    Criterion::default()
        .sample_size(20) // default is 100 — 5x faster
        .measurement_time(Duration::from_secs(3)) // default is 5s
        .warm_up_time(Duration::from_secs(1)) // default is 3s
}

// ── Parser ────────────────────────────────────────────────────────────────────

fn bench_parser(c: &mut Criterion) {
    let mut group = c.benchmark_group("parser");
    // Parser benchmarks are pure CPU — default sample size is fine
    // but keep them grouped so they finish together

    let simple = r#"from("logs") | filter(status == "error") | limit(10)"#;
    group.bench_function("simple", |b| {
        b.iter(|| {
            let _ = black_box(parse_pipeline(black_box(simple)));
        })
    });

    let complex = r#"from("telemetry") | filter(voltage > 12.5) | window(5000, average) | enrich("customers", "customer_id") | export(csv)"#;
    group.bench_function("complex", |b| {
        b.iter(|| {
            let _ = black_box(parse_pipeline(black_box(complex)));
        })
    });

    group.finish();
}

// ── Write Path ────────────────────────────────────────────────────────────────

fn bench_append(c: &mut Criterion) {
    let mut group = c.benchmark_group("append");
    group
        .sample_size(20)
        .measurement_time(Duration::from_secs(5));

    // Single record append
    {
        let (engine, _dir) = make_engine("append_single", 50);
        let mut idx = 0u64;
        group.throughput(Throughput::Elements(1));
        group.bench_function("single_record", |b| {
            b.iter(|| {
                let key = format!("k_{}", idx);
                idx += 1;
                let _ = black_box(engine.append(
                    "events",
                    &key,
                    DataValue::String(r#"{"type":"click"}"#.to_string()),
                    false,
                ));
            })
        });
    }

    // Batch append — reduced sizes for laptop
    for batch_size in [10usize, 100, 500] {
        let (engine, _dir) = make_engine(&format!("append_batch_{}", batch_size), 50);
        let mut counter = 0usize;

        group.throughput(Throughput::Elements(batch_size as u64));
        group.bench_with_input(
            BenchmarkId::new("batch", batch_size),
            &batch_size,
            |b, &size| {
                b.iter(|| {
                    let batch: Vec<(String, String, DataValue)> = (0..size)
                        .map(|i| {
                            (
                                "events".to_string(),
                                format!("bk_{}_{}", counter + i, i),
                                DataValue::String(format!(r#"{{"i":{}}}"#, i)),
                            )
                        })
                        .collect();
                    counter += size;
                    let _ = black_box(engine.append_batch(batch));
                })
            },
        );
    }

    group.finish();
}

fn bench_upsert(c: &mut Criterion) {
    let mut group = c.benchmark_group("upsert");
    group
        .sample_size(20)
        .measurement_time(Duration::from_secs(3))
        .throughput(Throughput::Elements(1));

    // Upsert on new key
    {
        let (engine, _dir) = make_engine("upsert_new", 50);
        let mut idx = 0u64;
        group.bench_function("new_key", |b| {
            b.iter(|| {
                let key = format!("k_{}", idx);
                idx += 1;
                let _ = black_box(engine.append(
                    "events",
                    &key,
                    DataValue::String(r#"{"v":1}"#.to_string()),
                    false,
                ));
            })
        });
    }

    // Upsert on existing key — measures tombstone + append cost
    {
        let (engine, _dir) = make_engine("upsert_existing", 50);
        engine
            .append(
                "events",
                "fixed_key",
                DataValue::String(r#"{"v":0}"#.to_string()),
                false,
            )
            .unwrap();

        let mut version = 1u64;
        group.bench_function("existing_key_tombstone_cost", |b| {
            b.iter(|| {
                let _ = engine.append("events", "fixed_key", DataValue::Null, true);
                let _ = black_box(engine.append(
                    "events",
                    "fixed_key",
                    DataValue::String(format!(r#"{{"v":{}}}"#, version)),
                    false,
                ));
                version += 1;
            })
        });
    }

    group.finish();
}

// ── Read Path ─────────────────────────────────────────────────────────────────

fn bench_point_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("point_lookup");
    group
        .sample_size(50) // reads are fast, more samples give better stats
        .measurement_time(Duration::from_secs(3))
        .throughput(Throughput::Elements(1));

    // Reduced dataset sizes — 100K takes too long to populate repeatedly
    for key_count in [1_000usize, 10_000] {
        let (engine, _dir) = make_engine(&format!("lookup_{}", key_count), 200);
        populate(&engine, "logs", key_count);

        let mid_key = format!("key_{:08}", key_count / 2);

        group.bench_with_input(
            BenchmarkId::new("existing_key", key_count),
            &key_count,
            |b, _| {
                b.iter(|| {
                    let _ = black_box(engine.get("logs", black_box(&mid_key)));
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("missing_key", key_count),
            &key_count,
            |b, _| {
                b.iter(|| {
                    let _ = black_box(engine.get("logs", black_box("key_not_exists")));
                })
            },
        );
    }

    group.finish();
}

fn bench_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("scan");
    group
        .sample_size(10) // scans are slow — minimum samples
        .measurement_time(Duration::from_secs(5));

    // Reduced dataset — 50K was too slow, 10K is representative
    for record_count in [1_000usize, 5_000, 10_000] {
        let (engine, _dir) = make_engine(&format!("scan_{}", record_count), 200);
        populate(&engine, "logs", record_count);

        group.throughput(Throughput::Elements(record_count as u64));

        group.bench_with_input(
            BenchmarkId::new("full_scan", record_count),
            &record_count,
            |b, _| {
                b.iter(|| {
                    let _ = black_box(engine.scan_historical());
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("scan_limited_100", record_count),
            &record_count,
            |b, _| {
                b.iter(|| {
                    let _ = black_box(engine.scan_historical_limited(100));
                })
            },
        );

        let query = Query::Pipeline(vec![
            PipelineStage::From {
                stream_name: "logs".to_string(),
            },
            PipelineStage::Limit { count: 50 },
        ]);
        group.bench_with_input(
            BenchmarkId::new("pipeline_limit_50", record_count),
            &record_count,
            |b, _| {
                b.iter(|| {
                    let _ = black_box(execute_query(&engine, &query));
                })
            },
        );
    }

    group.finish();
}

fn bench_time_range(c: &mut Criterion) {
    let mut group = c.benchmark_group("time_range");
    group
        .sample_size(20)
        .measurement_time(Duration::from_secs(3));

    // Reduced to 10K records — enough to exercise the index
    let (engine, _dir) = make_engine("time_range", 200);
    let base_ts: i64 = 1_700_000_000_000;

    for i in 0..10_000usize {
        engine
            .append(
                "events",
                &format!("key_{:08}", i),
                DataValue::String(format!(r#"{{"ts":{}}}"#, base_ts + i as i64)),
                false,
            )
            .expect("append");
    }

    for result_count in [100usize, 1_000, 5_000] {
        group.throughput(Throughput::Elements(result_count as u64));
        group.bench_with_input(
            BenchmarkId::new("timestamp_index", result_count),
            &result_count,
            |b, &n| {
                let start = base_ts;
                let end = base_ts + n as i64;
                b.iter(|| {
                    let _ = black_box(engine.scan_by_time_range("events", start, end));
                })
            },
        );
    }

    group.finish();
}

// ── Concurrent Reads ──────────────────────────────────────────────────────────

fn bench_concurrent_reads(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent");
    group
        .sample_size(10) // thread spawning is expensive on macOS
        .measurement_time(Duration::from_secs(5));

    let (engine, _dir) = make_engine("concurrent", 200);
    populate(&engine, "logs", 5_000); // reduced from 10K
    let engine = Arc::new(engine);

    // Pre-build keys outside the bench loop
    let keys: Vec<String> = (0..5_000).map(|i| format!("key_{:08}", i)).collect();
    let keys = Arc::new(keys);

    for thread_count in [1usize, 2, 4] {
        // Reduced from [1,4,8,16] — 16 threads on M2 causes scheduler pressure
        group.throughput(Throughput::Elements(thread_count as u64));
        group.bench_with_input(
            BenchmarkId::new("read_threads", thread_count),
            &thread_count,
            |b, &n| {
                b.iter(|| {
                    let barrier = Arc::new(std::sync::Barrier::new(n + 1));
                    let handles: Vec<_> = (0..n)
                        .map(|t| {
                            let engine = Arc::clone(&engine);
                            let keys = Arc::clone(&keys);
                            let barrier = Arc::clone(&barrier);
                            std::thread::spawn(move || {
                                barrier.wait();
                                let key = &keys[t * 100 % keys.len()];
                                let _ = engine.get("logs", key);
                            })
                        })
                        .collect();
                    barrier.wait();
                    for h in handles {
                        h.join().unwrap();
                    }
                })
            },
        );
    }

    group.finish();
}

// ── Compaction ────────────────────────────────────────────────────────────────

fn bench_compaction(c: &mut Criterion) {
    let mut group = c.benchmark_group("compaction");
    group
        .sample_size(10)
        .measurement_time(Duration::from_secs(10)); // compaction needs more time

    // Reduced record counts — 10K compaction setup was too slow per iteration
    for record_count in [500usize, 1_000] {
        group.bench_with_input(
            BenchmarkId::new("compact", record_count),
            &record_count,
            |b, &n| {
                b.iter_batched(
                    || {
                        // Setup: fresh engine with n records across small segments
                        let (engine, dir) = make_engine("compaction_setup", 1);
                        populate(&engine, "logs", n);
                        // Upsert half to create orphaned records
                        for i in 0..(n / 2) {
                            let _ = engine.append(
                                "logs",
                                &format!("key_{:08}", i),
                                DataValue::Null,
                                true,
                            );
                            let _ = engine.append(
                                "logs",
                                &format!("key_{:08}", i),
                                DataValue::Int(i as i64 + 1),
                                false,
                            );
                        }
                        (engine, dir)
                    },
                    |(engine, _dir)| {
                        let _ = black_box(engine.compact());
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

// ── Vector ────────────────────────────────────────────────────────────────────

fn bench_vector(c: &mut Criterion) {
    let mut group = c.benchmark_group("vector");
    group
        .sample_size(20)
        .measurement_time(Duration::from_secs(3));

    let (engine, _dir) = make_engine("vector", 200);

    // Reduced dimension range — 1536 is slow to clone repeatedly
    for dims in [128usize, 512] {
        let vec_val = DataValue::Vector(vec![127i8; dims]);
        let arr_val = DataValue::Array(vec![DataValue::Int(127); dims]);

        group.throughput(Throughput::Bytes(dims as u64));

        let mut v_idx = 0usize;
        let mut a_idx = 0usize;

        group.bench_with_input(BenchmarkId::new("append_quantized", dims), &dims, |b, _| {
            b.iter(|| {
                let key = format!("v_{}_{}", dims, v_idx);
                v_idx += 1;
                let _ = black_box(engine.append("vecs", &key, vec_val.clone(), false));
            })
        });

        group.bench_with_input(
            BenchmarkId::new("append_msgpack_array", dims),
            &dims,
            |b, _| {
                b.iter(|| {
                    let key = format!("a_{}_{}", dims, a_idx);
                    a_idx += 1;
                    let _ = black_box(engine.append("arrs", &key, arr_val.clone(), false));
                })
            },
        );
    }

    // Lookup benchmarks use fixed pre-inserted keys
    engine
        .append(
            "vecs",
            "lookup_vec",
            DataValue::Vector(vec![42i8; 512]),
            false,
        )
        .unwrap();
    engine
        .append(
            "arrs",
            "lookup_arr",
            DataValue::Array(vec![DataValue::Int(42); 512]),
            false,
        )
        .unwrap();

    group.throughput(Throughput::Bytes(512));

    group.bench_function("lookup_quantized_512d", |b| {
        b.iter(|| {
            let _ = black_box(engine.get("vecs", black_box("lookup_vec")));
        })
    });

    group.bench_function("lookup_msgpack_array_512d", |b| {
        b.iter(|| {
            let _ = black_box(engine.get("arrs", black_box("lookup_arr")));
        })
    });

    group.finish();
}

// ── Codec ─────────────────────────────────────────────────────────────────────

fn bench_codec(c: &mut Criterion) {
    use bytes::BytesMut;
    use liven::codec::{LivenCodec, LivenFrame};
    use tokio_util::codec::Encoder;

    let mut group = c.benchmark_group("codec");
    group
        .sample_size(50)
        .measurement_time(Duration::from_secs(3));

    for payload_bytes in [64usize, 1024, 16_384] {
        // Removed 131_072 — large payload clone is slow
        group.throughput(Throughput::Bytes(payload_bytes as u64));

        let frame = LivenFrame::Query("x".repeat(payload_bytes));
        group.bench_with_input(
            BenchmarkId::new("encode_query", payload_bytes),
            &payload_bytes,
            |b, _| {
                let mut dst = BytesMut::with_capacity(payload_bytes + 16);
                let mut codec = LivenCodec::new(false);
                b.iter(|| {
                    dst.clear();
                    let _ = black_box(codec.encode(black_box(frame.clone()), &mut dst));
                })
            },
        );
    }

    for dims in [128usize, 512] {
        // Removed 1536 — large vector clone adds clone overhead to measurement
        group.throughput(Throughput::Bytes(dims as u64));
        let frame = LivenFrame::Vector(vec![42i8; dims]);
        group.bench_with_input(BenchmarkId::new("encode_vector", dims), &dims, |b, _| {
            let mut dst = BytesMut::with_capacity(dims + 16);
            let mut codec = LivenCodec::new(false);
            b.iter(|| {
                dst.clear();
                let _ = black_box(codec.encode(black_box(frame.clone()), &mut dst));
            })
        });
    }

    group.finish();
}

// ── Groups ────────────────────────────────────────────────────────────────────

criterion_group! {
    name = benches;
    config = fast_config();
    targets =
        bench_parser,
        bench_append,
        bench_upsert,
        bench_point_lookup,
        bench_scan,
        bench_time_range,
        bench_concurrent_reads,
        bench_compaction,
        bench_vector,
        bench_codec,
}
criterion_main!(benches);
