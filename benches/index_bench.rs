//! A/B microbenchmark for the live-key index.
//!
//! The library now uses `bbtree::BBTreeMap` through
//! [`liven::storage::index::IndexMap`]. This bench compares it directly with
//! the previous `crossbeam_skiplist::SkipMap` on the operation the read path
//! leans on most: point lookups over the same key set.
//!
//! Run with:
//!   cargo bench --bench index_bench

use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use liven::storage::index::IndexMap;
use liven::types::LogPointer;
use rand::SeedableRng;
use rand::seq::SliceRandom;

const KEYS: usize = 100_000;

fn key(i: usize) -> String {
    format!("stream:{i:08}")
}

fn pointer(i: usize) -> LogPointer {
    LogPointer {
        segment_id: 1,
        file_offset: i as u64,
        seq_id: i as u64,
        timestamp: i as i64,
    }
}

fn shuffled_key_order() -> Vec<usize> {
    let mut order: Vec<usize> = (0..KEYS).collect();
    let mut rng = rand::rngs::StdRng::seed_from_u64(0x5EED);
    order.shuffle(&mut rng);
    order
}

fn populate() -> (
    Vec<String>,
    IndexMap<String>,
    crossbeam_skiplist::SkipMap<String, LogPointer>,
) {
    let keys: Vec<String> = (0..KEYS).map(key).collect();
    let btree = IndexMap::new();
    let skiplist = crossbeam_skiplist::SkipMap::new();
    for (i, k) in keys.iter().enumerate() {
        btree.insert(k.clone(), pointer(i));
        skiplist.insert(k.clone(), pointer(i));
    }
    (keys, btree, skiplist)
}

fn bench_point_get(c: &mut Criterion) {
    let (keys, btree, skiplist) = populate();
    let order = shuffled_key_order();
    let missing: Vec<String> = (0..KEYS).map(|i| key(i + KEYS)).collect();

    let mut group = c.benchmark_group("index_point_get_hit");
    group.throughput(Throughput::Elements(KEYS as u64));
    group.sample_size(20);
    group.bench_function("bbtree", |b| {
        b.iter(|| {
            let mut sum = 0u64;
            for &i in &order {
                if let Some(p) = btree.get(black_box(keys[i].as_str())) {
                    sum = sum.wrapping_add(p.file_offset);
                }
            }
            black_box(sum)
        })
    });
    group.bench_function("crossbeam_skiplist", |b| {
        b.iter(|| {
            let mut sum = 0u64;
            for &i in &order {
                if let Some(e) = skiplist.get(black_box(keys[i].as_str())) {
                    sum = sum.wrapping_add(e.value().file_offset);
                }
            }
            black_box(sum)
        })
    });
    group.finish();

    let mut group = c.benchmark_group("index_point_get_miss");
    group.throughput(Throughput::Elements(KEYS as u64));
    group.sample_size(20);
    group.bench_function("bbtree", |b| {
        b.iter(|| {
            let mut hits = 0usize;
            for k in &missing {
                if btree.get(black_box(k.as_str())).is_some() {
                    hits += 1;
                }
            }
            black_box(hits)
        })
    });
    group.bench_function("crossbeam_skiplist", |b| {
        b.iter(|| {
            let mut hits = 0usize;
            for k in &missing {
                if skiplist.get(black_box(k.as_str())).is_some() {
                    hits += 1;
                }
            }
            black_box(hits)
        })
    });
    group.finish();
}

criterion_group!(benches, bench_point_get);
criterion_main!(benches);
