//! Per-decision write-path benchmark.
//!
//! Measures single-record appends from N concurrent writers through:
//!   * the embedded `StorageEngine::append` path (no global write lock)
//!   * the executor with the *old* behavior (write_lock held across the ack)
//!   * the executor with the narrowed lock (released before the ack wait)
//!   * the native TCP server (parser + executor + codec + socket)
//!
//! It also reports flusher fsyncs/second and the average batch size observed by
//! the group-commit flusher, which is the direct evidence for whether callers
//! are sharing a batch.

use hdrhistogram::Histogram;
use liven::executor::execute_query;
use liven::storage::{RecoveryMode, StorageEngine};
use liven::types::{DataValue, Query};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

const STREAM: &str = "writebench";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Embedded,
    ExecutorHold,
    ExecutorNarrow,
    Tcp,
}

struct Args {
    writers: Vec<usize>,
    ops: usize,
    value_size: usize,
    modes: Vec<Mode>,
    segment_mb: u64,
    max_fds: usize,
    max_index_ram_mb: usize,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            writers: vec![1, 8, 32, 128],
            ops: 200,
            value_size: 128,
            modes: vec![
                Mode::Embedded,
                Mode::ExecutorHold,
                Mode::ExecutorNarrow,
                Mode::Tcp,
            ],
            segment_mb: 256,
            max_fds: 64,
            max_index_ram_mb: 1024,
        }
    }
}

fn parse_list(s: &str) -> Result<Vec<usize>, String> {
    s.split(',')
        .map(|p| {
            p.trim()
                .parse::<usize>()
                .map_err(|e: std::num::ParseIntError| e.to_string())
        })
        .collect()
}

fn mode_from(s: &str) -> Result<Mode, String> {
    match s {
        "embedded" => Ok(Mode::Embedded),
        "executor-hold" => Ok(Mode::ExecutorHold),
        "executor-narrow" => Ok(Mode::ExecutorNarrow),
        "tcp" => Ok(Mode::Tcp),
        "all" => Err("use --modes with an explicit list".to_string()),
        other => Err(format!("unknown mode '{other}'")),
    }
}

fn parse_args() -> Result<Args, String> {
    let mut args = Args::default();
    let mut iter = std::env::args().skip(1);
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--writers" => {
                args.writers = parse_list(&iter.next().ok_or("--writers needs a value")?)?
            }
            "--ops" => {
                args.ops = iter
                    .next()
                    .ok_or("--ops needs a value")?
                    .parse()
                    .map_err(|e: std::num::ParseIntError| e.to_string())?
            }
            "--value-size" => {
                args.value_size = iter
                    .next()
                    .ok_or("--value-size needs a value")?
                    .parse()
                    .map_err(|e: std::num::ParseIntError| e.to_string())?
            }
            "--modes" => {
                args.modes = iter
                    .next()
                    .ok_or("--modes needs a value")?
                    .split(',')
                    .map(mode_from)
                    .collect::<Result<Vec<_>, _>>()?;
            }
            "--segment-mb" => {
                args.segment_mb = iter
                    .next()
                    .ok_or("--segment-mb needs a value")?
                    .parse()
                    .map_err(|e: std::num::ParseIntError| e.to_string())?
            }
            "--max-fds" => {
                args.max_fds = iter
                    .next()
                    .ok_or("--max-fds needs a value")?
                    .parse()
                    .map_err(|e: std::num::ParseIntError| e.to_string())?
            }
            "--max-index-ram-mb" => {
                args.max_index_ram_mb = iter
                    .next()
                    .ok_or("--max-index-ram-mb needs a value")?
                    .parse()
                    .map_err(|e: std::num::ParseIntError| e.to_string())?
            }
            "--bench" => {}
            "--help" | "-h" => {
                println!(
                    "write_path_bench --writers 1,8,32,128 --ops 200 --modes embedded,executor-hold,executor-narrow,tcp"
                );
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument '{other}'")),
        }
    }
    Ok(args)
}

fn make_engine(args: &Args, dir: &std::path::Path) -> Arc<StorageEngine> {
    let engine = StorageEngine::new_with_tuning(
        dir,
        args.segment_mb * 1024 * 1024,
        RecoveryMode::Default,
        64,
        args.max_index_ram_mb as u64 * 1024 * 1024,
        args.max_fds,
        10_000,
        4_096,
        5_000,
        2_048,
    )
    .expect("engine");
    // No background compaction in this benchmark.
    let mut engine = engine;
    engine.compaction_threshold_segments = usize::MAX;
    engine.compaction_threshold_bytes = u64::MAX;
    Arc::new(engine)
}

fn worker_key(writer: usize, op: usize) -> String {
    format!("w{writer}-{op}")
}

fn merge_histograms(
    handles: Vec<std::thread::JoinHandle<(Histogram<u64>, Duration)>>,
) -> (Histogram<u64>, Duration) {
    let mut merged = Histogram::<u64>::new(3).expect("histogram");
    let mut wall = Duration::ZERO;
    for handle in handles {
        let (hist, elapsed) = handle.join().expect("worker panic");
        merged.add(&hist).expect("merge histogram");
        wall = wall.max(elapsed);
    }
    (merged, wall)
}

fn run_embedded(
    engine: &Arc<StorageEngine>,
    args: &Args,
    writers: usize,
) -> (Histogram<u64>, Duration) {
    let barrier = Arc::new(Barrier::new(writers + 1));
    let mut handles = Vec::with_capacity(writers);
    for writer in 0..writers {
        let engine = engine.clone();
        let barrier = barrier.clone();
        let ops = args.ops;
        let value_size = args.value_size;
        handles.push(std::thread::spawn(move || {
            let mut hist = Histogram::<u64>::new(3).expect("histogram");
            let mut value = String::new();
            while value.len() < value_size {
                value.push('x');
            }
            value.truncate(value_size);
            barrier.wait();
            let start = Instant::now();
            for op in 0..ops {
                let key = worker_key(writer, op);
                let op_start = Instant::now();
                let _ = std::hint::black_box(
                    engine
                        .append(STREAM, &key, DataValue::String(value.clone()), false)
                        .expect("append"),
                );
                hist.record(op_start.elapsed().as_nanos() as u64).ok();
            }
            (hist, start.elapsed())
        }));
    }
    barrier.wait();
    merge_histograms(handles)
}

fn run_executor_hold(
    engine: &Arc<StorageEngine>,
    args: &Args,
    writers: usize,
) -> (Histogram<u64>, Duration) {
    let barrier = Arc::new(Barrier::new(writers + 1));
    let mut handles = Vec::with_capacity(writers);
    for writer in 0..writers {
        let engine = engine.clone();
        let barrier = barrier.clone();
        let ops = args.ops;
        let value_size = args.value_size;
        handles.push(std::thread::spawn(move || {
            let mut hist = Histogram::<u64>::new(3).expect("histogram");
            let mut value = String::new();
            while value.len() < value_size {
                value.push('x');
            }
            value.truncate(value_size);
            barrier.wait();
            let start = Instant::now();
            for op in 0..ops {
                let key = worker_key(writer, op);
                let op_start = Instant::now();
                // Old behavior: the global lock is held across the ack wait.
                let _guard = engine.write_lock.lock().unwrap();
                assert!(engine.get(STREAM, &key).expect("get").is_none());
                let _ = std::hint::black_box(
                    engine
                        .append(STREAM, &key, DataValue::String(value.clone()), false)
                        .expect("append"),
                );
                drop(_guard);
                hist.record(op_start.elapsed().as_nanos() as u64).ok();
            }
            (hist, start.elapsed())
        }));
    }
    barrier.wait();
    merge_histograms(handles)
}

fn run_executor_narrow(
    engine: &Arc<StorageEngine>,
    args: &Args,
    writers: usize,
) -> (Histogram<u64>, Duration) {
    let barrier = Arc::new(Barrier::new(writers + 1));
    let mut handles = Vec::with_capacity(writers);
    for writer in 0..writers {
        let engine = engine.clone();
        let barrier = barrier.clone();
        let ops = args.ops;
        let value_size = args.value_size;
        handles.push(std::thread::spawn(move || {
            let mut hist = Histogram::<u64>::new(3).expect("histogram");
            let mut value = String::new();
            while value.len() < value_size {
                value.push('x');
            }
            value.truncate(value_size);
            barrier.wait();
            let start = Instant::now();
            for op in 0..ops {
                let key = worker_key(writer, op);
                let op_start = Instant::now();
                let _ = std::hint::black_box(
                    execute_query(
                        engine.as_ref(),
                        &Query::Insert {
                            stream_name: STREAM.to_string(),
                            key: key.clone(),
                            value: serde_json::json!(value),
                        },
                    )
                    .expect("insert"),
                );
                hist.record(op_start.elapsed().as_nanos() as u64).ok();
            }
            (hist, start.elapsed())
        }));
    }
    barrier.wait();
    merge_histograms(handles)
}

#[cfg(feature = "server")]
fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    listener.local_addr().expect("local addr").port()
}

#[cfg(feature = "server")]
fn start_server(engine: Arc<StorageEngine>, data_dir: &std::path::Path, port: u16, workers: usize) {
    use liven::config::{AppConfig, LimitsConfig, SecurityConfig, ServerConfig, StorageConfig};

    let config = AppConfig {
        version: env!("CARGO_PKG_VERSION").to_string(),
        server: ServerConfig {
            environment: "development".to_string(),
            host: "127.0.0.1".to_string(),
            db_port: port,
            webui_port: 0,
            max_connections: 10_000,
            broadcast_capacity: 4096,
        },
        storage: StorageConfig {
            data_directory: data_dir.to_string_lossy().into_owned(),
            max_segment_size_mb: 256,
            sync_mode: "always".to_string(),
        },
        limits: LimitsConfig {
            max_concurrent_streams: 64,
            max_open_file_descriptors: 64,
            max_index_ram_mb: 1024,
            max_segment_size_mb: 256,
            max_scan_results: 100_000,
        },
        security: SecurityConfig {
            mode: "none".to_string(),
            auth_key: None,
            master_key: None,
            ztna: None,
        },
    };

    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(workers.max(8))
            .enable_all()
            .build()
            .expect("server runtime");
        let _ = rt.block_on(liven::server::run_server(engine, config, false));
    });

    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("TCP server did not become reachable on port {port}");
}

#[cfg(feature = "server")]
fn run_tcp(
    engine: &Arc<StorageEngine>,
    args: &Args,
    writers: usize,
    data_dir: &std::path::Path,
) -> (Histogram<u64>, Duration) {
    use liven::client::LivenClient;

    let port = free_port();
    start_server(engine.clone(), data_dir, port, writers);
    let addr = format!("127.0.0.1:{port}");
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(writers.max(4))
        .enable_all()
        .build()
        .expect("client runtime");

    rt.block_on(async move {
        let mut clients = Vec::with_capacity(writers);
        for _ in 0..writers {
            let client = LivenClient::connect_with_auth_mode(&addr, "writebench", "none")
                .await
                .expect("connect");
            clients.push(client);
        }
        let barrier = Arc::new(tokio::sync::Barrier::new(writers));
        let mut handles = Vec::with_capacity(writers);
        for (writer, mut client) in clients.into_iter().enumerate() {
            let barrier = barrier.clone();
            let ops = args.ops;
            let value_size = args.value_size;
            handles.push(tokio::spawn(async move {
                let mut hist = Histogram::<u64>::new(3).expect("histogram");
                let mut value = String::new();
                while value.len() < value_size {
                    value.push('x');
                }
                value.truncate(value_size);
                barrier.wait().await;
                let start = Instant::now();
                for op in 0..ops {
                    let key = worker_key(writer, op);
                    let op_start = Instant::now();
                    let _ = std::hint::black_box(
                        client
                            .insert(STREAM, &key, serde_json::json!(value))
                            .await
                            .expect("tcp insert"),
                    );
                    hist.record(op_start.elapsed().as_nanos() as u64).ok();
                }
                (hist, start.elapsed())
            }));
        }
        let mut merged = Histogram::<u64>::new(3).expect("histogram");
        let mut wall = Duration::ZERO;
        for handle in handles {
            let (hist, elapsed) = handle.await.expect("client task");
            merged.add(&hist).expect("merge histogram");
            wall = wall.max(elapsed);
        }
        (merged, wall)
    })
}

fn print_row(
    mode: &str,
    writers: usize,
    hist: &Histogram<u64>,
    wall: Duration,
    metrics: liven::storage::FlushMetrics,
    ops: usize,
) {
    let secs = wall.as_secs_f64().max(1e-9);
    let total = (writers * ops) as f64;
    println!(
        "| {mode} | {writers} | {:.0} | {:.2} | {:.2} | {:.0} | {:.2} |",
        total / secs,
        hist.value_at_quantile(0.5) as f64 / 1000.0,
        hist.value_at_quantile(0.99) as f64 / 1000.0,
        metrics.fsyncs as f64 / secs,
        metrics.average_batch_size()
    );
}

fn main() {
    let args = parse_args().unwrap_or_else(|e| {
        eprintln!("error: {e}");
        std::process::exit(2);
    });

    println!("# Liven write-path benchmark\n");
    println!("| mode | writers | ops/s | p50 µs | p99 µs | fsync/s | avg batch |");
    println!("|---|---:|---:|---:|---:|---:|---:|");

    for mode in &args.modes {
        for &writers in &args.writers {
            let dir = tempfile::tempdir().expect("tempdir");
            let engine = make_engine(&args, dir.path());
            engine.reset_flush_metrics();

            let (hist, wall) = match mode {
                Mode::Embedded => run_embedded(&engine, &args, writers),
                Mode::ExecutorHold => run_executor_hold(&engine, &args, writers),
                Mode::ExecutorNarrow => run_executor_narrow(&engine, &args, writers),
                #[cfg(feature = "server")]
                Mode::Tcp => run_tcp(&engine, &args, writers, dir.path()),
                #[cfg(not(feature = "server"))]
                Mode::Tcp => panic!("tcp mode requires the server feature"),
            };
            let metrics = engine.flush_metrics();
            let label = match mode {
                Mode::Embedded => "embedded",
                Mode::ExecutorHold => "executor-hold",
                Mode::ExecutorNarrow => "executor-narrow",
                Mode::Tcp => "tcp",
            };
            print_row(label, writers, &hist, wall, metrics, args.ops);
            if *mode == Mode::Tcp {
                // The server thread owns the engine; keep the data dir alive.
                std::mem::forget(dir);
            }
        }
    }

    println!("\nNotes:");
    println!("- executor-hold reproduces the old behavior: write_lock held across the ack wait.");
    println!(
        "- executor-narrow is the current executor path: lock held only through validation and enqueue."
    );
    println!(
        "- `avg batch` is flusher entries / flusher batches; fsync/s is successful fsyncs / wall time."
    );
    println!("- embedded has no global write lock and is the engine ceiling.");
}
