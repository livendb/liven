//! Reproducible load benchmark for LivenDB.
//!
//! Run with:
//!   cargo bench --bench load_bench -- --records 100000 --reads 100000 --mode embedded --threads 1,4,8,16
//!
//! This is deliberately not a Criterion benchmark. It uses HdrHistogram and
//! separates insert, reopen/recovery, and point-read phases.

use hdrhistogram::Histogram;
use liven::storage::{RecoveryMode, StorageEngine};
use liven::types::DataValue;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::Serialize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const STREAM: &str = "bench";

// ── CLI ──────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Embedded,
    Tcp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReadPattern {
    Uniform,
    Zipf,
    Sequential,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Warm {
    Hot,
    Cold,
}

#[derive(Debug, Clone)]
struct Args {
    records: usize,
    reads: usize,
    value_size: usize,
    key_len: usize,
    mode: Mode,
    threads: Vec<usize>,
    clients: Vec<usize>,
    batch: usize,
    read_pattern: ReadPattern,
    warm: Warm,
    seed: u64,
    mixed: bool,
    segment_mb: u64,
    max_fds: usize,
    max_index_ram_mb: usize,
    data_dir: Option<PathBuf>,
    skip_insert: bool,
    keep_data: bool,
    tag: String,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            records: 100_000,
            reads: 100_000,
            value_size: 128,
            key_len: 16,
            mode: Mode::Embedded,
            threads: vec![1, 4, 8, 16],
            clients: Vec::new(),
            batch: 500,
            read_pattern: ReadPattern::Uniform,
            warm: Warm::Hot,
            seed: 42,
            mixed: false,
            segment_mb: 256,
            max_fds: 64,
            max_index_ram_mb: 1024,
            data_dir: None,
            skip_insert: false,
            keep_data: false,
            tag: String::new(),
        }
    }
}

fn parse_list(s: &str) -> Result<Vec<usize>, String> {
    let mut out = Vec::new();
    for part in s.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let v: usize = part
            .parse()
            .map_err(|_| format!("invalid list value '{part}'"))?;
        if v == 0 {
            return Err("list values must be > 0".to_string());
        }
        out.push(v);
    }
    if out.is_empty() {
        return Err("empty list".to_string());
    }
    Ok(out)
}

fn parse_args() -> Result<Args, String> {
    let mut a = Args::default();
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        let mut next = |name: &str| -> Result<String, String> {
            it.next().ok_or_else(|| format!("{name} requires a value"))
        };
        match arg.as_str() {
            "--records" => a.records = next("--records")?.parse().map_err(|e| format!("{e}"))?,
            "--reads" => a.reads = next("--reads")?.parse().map_err(|e| format!("{e}"))?,
            "--value-size" => {
                a.value_size = next("--value-size")?.parse().map_err(|e| format!("{e}"))?
            }
            "--key-len" => a.key_len = next("--key-len")?.parse().map_err(|e| format!("{e}"))?,
            "--mode" => {
                a.mode = match next("--mode")?.as_str() {
                    "embedded" => Mode::Embedded,
                    "tcp" => Mode::Tcp,
                    other => return Err(format!("unknown mode '{other}'")),
                }
            }
            "--threads" => a.threads = parse_list(&next("--threads")?)?,
            "--clients" => a.clients = parse_list(&next("--clients")?)?,
            "--batch" => a.batch = next("--batch")?.parse().map_err(|e| format!("{e}"))?,
            "--read-pattern" => {
                a.read_pattern = match next("--read-pattern")?.as_str() {
                    "uniform" => ReadPattern::Uniform,
                    "zipf" => ReadPattern::Zipf,
                    "sequential" => ReadPattern::Sequential,
                    other => return Err(format!("unknown read pattern '{other}'")),
                }
            }
            "--warm" => {
                a.warm = match next("--warm")?.as_str() {
                    "hot" => Warm::Hot,
                    "cold" => Warm::Cold,
                    other => return Err(format!("unknown warm mode '{other}'")),
                }
            }
            "--seed" => a.seed = next("--seed")?.parse().map_err(|e| format!("{e}"))?,
            "--segment-mb" => {
                a.segment_mb = next("--segment-mb")?.parse().map_err(|e| format!("{e}"))?
            }
            "--max-fds" => a.max_fds = next("--max-fds")?.parse().map_err(|e| format!("{e}"))?,
            "--max-index-ram-mb" => {
                a.max_index_ram_mb = next("--max-index-ram-mb")?
                    .parse()
                    .map_err(|e| format!("{e}"))?
            }
            "--data-dir" => a.data_dir = Some(PathBuf::from(next("--data-dir")?)),
            "--tag" => a.tag = next("--tag")?,
            "--skip-insert" => a.skip_insert = true,
            "--keep-data" => a.keep_data = true,
            "--mixed" => a.mixed = true,
            // Cargo appends this for `cargo bench`; harness=false does not consume it.
            "--bench" => {}
            "--help" | "-h" => {
                print_help();
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument '{other}' (use --help)")),
        }
    }

    if a.records == 0 {
        return Err("--records must be > 0".to_string());
    }
    if a.batch == 0 {
        return Err("--batch must be > 0".to_string());
    }
    if a.key_len == 0 || a.key_len > 32 {
        return Err("--key-len must be in 1..=32 (engine limit)".to_string());
    }
    let digits = format!("{:x}", a.records.saturating_sub(1)).len();
    if a.key_len < digits {
        return Err(format!(
            "--key-len {} is too short for {} unique hex keys (need at least {})",
            a.key_len, a.records, digits
        ));
    }
    if a.clients.is_empty() {
        a.clients = a.threads.clone();
    }
    if a.mode == Mode::Tcp && a.clients.len() != a.threads.len() {
        return Err(
            "--threads and --clients must have the same number of entries in tcp mode".to_string(),
        );
    }
    Ok(a)
}

fn print_help() {
    println!(
        "liven load_bench\n\
         \n\
         --records N            records to insert (default 100000)\n\
         --reads N              point reads per read phase (default 100000)\n\
         --value-size N         value bytes (default 128)\n\
         --key-len N            key bytes, 1..=32 (default 16)\n\
         --mode embedded|tcp    read path (default embedded)\n\
         --threads LIST         embedded threads / TCP runtime threads (default 1,4,8,16)\n\
         --clients LIST         TCP clients (default: same as --threads)\n\
         --batch N              append_batch size (default 500)\n\
         --read-pattern P       uniform|zipf|sequential (default uniform)\n\
         --warm hot|cold        page-cache warmup (default hot)\n\
         --seed N               deterministic seed (default 42)\n\
         --segment-mb N         max segment size (default 256)\n\
         --max-fds N            file-handle cache limit (default 64)\n\
         --max-index-ram-mb N   explicit index budget (default 1024)\n\
         --mixed                run optional 90/10 mixed phase\n\
         --data-dir PATH        use an existing data dir (for profiling)\n\
         --tag NAME             suffix for output filenames\n\
         --skip-insert          skip insert/recover, use --data-dir\n\
         --keep-data            keep the temporary data dir\n\
         --help                 show this help\n"
    );
}

// ── Data generation ──────────────────────────────────────────────────────────

fn key_for(i: usize, key_len: usize) -> String {
    format!("{:0width$x}", i, width = key_len)
}

fn value_for(i: usize, size: usize, seed: u64) -> String {
    let mut s = format!("{:016x}:{:016x}:", seed, i);
    if s.len() > size {
        s.truncate(size);
        return s;
    }
    while s.len() < size {
        s.push('x');
    }
    s
}

fn build_zipf_cdf(n: usize) -> Arc<Vec<f64>> {
    let mut cdf = Vec::with_capacity(n);
    let mut sum = 0.0f64;
    for i in 0..n {
        sum += 1.0 / ((i + 1) as f64).powf(1.1);
        cdf.push(sum);
    }
    if sum > 0.0 {
        for v in &mut cdf {
            *v /= sum;
        }
    }
    Arc::new(cdf)
}

fn pick_index(
    rng: &mut StdRng,
    pattern: ReadPattern,
    seq: usize,
    records: usize,
    cdf: Option<&Vec<f64>>,
) -> usize {
    match pattern {
        ReadPattern::Sequential => seq % records,
        ReadPattern::Uniform => rng.gen_range(0..records),
        ReadPattern::Zipf => {
            let cdf = cdf.expect("zipf cdf");
            let u = rng.r#gen::<f64>();
            let idx = cdf.partition_point(|&x| x < u);
            idx.min(records - 1)
        }
    }
}

// ── Measurement helpers ──────────────────────────────────────────────────────

#[derive(Serialize, Clone, Default)]
struct Lat {
    p50_ns: u64,
    p99_ns: u64,
    p999_ns: u64,
    max_ns: u64,
}

fn lat_from_values(values: &[u64]) -> Lat {
    if values.is_empty() {
        return Lat::default();
    }
    let mut h = Histogram::<u64>::new(3).expect("histogram");
    for &v in values {
        let _ = h.record(v.max(1));
    }
    Lat {
        p50_ns: h.value_at_quantile(0.50),
        p99_ns: h.value_at_quantile(0.99),
        p999_ns: h.value_at_quantile(0.999),
        max_ns: h.max(),
    }
}

fn fmt_ns(ns: u64) -> String {
    if ns >= 1_000_000 {
        format!("{:.2}ms", ns as f64 / 1_000_000.0)
    } else if ns >= 1_000 {
        format!("{:.2}µs", ns as f64 / 1_000.0)
    } else {
        format!("{}ns", ns)
    }
}

#[cfg(target_os = "linux")]
fn rss_bytes() -> u64 {
    if let Ok(statm) = std::fs::read_to_string("/proc/self/statm")
        && let Some(pages) = statm.split_whitespace().nth(1)
        && let Ok(pages) = pages.parse::<u64>()
    {
        return pages * 4096;
    }
    0
}

#[cfg(target_os = "macos")]
fn rss_bytes() -> u64 {
    if let Ok(out) = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        && let Ok(s) = String::from_utf8(out.stdout)
        && let Ok(kb) = s.trim().parse::<u64>()
    {
        return kb * 1024;
    }
    0
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn rss_bytes() -> u64 {
    0
}

fn disk_stats(dir: &Path) -> (u64, u64) {
    let mut count = 0u64;
    let mut bytes = 0u64;
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("liven")
                && let Ok(meta) = path.metadata()
            {
                count += 1;
                bytes += meta.len();
            }
        }
    }
    (count, bytes)
}

fn cmd_stdout(program: &str, args: &[&str]) -> Option<String> {
    std::process::Command::new(program)
        .args(args)
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .filter(|s| !s.is_empty())
}

fn cpu_model() -> String {
    #[cfg(target_os = "macos")]
    {
        if let Some(v) = cmd_stdout("sysctl", &["-n", "machdep.cpu.brand_string"]) {
            return v;
        }
    }
    #[cfg(target_os = "linux")]
    {
        if let Ok(cpuinfo) = std::fs::read_to_string("/proc/cpuinfo") {
            for line in cpuinfo.lines() {
                if let Some(rest) = line.strip_prefix("model name") {
                    if let Some((_, v)) = rest.split_once(':') {
                        return v.trim().to_string();
                    }
                }
            }
        }
    }
    "unverified".to_string()
}

fn total_ram_mb() -> u64 {
    #[cfg(target_os = "macos")]
    {
        if let Some(v) = cmd_stdout("sysctl", &["-n", "hw.memsize"])
            && let Ok(bytes) = v.parse::<u64>()
        {
            return bytes / 1024 / 1024;
        }
    }
    #[cfg(target_os = "linux")]
    {
        if let Ok(meminfo) = std::fs::read_to_string("/proc/meminfo") {
            for line in meminfo.lines() {
                if let Some(rest) = line.strip_prefix("MemTotal:") {
                    if let Some(kb) = rest.split_whitespace().next()
                        && let Ok(kb) = kb.parse::<u64>()
                    {
                        return kb / 1024;
                    }
                }
            }
        }
    }
    #[allow(unreachable_code)]
    0
}

fn os_version() -> String {
    cmd_stdout("uname", &["-srm"]).unwrap_or_else(|| std::env::consts::OS.to_string())
}

fn fs_type(dir: &Path) -> String {
    let d = dir.to_string_lossy().to_string();
    #[cfg(target_os = "linux")]
    {
        if let Some(v) = cmd_stdout("stat", &["-f", "-c", "%T", &d]) {
            return v;
        }
    }
    #[cfg(target_os = "macos")]
    {
        if let Some(v) = cmd_stdout("stat", &["-f", "%T", &d]) {
            return v;
        }
    }
    "unverified".to_string()
}

fn storage_type() -> String {
    #[cfg(target_os = "macos")]
    {
        if let Some(out) = cmd_stdout("diskutil", &["info", "/"]) {
            for line in out.lines() {
                if line.contains("Device / Media Name") {
                    if let Some((_, v)) = line.split_once(':') {
                        return v.trim().to_string();
                    }
                }
            }
        }
    }
    #[cfg(target_os = "linux")]
    {
        if let Some(v) = cmd_stdout("lsblk", &["-ndo", "NAME,ROTA,TRAN"]) {
            return v.lines().next().unwrap_or("unverified").to_string();
        }
    }
    "unverified".to_string()
}

fn git_commit() -> String {
    let manifest = env!("CARGO_MANIFEST_DIR");
    cmd_stdout("git", &["-C", manifest, "rev-parse", "--short", "HEAD"])
        .unwrap_or_else(|| "unknown".to_string())
}

fn git_dirty() -> bool {
    let manifest = env!("CARGO_MANIFEST_DIR");
    std::process::Command::new("git")
        .args(["-C", manifest, "status", "--porcelain"])
        .output()
        .map(|o| !o.stdout.is_empty())
        .unwrap_or(false)
}

// ── Result types ─────────────────────────────────────────────────────────────

#[derive(Serialize)]
struct Meta {
    utc: String,
    date: String,
    commit: String,
    dirty: bool,
    rustc: String,
    profile: String,
    cpu: String,
    cores: usize,
    ram_mb: u64,
    os: String,
    filesystem: String,
    storage: String,
    tag: String,
    args: String,
}

#[derive(Serialize)]
struct InsertResult {
    records: usize,
    batch: usize,
    elapsed_s: f64,
    ops_per_s: f64,
    logical_mb_per_s: f64,
    disk_mb_per_s: f64,
    batch_latency: Lat,
    disk_segments: u64,
    disk_bytes: u64,
    rss_bytes: u64,
    index_ram_bytes: u64,
    skipmap_len: usize,
}

#[derive(Serialize)]
struct RecoverResult {
    rebuild_s: f64,
    rss_before_bytes: u64,
    rss_after_bytes: u64,
    disk_segments: u64,
    disk_bytes: u64,
    skipmap_len: usize,
}

#[derive(Serialize, Clone)]
struct PhaseResult {
    phase: String,
    mode: String,
    threads: usize,
    clients: usize,
    pattern: String,
    warm: String,
    ops: u64,
    elapsed_s: f64,
    ops_per_s: f64,
    latency: Lat,
    mismatches: u64,
}

#[derive(Serialize)]
struct MixedResult {
    mode: String,
    threads: usize,
    clients: usize,
    ops: u64,
    reads: u64,
    writes: u64,
    elapsed_s: f64,
    ops_per_s: f64,
    read_latency: Lat,
    write_latency: Lat,
    errors: u64,
}

#[derive(Serialize)]
struct RunResult {
    meta: Meta,
    insert: Option<InsertResult>,
    recover: Option<RecoverResult>,
    reads: Vec<PhaseResult>,
    mixed: Option<MixedResult>,
    notes: Vec<String>,
}

// ── Engine setup and phases ──────────────────────────────────────────────────

fn open_engine(dir: &Path, args: &Args) -> liven::error::Result<StorageEngine> {
    let mut engine = StorageEngine::new_with_limits(
        dir,
        args.segment_mb * 1024 * 1024,
        RecoveryMode::Default,
        64,
        args.max_index_ram_mb as u64 * 1024 * 1024,
        args.max_fds,
    )?;
    engine.set_max_scan_results(100_000);
    // Background compaction is a separate known-buggy area; disable it here.
    engine.compaction_threshold_segments = usize::MAX;
    engine.compaction_threshold_bytes = u64::MAX;
    Ok(engine)
}

fn run_insert(
    engine: &StorageEngine,
    data_dir: &Path,
    keys: &[String],
    values: &[String],
    args: &Args,
) -> InsertResult {
    let mut batch_ns = Vec::with_capacity(args.records.div_ceil(args.batch));
    let started = Instant::now();
    let mut start = 0usize;
    while start < args.records {
        let end = (start + args.batch).min(args.records);
        let batch: Vec<(String, String, DataValue)> = (start..end)
            .map(|i| {
                (
                    STREAM.to_string(),
                    keys[i].clone(),
                    DataValue::String(values[i].clone()),
                )
            })
            .collect();
        let t = Instant::now();
        engine.append_batch(batch).expect("append_batch failed");
        batch_ns.push(t.elapsed().as_nanos() as u64);
        start = end;
    }
    let elapsed = started.elapsed();
    let (disk_segments, disk_bytes) = disk_stats(data_dir);
    InsertResult {
        records: args.records,
        batch: args.batch,
        elapsed_s: elapsed.as_secs_f64(),
        ops_per_s: args.records as f64 / elapsed.as_secs_f64(),
        logical_mb_per_s: (args.records * args.value_size) as f64
            / 1_000_000.0
            / elapsed.as_secs_f64(),
        disk_mb_per_s: disk_bytes as f64 / 1_000_000.0 / elapsed.as_secs_f64(),
        batch_latency: lat_from_values(&batch_ns),
        disk_segments,
        disk_bytes,
        rss_bytes: rss_bytes(),
        index_ram_bytes: engine.index_ram_bytes.load(Ordering::Relaxed),
        skipmap_len: engine.skipmap.len(),
    }
}

fn run_recover(
    engine: StorageEngine,
    dir: &Path,
    args: &Args,
) -> liven::error::Result<(StorageEngine, RecoverResult)> {
    let rss_before = rss_bytes();
    drop(engine);
    let started = Instant::now();
    let engine = open_engine(dir, args)?;
    let rebuild = started.elapsed();
    let (disk_segments, disk_bytes) = disk_stats(dir);
    let result = RecoverResult {
        rebuild_s: rebuild.as_secs_f64(),
        rss_before_bytes: rss_before,
        rss_after_bytes: rss_bytes(),
        disk_segments,
        disk_bytes,
        skipmap_len: engine.skipmap.len(),
    };
    Ok((engine, result))
}

fn warmup_embedded(engine: &StorageEngine, keys: &[String]) {
    for key in keys {
        let _ = engine.get(STREAM, key);
    }
}

fn run_embedded_reads(
    engine: &Arc<StorageEngine>,
    keys: &Arc<Vec<String>>,
    values: &Arc<Vec<String>>,
    args: &Args,
    threads: usize,
    cdf: Option<Arc<Vec<f64>>>,
) -> PhaseResult {
    let reads = args.reads;
    let per = reads / threads;
    let rem = reads % threads;
    let mut all = Vec::with_capacity(reads);
    let mut mismatches = 0u64;
    let started = Instant::now();

    std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(threads);
        for t in 0..threads {
            let count = per + usize::from(t < rem);
            let engine = engine.clone();
            let keys = keys.clone();
            let values = values.clone();
            let cdf = cdf.clone();
            let pattern = args.read_pattern;
            let seed = args.seed;
            let base_seq = t * per + t.min(rem);
            handles.push(scope.spawn(move || {
                let mut rng =
                    StdRng::seed_from_u64(seed ^ ((t as u64 + 1) * 0x9E37_79B9_7F4A_7C15));
                let mut local = Vec::with_capacity(count);
                let mut mis = 0u64;
                for j in 0..count {
                    let idx =
                        pick_index(&mut rng, pattern, base_seq + j, keys.len(), cdf.as_deref());
                    let t0 = Instant::now();
                    let res = engine.get(STREAM, &keys[idx]);
                    let ns = t0.elapsed().as_nanos() as u64;
                    local.push(ns);
                    match res {
                        Ok(Some(rec)) => {
                            if !matches!(&rec.value, DataValue::String(s) if s == &values[idx]) {
                                mis += 1;
                            }
                        }
                        _ => mis += 1,
                    }
                }
                (local, mis)
            }));
        }
        for h in handles {
            let (mut v, m) = h.join().expect("read worker panicked");
            all.append(&mut v);
            mismatches += m;
        }
    });

    let elapsed = started.elapsed();
    PhaseResult {
        phase: "read".to_string(),
        mode: "embedded".to_string(),
        threads,
        clients: 0,
        pattern: format!("{:?}", args.read_pattern).to_lowercase(),
        warm: format!("{:?}", args.warm).to_lowercase(),
        ops: reads as u64,
        elapsed_s: elapsed.as_secs_f64(),
        ops_per_s: reads as f64 / elapsed.as_secs_f64(),
        latency: lat_from_values(&all),
        mismatches,
    }
}

// ── TCP ──────────────────────────────────────────────────────────────────────

#[cfg(feature = "server")]
fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    listener.local_addr().expect("local_addr").port()
}

#[cfg(feature = "server")]
fn wait_for_port(port: u16, timeout: Duration) -> bool {
    let addr = format!("127.0.0.1:{port}");
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if std::net::TcpStream::connect(&addr).is_ok() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    false
}

#[cfg(feature = "server")]
fn start_tcp_server(
    engine: Arc<StorageEngine>,
    data_dir: &Path,
    args: &Args,
) -> (u16, std::thread::JoinHandle<()>) {
    use liven::config::{AppConfig, LimitsConfig, SecurityConfig, ServerConfig, StorageConfig};
    let port = free_port();
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
            max_segment_size_mb: args.segment_mb as usize,
            sync_mode: "always".to_string(),
        },
        limits: LimitsConfig {
            max_concurrent_streams: 64,
            max_open_file_descriptors: args.max_fds,
            max_index_ram_mb: args.max_index_ram_mb,
            max_segment_size_mb: args.segment_mb as usize,
            max_scan_results: 100_000,
        },
        security: SecurityConfig {
            mode: "none".to_string(),
            auth_key: None,
            master_key: None,
            ztna: None,
        },
    };

    let handle = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("server runtime");
        let _ = rt.block_on(liven::server::run_server(engine, config, false));
    });

    assert!(
        wait_for_port(port, Duration::from_secs(10)),
        "TCP server did not become reachable on port {port}"
    );
    (port, handle)
}

#[cfg(feature = "server")]
async fn run_tcp_reads_once(
    addr: &str,
    keys: &Arc<Vec<String>>,
    values: &Arc<Vec<String>>,
    args: &Args,
    threads: usize,
    clients: usize,
    cdf: Option<Arc<Vec<f64>>>,
) -> PhaseResult {
    use futures_util::future::join_all;
    use liven::client::LivenClient;

    let mut connected = Vec::with_capacity(clients);
    for _ in 0..clients {
        let client = LivenClient::connect_with_auth_mode(addr, "bench", "none")
            .await
            .expect("connect client");
        connected.push(client);
    }

    let reads = args.reads;
    let per = reads / clients;
    let rem = reads % clients;
    let started = Instant::now();
    let mut tasks = Vec::with_capacity(clients);
    for (t, mut client) in connected.into_iter().enumerate() {
        let count = per + usize::from(t < rem);
        let keys = keys.clone();
        let values = values.clone();
        let cdf = cdf.clone();
        let pattern = args.read_pattern;
        let seed = args.seed;
        let base_seq = t * per + t.min(rem);
        tasks.push(tokio::spawn(async move {
            let mut rng = StdRng::seed_from_u64(seed ^ ((t as u64 + 1) * 0x9E37_79B9_7F4A_7C15));
            let mut local = Vec::with_capacity(count);
            let mut mis = 0u64;
            for j in 0..count {
                let idx = pick_index(&mut rng, pattern, base_seq + j, keys.len(), cdf.as_deref());
                let t0 = Instant::now();
                let res = client.get(STREAM, &keys[idx]).await;
                let ns = t0.elapsed().as_nanos() as u64;
                local.push(ns);
                match res {
                    Ok(recs) => {
                        if recs.len() != 1
                            || !matches!(&recs[0].value, DataValue::String(s) if s == &values[idx])
                        {
                            mis += 1;
                        }
                    }
                    Err(_) => mis += 1,
                }
            }
            (local, mis)
        }));
    }

    let joined = join_all(tasks).await;
    let mut all = Vec::with_capacity(reads);
    let mut mismatches = 0u64;
    for r in joined {
        let (mut v, m) = r.expect("client task panicked");
        all.append(&mut v);
        mismatches += m;
    }
    let elapsed = started.elapsed();

    PhaseResult {
        phase: "read".to_string(),
        mode: "tcp".to_string(),
        threads,
        clients,
        pattern: format!("{:?}", args.read_pattern).to_lowercase(),
        warm: format!("{:?}", args.warm).to_lowercase(),
        ops: reads as u64,
        elapsed_s: elapsed.as_secs_f64(),
        ops_per_s: reads as f64 / elapsed.as_secs_f64(),
        latency: lat_from_values(&all),
        mismatches,
    }
}

// ── Cache dropping / notes ───────────────────────────────────────────────────

fn try_drop_caches() -> String {
    #[cfg(target_os = "linux")]
    {
        let _ = std::process::Command::new("sync").status();
        match std::fs::write("/proc/sys/vm/drop_caches", b"3\n") {
            Ok(()) => "cold: dropped Linux page cache (wrote /proc/sys/vm/drop_caches)".to_string(),
            Err(e) => format!("cold: SKIPPED (cannot write /proc/sys/vm/drop_caches: {e})"),
        }
    }
    #[cfg(target_os = "macos")]
    {
        match std::process::Command::new("purge").status() {
            Ok(s) if s.success() => "cold: ran purge".to_string(),
            Ok(_) => "cold: SKIPPED (purge failed; typically requires root)".to_string(),
            Err(e) => format!("cold: SKIPPED (purge unavailable: {e})"),
        }
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        "cold: SKIPPED (unsupported platform)".to_string()
    }
}

fn syscall_note() -> String {
    #[cfg(target_os = "linux")]
    {
        if cmd_stdout("which", &["strace"]).is_some() {
            "syscalls: run separately with `strace -c -f -e trace=openat,newfstatat,pread64 cargo bench --bench load_bench -- ...`; not captured inline.".to_string()
        } else {
            "syscalls: unverified (strace not installed)".to_string()
        }
    }
    #[cfg(target_os = "macos")]
    {
        "syscalls: unverified (dtruss requires root; no strace on macOS)".to_string()
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        "syscalls: unverified (unsupported platform)".to_string()
    }
}

// ── Mixed phase ──────────────────────────────────────────────────────────────

fn run_embedded_mixed(
    engine: &Arc<StorageEngine>,
    keys: &Arc<Vec<String>>,
    values: &Arc<Vec<String>>,
    args: &Args,
    threads: usize,
    cdf: Option<Arc<Vec<f64>>>,
) -> MixedResult {
    let reads = args.reads;
    let per = reads / threads;
    let rem = reads % threads;
    let mix_counter = Arc::new(AtomicU64::new(0));
    let started = Instant::now();
    let mut read_lat = Vec::new();
    let mut write_lat = Vec::new();
    let mut read_ops = 0u64;
    let mut write_ops = 0u64;
    let mut errors = 0u64;

    std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(threads);
        for t in 0..threads {
            let count = per + usize::from(t < rem);
            let engine = engine.clone();
            let keys = keys.clone();
            let values = values.clone();
            let cdf = cdf.clone();
            let counter = mix_counter.clone();
            let pattern = args.read_pattern;
            let seed = args.seed;
            let base_seq = t * per + t.min(rem);
            handles.push(scope.spawn(move || {
                let mut rng =
                    StdRng::seed_from_u64(seed ^ ((t as u64 + 2) * 0x9E37_79B9_7F4A_7C15));
                let mut r_local = Vec::new();
                let mut w_local = Vec::new();
                let mut r_ops = 0u64;
                let mut w_ops = 0u64;
                let mut err = 0u64;
                for j in 0..count {
                    if rng.r#gen::<f64>() < 0.9 {
                        let idx =
                            pick_index(&mut rng, pattern, base_seq + j, keys.len(), cdf.as_deref());
                        let t0 = Instant::now();
                        let res = engine.get(STREAM, &keys[idx]);
                        let ns = t0.elapsed().as_nanos() as u64;
                        r_local.push(ns);
                        match res {
                            Ok(Some(rec)) => {
                                if !matches!(&rec.value, DataValue::String(s) if s == &values[idx])
                                {
                                    err += 1;
                                }
                            }
                            _ => err += 1,
                        }
                        r_ops += 1;
                    } else {
                        let n = counter.fetch_add(1, Ordering::Relaxed);
                        let key = format!(
                            "{:0width$x}",
                            args.records + n as usize,
                            width = args.key_len
                        );
                        let value =
                            value_for(args.records + n as usize, args.value_size, args.seed);
                        let t0 = Instant::now();
                        let res = engine.append(STREAM, &key, DataValue::String(value), false);
                        let ns = t0.elapsed().as_nanos() as u64;
                        w_local.push(ns);
                        if res.is_err() {
                            err += 1;
                        }
                        w_ops += 1;
                    }
                }
                (r_local, w_local, r_ops, w_ops, err)
            }));
        }
        for h in handles {
            let (mut r, mut w, ro, wo, e) = h.join().expect("mixed worker panicked");
            read_lat.append(&mut r);
            write_lat.append(&mut w);
            read_ops += ro;
            write_ops += wo;
            errors += e;
        }
    });

    let elapsed = started.elapsed();
    MixedResult {
        mode: "embedded".to_string(),
        threads,
        clients: 0,
        ops: read_ops + write_ops,
        reads: read_ops,
        writes: write_ops,
        elapsed_s: elapsed.as_secs_f64(),
        ops_per_s: (read_ops + write_ops) as f64 / elapsed.as_secs_f64(),
        read_latency: lat_from_values(&read_lat),
        write_latency: lat_from_values(&write_lat),
        errors,
    }
}

#[cfg(feature = "server")]
async fn run_tcp_mixed(
    addr: &str,
    keys: &Arc<Vec<String>>,
    values: &Arc<Vec<String>>,
    args: &Args,
    threads: usize,
    clients: usize,
    cdf: Option<Arc<Vec<f64>>>,
) -> MixedResult {
    use futures_util::future::join_all;
    use liven::client::LivenClient;

    let mut connected = Vec::with_capacity(clients);
    for _ in 0..clients {
        connected.push(
            LivenClient::connect_with_auth_mode(addr, "bench", "none")
                .await
                .expect("connect client"),
        );
    }
    let reads = args.reads;
    let per = reads / clients;
    let rem = reads % clients;
    let counter = Arc::new(AtomicU64::new(0));
    let started = Instant::now();
    let mut tasks = Vec::with_capacity(clients);
    for (t, mut client) in connected.into_iter().enumerate() {
        let count = per + usize::from(t < rem);
        let keys = keys.clone();
        let values = values.clone();
        let cdf = cdf.clone();
        let counter = counter.clone();
        let pattern = args.read_pattern;
        let seed = args.seed;
        let base_seq = t * per + t.min(rem);
        let records = args.records;
        let key_len = args.key_len;
        let value_size = args.value_size;
        tasks.push(tokio::spawn(async move {
            let mut rng =
                StdRng::seed_from_u64(seed ^ ((t as u64 + 2) * 0x9E37_79B9_7F4A_7C15));
            let mut r_local = Vec::new();
            let mut w_local = Vec::new();
            let mut r_ops = 0u64;
            let mut w_ops = 0u64;
            let mut err = 0u64;
            for j in 0..count {
                if rng.r#gen::<f64>() < 0.9 {
                    let idx =
                        pick_index(&mut rng, pattern, base_seq + j, keys.len(), cdf.as_deref());
                    let t0 = Instant::now();
                    let res = client.get(STREAM, &keys[idx]).await;
                    let ns = t0.elapsed().as_nanos() as u64;
                    r_local.push(ns);
                    match res {
                        Ok(recs) => {
                            if recs.len() != 1
                                || !matches!(&recs[0].value, DataValue::String(s) if s == &values[idx])
                            {
                                err += 1;
                            }
                        }
                        Err(_) => err += 1,
                    }
                    r_ops += 1;
                } else {
                    let n = counter.fetch_add(1, Ordering::Relaxed);
                    let key = format!("{:0width$x}", records + n as usize, width = key_len);
                    let value = value_for(records + n as usize, value_size, seed);
                    let t0 = Instant::now();
                    let res = client
                        .insert(STREAM, &key, serde_json::Value::String(value))
                        .await;
                    let ns = t0.elapsed().as_nanos() as u64;
                    w_local.push(ns);
                    if res.is_err() {
                        err += 1;
                    }
                    w_ops += 1;
                }
            }
            (r_local, w_local, r_ops, w_ops, err)
        }));
    }
    let joined = join_all(tasks).await;
    let mut read_lat = Vec::new();
    let mut write_lat = Vec::new();
    let mut read_ops = 0u64;
    let mut write_ops = 0u64;
    let mut errors = 0u64;
    for r in joined {
        let (mut rl, mut wl, ro, wo, e) = r.expect("mixed client task panicked");
        read_lat.append(&mut rl);
        write_lat.append(&mut wl);
        read_ops += ro;
        write_ops += wo;
        errors += e;
    }
    let elapsed = started.elapsed();
    MixedResult {
        mode: "tcp".to_string(),
        threads,
        clients,
        ops: read_ops + write_ops,
        reads: read_ops,
        writes: write_ops,
        elapsed_s: elapsed.as_secs_f64(),
        ops_per_s: (read_ops + write_ops) as f64 / elapsed.as_secs_f64(),
        read_latency: lat_from_values(&read_lat),
        write_latency: lat_from_values(&write_lat),
        errors,
    }
}

// ── Output ───────────────────────────────────────────────────────────────────

fn markdown(result: &RunResult) -> String {
    let mut s = String::new();
    s.push_str("# Liven load benchmark\n\n");
    s.push_str(&format!(
        "- utc: `{}`\n- commit: `{}`{}\n- rustc: `{}`\n- profile: `{}`\n- cpu: `{}` ({} cores)\n- ram_mb: {}\n- os: `{}`\n- filesystem: `{}`\n- storage: `{}`\n- args: `{}`\n\n",
        result.meta.utc,
        result.meta.commit,
        if result.meta.dirty { " (dirty)" } else { "" },
        result.meta.rustc,
        result.meta.profile,
        result.meta.cpu,
        result.meta.cores,
        result.meta.ram_mb,
        result.meta.os,
        result.meta.filesystem,
        result.meta.storage,
        result.meta.args
    ));

    s.push_str("## Summary\n\n");
    s.push_str(
        "| phase | mode | threads | clients | ops/s | p50 | p99 | p99.9 | max | mismatches |\n",
    );
    s.push_str("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|\n");
    if let Some(ins) = &result.insert {
        s.push_str(&format!(
            "| insert | - | - | - | {:.0} | {} | {} | {} | {} | - |\n",
            ins.ops_per_s,
            fmt_ns(ins.batch_latency.p50_ns),
            fmt_ns(ins.batch_latency.p99_ns),
            fmt_ns(ins.batch_latency.p999_ns),
            fmt_ns(ins.batch_latency.max_ns)
        ));
    }
    if let Some(rec) = &result.recover {
        s.push_str(&format!(
            "| recover | - | - | - | - | - | - | - | - | - |\n"
        ));
        let _ = rec;
    }
    for r in &result.reads {
        s.push_str(&format!(
            "| {} | {} | {} | {} | {:.0} | {} | {} | {} | {} | {} |\n",
            r.phase,
            r.mode,
            r.threads,
            r.clients,
            r.ops_per_s,
            fmt_ns(r.latency.p50_ns),
            fmt_ns(r.latency.p99_ns),
            fmt_ns(r.latency.p999_ns),
            fmt_ns(r.latency.max_ns),
            r.mismatches
        ));
    }
    if let Some(m) = &result.mixed {
        s.push_str(&format!(
            "| mixed | {} | {} | {} | {:.0} | {} | {} | - | - | {} |\n",
            m.mode,
            m.threads,
            m.clients,
            m.ops_per_s,
            fmt_ns(m.read_latency.p50_ns),
            fmt_ns(m.read_latency.p99_ns),
            m.errors
        ));
    }

    if let Some(ins) = &result.insert {
        s.push_str("\n## Insert\n\n");
        s.push_str(&format!(
            "- records: {}, batch: {}, elapsed: {:.3}s\n- ops/s: {:.0}, logical MB/s: {:.2}, disk MB/s: {:.2}\n- batch p50/p99/p99.9/max: {} / {} / {} / {}\n- segments: {}, bytes: {}\n- rss: {} bytes, index_ram_bytes: {}, skipmap_len: {}\n",
            ins.records,
            ins.batch,
            ins.elapsed_s,
            ins.ops_per_s,
            ins.logical_mb_per_s,
            ins.disk_mb_per_s,
            fmt_ns(ins.batch_latency.p50_ns),
            fmt_ns(ins.batch_latency.p99_ns),
            fmt_ns(ins.batch_latency.p999_ns),
            fmt_ns(ins.batch_latency.max_ns),
            ins.disk_segments,
            ins.disk_bytes,
            ins.rss_bytes,
            ins.index_ram_bytes,
            ins.skipmap_len
        ));
    }

    if let Some(rec) = &result.recover {
        s.push_str("\n## Recover\n\n");
        s.push_str(&format!(
            "- rebuild: {:.3}s\n- rss before/after: {} / {} bytes\n- segments: {}, bytes: {}, skipmap_len: {}\n",
            rec.rebuild_s,
            rec.rss_before_bytes,
            rec.rss_after_bytes,
            rec.disk_segments,
            rec.disk_bytes,
            rec.skipmap_len
        ));
    }

    s.push_str("\n## Notes\n\n");
    for n in &result.notes {
        s.push_str(&format!("- {n}\n"));
    }
    s
}

fn print_summary(result: &RunResult) {
    println!(
        "\n{:<10} {:<10} {:>8} {:>8} {:>12} {:>10} {:>10} {:>10}",
        "phase", "mode", "threads", "clients", "ops/s", "p50", "p99", "p99.9"
    );
    if let Some(ins) = &result.insert {
        println!(
            "{:<10} {:<10} {:>8} {:>8} {:>12.0} {:>10} {:>10} {:>10}",
            "insert",
            "-",
            "-",
            "-",
            ins.ops_per_s,
            fmt_ns(ins.batch_latency.p50_ns),
            fmt_ns(ins.batch_latency.p99_ns),
            fmt_ns(ins.batch_latency.p999_ns)
        );
    }
    if result.recover.is_some() {
        println!("{:<10} {:<10} {:>8} {:>8}", "recover", "-", "-", "-");
    }
    for r in &result.reads {
        println!(
            "{:<10} {:<10} {:>8} {:>8} {:>12.0} {:>10} {:>10} {:>10}",
            r.phase,
            r.mode,
            r.threads,
            r.clients,
            r.ops_per_s,
            fmt_ns(r.latency.p50_ns),
            fmt_ns(r.latency.p99_ns),
            fmt_ns(r.latency.p999_ns)
        );
    }
    if let Some(m) = &result.mixed {
        println!(
            "{:<10} {:<10} {:>8} {:>8} {:>12.0} {:>10} {:>10} {:>10}",
            "mixed",
            m.mode,
            m.threads,
            m.clients,
            m.ops_per_s,
            fmt_ns(m.read_latency.p50_ns),
            fmt_ns(m.read_latency.p99_ns),
            "-"
        );
    }
}

fn write_outputs(result: &RunResult) -> std::io::Result<(PathBuf, PathBuf)> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("bench-results");
    std::fs::create_dir_all(&dir)?;
    let stem = if result.meta.tag.is_empty() {
        format!("{}-{}", result.meta.date, result.meta.commit)
    } else {
        format!(
            "{}-{}-{}",
            result.meta.date, result.meta.commit, result.meta.tag
        )
    };
    let json_path = dir.join(format!("{stem}.json"));
    let md_path = dir.join(format!("{stem}.md"));
    std::fs::write(&json_path, serde_json::to_string_pretty(result).unwrap())?;
    std::fs::write(&md_path, markdown(result))?;
    Ok((json_path, md_path))
}

// ── Main ─────────────────────────────────────────────────────────────────────

fn main() {
    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(2);
        }
    };

    let date = cmd_stdout("date", &["-u", "+%Y-%m-%d"]).unwrap_or_else(|| "1970-01-01".to_string());
    let utc =
        cmd_stdout("date", &["-u", "+%Y-%m-%dT%H:%M:%SZ"]).unwrap_or_else(|| "unknown".to_string());

    let (run_dir, data_dir) = match &args.data_dir {
        Some(d) => {
            if !d.exists() {
                eprintln!("error: --data-dir does not exist: {}", d.display());
                std::process::exit(2);
            }
            (d.clone(), d.clone())
        }
        None => {
            let dir = std::env::temp_dir().join(format!(
                "liven_load_bench_{}_{}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos()
            ));
            let data = dir.join("data");
            std::fs::create_dir_all(&data).expect("create temp data dir");
            (dir, data)
        }
    };

    // The server writes liven.status into its cwd; keep it inside the run dir.
    let original_cwd = std::env::current_dir().ok();
    if args.mode == Mode::Tcp && std::env::set_current_dir(&run_dir).is_err() {
        eprintln!("warning: could not chdir to {}", run_dir.display());
    }

    let mut notes = Vec::new();
    notes.push("insert uses StorageEngine::append_batch directly in both modes; TCP measures the read-path overhead on top.".to_string());
    notes.push(format!(
        "background compaction disabled (thresholds: segments={}, bytes={}); segment_mb={}, max_fds={}, max_index_ram_mb={}",
        usize::MAX,
        u64::MAX,
        args.segment_mb,
        args.max_fds,
        args.max_index_ram_mb
    ));
    notes.push(format!(
        "key_len={} value_size={} batch={} seed={}",
        args.key_len, args.value_size, args.batch, args.seed
    ));
    if args.warm == Warm::Cold {
        notes.push("cold: cache-drop attempted before each thread/client run".to_string());
    } else {
        notes.push("hot: one untimed warmup pass over all keys before the read sweep".to_string());
    }
    notes.push(syscall_note());
    notes.push("flamegraph: see bench-results/flamegraph-* (generated separately with cargo flamegraph/sample)".to_string());

    let keys: Vec<String> = (0..args.records)
        .map(|i| key_for(i, args.key_len))
        .collect();
    let values: Vec<String> = (0..args.records)
        .map(|i| value_for(i, args.value_size, args.seed))
        .collect();
    let keys = Arc::new(keys);
    let values = Arc::new(values);
    let cdf = if args.read_pattern == ReadPattern::Zipf {
        Some(build_zipf_cdf(args.records))
    } else {
        None
    };

    let mut insert_result = None;
    let mut recover_result = None;
    let mut engine: StorageEngine;

    if args.skip_insert {
        notes.push(
            "skip-insert: using existing --data-dir; insert and recover phases skipped".to_string(),
        );
        engine = match open_engine(&data_dir, &args) {
            Ok(e) => e,
            Err(e) => {
                eprintln!("error: open_engine failed: {e}");
                std::process::exit(1);
            }
        };
    } else {
        engine = match open_engine(&data_dir, &args) {
            Ok(e) => e,
            Err(e) => {
                eprintln!("error: open_engine failed: {e}");
                std::process::exit(1);
            }
        };
        let ins = run_insert(&engine, &data_dir, &keys, &values, &args);
        println!(
            "INSERT: {:.0} ops/s, {:.2} MB/s logical, batch p50 {} p99 {} p99.9 {}, disk {} segments / {} bytes",
            ins.ops_per_s,
            ins.logical_mb_per_s,
            fmt_ns(ins.batch_latency.p50_ns),
            fmt_ns(ins.batch_latency.p99_ns),
            fmt_ns(ins.batch_latency.p999_ns),
            ins.disk_segments,
            ins.disk_bytes
        );
        insert_result = Some(ins);
        match run_recover(engine, &data_dir, &args) {
            Ok((e, rec)) => {
                println!(
                    "RECOVER: {:.3}s, skipmap_len={}, rss {} -> {}",
                    rec.rebuild_s, rec.skipmap_len, rec.rss_before_bytes, rec.rss_after_bytes
                );
                recover_result = Some(rec);
                engine = e;
            }
            Err(e) => {
                eprintln!("error: recover failed: {e}");
                std::process::exit(1);
            }
        }
    }

    if args.warm == Warm::Hot {
        warmup_embedded(&engine, &keys);
    }

    let engine = Arc::new(engine);
    let mut reads = Vec::new();
    let mut mixed = None;

    match args.mode {
        Mode::Embedded => {
            for &threads in &args.threads {
                if args.warm == Warm::Cold {
                    let note = try_drop_caches();
                    if !notes.iter().any(|n| n == &note) {
                        notes.push(note);
                    }
                }
                let r = run_embedded_reads(&engine, &keys, &values, &args, threads, cdf.clone());
                println!(
                    "READ embedded threads={threads}: {:.0} ops/s, p50 {} p99 {} p99.9 {}, mismatches {}",
                    r.ops_per_s,
                    fmt_ns(r.latency.p50_ns),
                    fmt_ns(r.latency.p99_ns),
                    fmt_ns(r.latency.p999_ns),
                    r.mismatches
                );
                reads.push(r);
            }

            if args.mixed {
                let best = reads
                    .iter()
                    .max_by(|a, b| a.ops_per_s.partial_cmp(&b.ops_per_s).unwrap())
                    .expect("read results");
                let threads = best.threads;
                let m = run_embedded_mixed(&engine, &keys, &values, &args, threads, cdf.clone());
                println!(
                    "MIXED embedded threads={threads}: {:.0} ops/s ({} reads / {} writes), p99 read {} write {}",
                    m.ops_per_s,
                    m.reads,
                    m.writes,
                    fmt_ns(m.read_latency.p99_ns),
                    fmt_ns(m.write_latency.p99_ns)
                );
                mixed = Some(m);
            }
        }
        Mode::Tcp => {
            #[cfg(feature = "server")]
            {
                let (port, _server_handle) = start_tcp_server(engine.clone(), &data_dir, &args);
                let addr = format!("127.0.0.1:{port}");

                for (&threads, &clients) in args.threads.iter().zip(args.clients.iter()) {
                    if args.warm == Warm::Cold {
                        let note = try_drop_caches();
                        if !notes.iter().any(|n| n == &note) {
                            notes.push(note);
                        }
                    }
                    let rt = tokio::runtime::Builder::new_multi_thread()
                        .worker_threads(threads.max(1))
                        .enable_all()
                        .build()
                        .expect("client runtime");
                    let r = rt.block_on(run_tcp_reads_once(
                        &addr,
                        &keys,
                        &values,
                        &args,
                        threads,
                        clients,
                        cdf.clone(),
                    ));
                    rt.shutdown_timeout(Duration::from_secs(1));
                    println!(
                        "READ tcp threads={threads} clients={clients}: {:.0} ops/s, p50 {} p99 {} p99.9 {}, mismatches {}",
                        r.ops_per_s,
                        fmt_ns(r.latency.p50_ns),
                        fmt_ns(r.latency.p99_ns),
                        fmt_ns(r.latency.p999_ns),
                        r.mismatches
                    );
                    reads.push(r);
                }

                if args.mixed {
                    let best = reads
                        .iter()
                        .max_by(|a, b| a.ops_per_s.partial_cmp(&b.ops_per_s).unwrap())
                        .expect("read results");
                    let (threads, clients) = (best.threads, best.clients);
                    let rt = tokio::runtime::Builder::new_multi_thread()
                        .worker_threads(threads.max(1))
                        .enable_all()
                        .build()
                        .expect("client runtime");
                    let m = rt.block_on(run_tcp_mixed(
                        &addr,
                        &keys,
                        &values,
                        &args,
                        threads,
                        clients,
                        cdf.clone(),
                    ));
                    rt.shutdown_timeout(Duration::from_secs(1));
                    println!(
                        "MIXED tcp threads={threads} clients={clients}: {:.0} ops/s ({} reads / {} writes), p99 read {} write {}",
                        m.ops_per_s,
                        m.reads,
                        m.writes,
                        fmt_ns(m.read_latency.p99_ns),
                        fmt_ns(m.write_latency.p99_ns)
                    );
                    mixed = Some(m);
                }
            }
            #[cfg(not(feature = "server"))]
            {
                eprintln!(
                    "error: tcp mode requires the `server` feature (build with default features)"
                );
                std::process::exit(2);
            }
        }
    }

    let meta = Meta {
        utc,
        date,
        commit: git_commit(),
        dirty: git_dirty(),
        rustc: cmd_stdout("rustc", &["--version"]).unwrap_or_else(|| "unverified".to_string()),
        profile: if cfg!(debug_assertions) {
            "debug".to_string()
        } else {
            "release".to_string()
        },
        cpu: cpu_model(),
        cores: std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(0),
        ram_mb: total_ram_mb(),
        os: os_version(),
        filesystem: fs_type(&data_dir),
        storage: storage_type(),
        tag: args.tag.clone(),
        args: format!("{args:?}"),
    };

    let result = RunResult {
        meta,
        insert: insert_result,
        recover: recover_result,
        reads,
        mixed,
        notes,
    };

    let (json_path, md_path) = match write_outputs(&result) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("error: writing results: {e}");
            std::process::exit(1);
        }
    };

    print_summary(&result);
    println!("\nJSON: {}", json_path.display());
    println!("Markdown: {}", md_path.display());

    let failed = result.reads.iter().any(|r| r.mismatches > 0)
        || result.mixed.as_ref().is_some_and(|m| m.errors > 0);
    if failed {
        eprintln!("FAIL: read verification mismatches detected");
        std::process::exit(1);
    }

    if args.data_dir.is_none() && !args.keep_data {
        // Restore cwd before deleting the run dir when possible.
        if args.mode == Mode::Tcp
            && let Some(cwd) = original_cwd
        {
            let _ = std::env::set_current_dir(cwd);
        }
        let _ = std::fs::remove_dir_all(&run_dir);
    }
}
