# Liven Rust Crate API Reference

Complete reference for using Liven as a Rust library — either embedded (in-process)
or as a client over the wire protocol.

## Table of Contents

- [Adding the Dependency](#adding-the-dependency)
- [Initialization](#initialization)
- [CRUD Operations](#crud-operations)
- [Pipeline Operations](#pipeline-operations)
- [Pipeline Builder](#pipeline-builder)
- [Batch Operations](#batch-operations)
- [Metadata](#metadata)
- [Explain](#explain)
- [Pipeline Update / Delete](#pipeline-update--delete)
- [Real-Time Subscriptions](#real-time-subscriptions)
- [Metrics & Compaction](#metrics--compaction)
- [Typed Filters](#typed-filters)
- [Working with Records](#working-with-records)
- [Configuration](#configuration)
- [Full Examples](#full-examples)

---

## Adding the Dependency

```toml
[dependencies]
liven = "0.0.4"                        # full build (server, TUI, TLS)
```

For a minimal embedded build with no server, TUI, or TLS:

```toml
[dependencies]
liven = { version = "0.0.4", default-features = false }      # core only
```

Select individual features:

```toml
[dependencies]
liven = { version = "0.0.4", default-features = false, features = ["tls"] }   # core + TLS
liven = { version = "0.0.4", default-features = false, features = ["server", "tls"] }  # core + server + TLS
liven = { version = "0.0.4", features = ["tui"] }  # full + TUI (already included)
```

---

## Initialization

Liven provides two usage modes with the same method signatures.

### Embedded — `Liven`

Opens a database at a filesystem path. All operations run in-process.

```rust
use liven::Liven;
use liven::embed::LivenConfig;

// Default config
let db = Liven::open("./data")?;

// Custom config
let db = Liven::open_with_config("./data", LivenConfig {
    max_streams: 128,
    max_index_ram_mb: 1024,
    ..Default::default()
})?;
```

### Wire — `LivenClient`

Connects to a remote Liven server over TCP. All operations are async.

```rust
use liven::client::LivenClient;

// Plain TCP (no authentication)
let mut client = LivenClient::connect("127.0.0.1:43121").await?;

// With auth key in URL
let mut client = LivenClient::connect("127.0.0.1:43121?auth_key=my_secret").await?;
```

---

## CRUD Operations

Every CRUD method below exists on both `Liven` (sync) and `LivenClient` (async).

### insert

Insert a single record into a stream.

```rust
use serde_json::json;

// Embedded (sync)
db.insert("users", "u1", json!({"name": "Alice", "email": "alice@x.com"}))?;

// Wire (async)
client.insert("users", "u1", json!({"name": "Alice"})).await?;
```

### upsert

Insert a record or replace it if the key already exists.

```rust
db.upsert("users", "u1", json!({"name": "Alice", "email": "alice@new.com"}))?;
```

### update

Update specific fields on an existing record (merges with current value).

```rust
db.update("users", "u1", json!({"status": "active"}))?;
```

### get

Retrieve a single record by key.

```rust
let result = db.get("users", "u1")?;
```

### delete

Delete a single record by key.

```rust
db.delete("users", "u1")?;
```

### clear

Clear all records from a stream without removing the stream itself.

```rust
db.clear("logs")?;
```

### drop_stream

Drop a stream and all its data entirely.

```rust
db.drop_stream("temp_data")?;
```

### insert_many

Insert multiple records in a single batch.

```rust
db.insert_many("orders", vec![
    ("o1".into(), json!({"amount": 100, "status": "pending"})),
    ("o2".into(), json!({"amount": 200, "status": "completed"})),
    ("o3".into(), json!({"amount": 150, "status": "pending"})),
])?;
```

### upsert_many

Upsert multiple records in a single batch.

```rust
db.upsert_many("orders", vec![
    ("o1".into(), json!({"amount": 110})),
    ("o4".into(), json!({"amount": 300})),
])?;
```

---

## Pipeline Operations

These are shorthand methods for common single-stage pipelines.
Each builds `Pipeline::from(stream) | stage` internally.

All methods exist on both `Liven` (sync) and `LivenClient` (async).

### filter

Filter records by a condition.

```rust
use liven::query::Filter;

// Embedded
db.filter("events", Filter::field("type").eq("click"))?;

// Wire
client.filter("events", Filter::field("type").eq("click")).await?;
```

### limit

Limit the number of results.

```rust
db.limit("events", 10)?;
```

### count

Count records in a stream.

```rust
let result = db.count("events")?;
```

### sort

Sort results by a field.

```rust
db.sort("orders", "amount", true)?;   // descending
db.sort("orders", "created_at", false)?; // ascending
```

### page

Paginate through results (1-based page number).

```rust
db.page("events", 1, 50)?;  // page 1, 50 items per page
```

### page_cursor

Cursor-based pagination.

```rust
db.page_cursor("events", "cursor_abc123", 50)?;
```

### map

Project specific fields from records.

```rust
db.map("users", vec!["name".into(), "email".into()])?;
```

### window

Time-windowed aggregation.

```rust
use liven::types::AggregateStrategy;

db.window("metrics", 60_000, AggregateStrategy::avg())?;
db.window("events", 30_000, AggregateStrategy::count())?;
db.window("orders", 86_400_000, AggregateStrategy::sum())?;
```

### group

Group records by a field with aggregations.

```rust
db.group("events", "type", vec!["count".into()])?;
db.group("orders", "status", vec!["sum(amount)".into(), "count".into()])?;
```

### distinct

Deduplicate records by a specific field.

```rust
db.distinct("users", "email")?;
```

### vector_filter

Vector similarity search on an int8 quantized vector field with a similarity threshold.

```rust
db.vector_filter("embeddings", "vector", vec![12, -5, 3, 0, -8], 0.85)?;
```

### enrich

Left-join records from another stream.

```rust
db.enrich("logs", "users", "user_id")?;
```

### correlate

Windowed join: links records from two streams on a shared key within a time window.
Used for behavioral correlation — fraud detection, anomaly signals, session linking.

```rust
db.correlate("events", "orders", "user_id", 5000)?;
```

### chain

Multi-hop join: follows key relationships across streams hop by hop.
Used for AI memory linking, transaction lineage, multi-step event tracing.

```rust
db.chain("prompts", "responses", "prompt_id")?;
```

### sequence

Ordered event pattern detection within a time window using a finite state machine.
Used for predictive failure detection, fraud pattern matching, behavioral flow analysis.

```rust
use liven::query::Filter;

db.sequence("system_events", vec![
    Filter::field("event").eq("disk_full"),
    Filter::field("event").eq("crash"),
], 10_000)?;
```

---

## Pipeline Builder

For complex chains with multiple stages, use the `Pipeline` builder.

```rust
use liven::query::{Pipeline, Filter};
use liven::types::AggregateStrategy;

let pipeline = Pipeline::from("orders")
    .filter(Filter::field("status").eq("completed"))
    .filter(Filter::field("amount").gte(100.0))
    .sort("amount", true)
    .limit(10);

// Execute — both modes
db.run(pipeline.clone())?;                     // embedded
client.run(&pipeline.build()).await?;          // wire
```

### Build variants

```rust
// Standard pipeline query
let q = pipeline.build();            // -> Query::Pipeline

// Live subscription
let q = pipeline.build_listen();     // -> Query::Listen

// Update matching records
let q = pipeline.build_update(json!({"status": "archived"}));  // -> Query::PipelineUpdate

// Delete matching records
let q = pipeline.build_delete();     // -> Query::PipelineDelete
```

### Builder stages

| Method | PipelineStage | Description |
|--------|--------------|-------------|
| `.filter(f)` | `Filter` | Filter by condition |
| `.get(key)` | `Get` | Get by key |
| `.map(fields)` | `Map` | Field projection |
| `.limit(n)` | `Limit` | Limit results |
| `.count()` | `Count` | Count results |
| `.sort(field, desc)` | `Sort` | Sort by field |
| `.page(n, size)` | `Page` | Paginate |
| `.page_cursor(c, size)` | `PageCursor` | Cursor pagination |
| `.window(ms, strategy)` | `Window` | Time-windowed aggregation |
| `.group(field, aggs)` | `Group` | Group by field |
| `.distinct(field)` | `Distinct` | Deduplicate |
| `.vector_filter(field, vec, threshold)` | `VectorFilter` | Vector similarity |
| `.enrich(stream, key)` | `Enrich` | Left join |
| `.correlate(stream, key, ms)` | `Correlate` | Windowed join |
| `.chain(stream, key)` | `Chain` | Multi-hop join |
| `.sequence(steps, ms)` | `Sequence` | Event pattern FSM |

---

## Batch Operations

```rust
// insert_many
db.insert_many("orders", vec![
    ("o1".into(), json!({"amount": 100})),
    ("o2".into(), json!({"amount": 200})),
])?;

// upsert_many
db.upsert_many("orders", vec![
    ("o1".into(), json!({"amount": 150})),
    ("o3".into(), json!({"amount": 300})),
])?;
```

---

## Metadata

```rust
// List all streams
let streams = db.streams()?;

// Server status
let status = db.status()?;
```

---

## Explain

Returns the execution plan of a query without running it.

```rust
use liven::query::Query as Q;

let plan = db.explain(Q::insert("events", "e1", json!({"x": 1})))?;
```

---

## Pipeline Update / Delete

Update or delete all records matching a pipeline filter.

```rust
let pipeline = Pipeline::from("orders")
    .filter(Filter::field("status").eq("pending"));

// Update all matching records
db.pipeline_update(pipeline.clone(), json!({"status": "cancelled"}))?;

// Delete all matching records
db.pipeline_delete(pipeline)?;
```

---

## Real-Time Subscriptions

### Embedded — blocking (non-async)

```rust
use std::time::Duration;

loop {
    if let Some(record) = db.subscribe_sync(Duration::from_millis(100))? {
        println!("New record: key={}, value={:?}", record.key, record.value);
    }
}
```

### Embedded — async (Tokio)

```rust
let mut rx = db.subscribe();

tokio::spawn(async move {
    while let Ok(record) = rx.recv().await {
        println!("Live record: {:?}", record);
    }
});
```

### Wire — streaming

```rust
use futures_util::StreamExt;

let mut stream = client.listen("events").await?;
while let Some(Ok(record)) = stream.next().await {
    println!("Got record: key={}", record.key);
}

// Or with formatted output
client.tail_stream("events", "json").await?;
```

---

## Metrics & Compaction

Embedded-only — storage engine introspection.

```rust
// Database metrics: (ram_bytes, disk_bytes, segments, streams)
let (ram, disk, segments, streams) = db.metrics()?;

println!(
    "RAM: {} MB | Disk: {} MB | Segments: {} | Streams: {}",
    ram / 1024 / 1024,
    disk / 1024 / 1024,
    segments,
    streams,
);

// Manual compaction
db.compact()?;

// Auto-compaction (requires Tokio runtime)
db.start_auto_compact(
    tokio::runtime::Handle::current(),
    std::time::Duration::from_secs(60),
);
```

---

## Typed Filters

The `Filter` builder creates typed filter expressions without string parsing.

### Comparisons

```rust
use liven::query::Filter;

Filter::field("status").eq("active")           // ==
Filter::field("amount").ne(0)                   // !=
Filter::field("age").gt(18)                     // >
Filter::field("score").gte(90.0)                // >=
Filter::field("priority").lt(3)                 // <
Filter::field("temperature").lte(100.0)         // <=
```

### String matching

```rust
Filter::field("name").contains("alice")         // substring
Filter::field("email").starts_with("admin")     // prefix
Filter::field("path").ends_with(".log")         // suffix
```

### Range and membership

```rust
Filter::field("amount").between(10.0, 100.0)    // inclusive range
Filter::field("role").in(vec!["admin", "moderator", "owner"])
```

### Compound logic

```rust
// AND
Filter::and(vec![
    Filter::field("status").eq("active"),
    Filter::field("age").gte(18),
])

// OR
Filter::or(vec![
    Filter::field("role").eq("admin"),
    Filter::field("role").eq("owner"),
])

// NOT
Filter::not(Filter::field("status").eq("deleted"))
```

### Using filters in pipeline builder

```rust
let pipeline = Pipeline::from("users")
    .filter(Filter::and(vec![
        Filter::field("status").eq("active"),
        Filter::field("age").gte(18),
        Filter::or(vec![
            Filter::field("plan").eq("premium"),
            Filter::field("plan").eq("enterprise"),
        ]),
    ]))
    .limit(100);
```

---

## Working with Records

Query results are returned as `Vec<Record>`.

```rust
use liven::types::DataValue;

pub struct Record {
    pub sequence_id: u64,    // Monotonic sequence number
    pub timestamp: i64,      // Unix millisecond timestamp
    pub stream_name: String, // Source stream
    pub key: String,         // Record key
    pub value: DataValue,    // The stored value
}
```

The `value` field is a `DataValue` enum:

```rust
match &record.value {
    DataValue::String(s) => println!("String: {}", s),
    DataValue::Int(n) => println!("Integer: {}", n),
    DataValue::UInt(n) => println!("Unsigned: {}", n),
    DataValue::Float(f) => println!("Float: {}", f),
    DataValue::Bool(b) => println!("Bool: {}", b),
    DataValue::Null => println!("Null"),
    DataValue::Object(obj) => println!("Object: {:?}", obj),
    DataValue::Array(arr) => println!("Array: {:?}", arr),
    DataValue::Vector(vec) => println!("Vector ({} dims)", vec.len()),
    DataValue::Binary(b) => println!("Binary ({} bytes)", b.len()),
}
```

---

## Configuration

### Embedded config

```rust
use liven::embed::LivenConfig;

let config = LivenConfig {
    max_streams: 128,                    // Max concurrent streams
    max_index_ram_mb: 1024,              // Max in-memory index (MB)
    max_segment_mb: 32,                  // Max segment file size (MB)
    max_open_fds: 64,                    // Max cached file descriptors
    broadcast_capacity: 4096,            // Subscription channel capacity
    compaction_threshold_segments: 4,    // Compaction trigger (segments)
    compaction_threshold_bytes: 64_000_000, // Compaction trigger (bytes)
    max_scan_results: 100_000,           // Max scan results
};

let db = Liven::open_with_config("./data", config)?;
```

### Feature flags

| Feature | What's included |
|---------|----------------|
| `full` | All features below (default) |
| `server` | REST API + WebSocket + embedded Web UI |
| `tui` | Interactive terminal dashboard |
| `tls` | mTLS support with X.509 certificates |

```sh
# Minimal embedded build
cargo build --release --no-default-features

# Embedded with TLS
cargo build --release --no-default-features --features tls
```

---

## Full Examples

### Embedded — complete program

```rust
use liven::Liven;
use liven::query::{Pipeline, Filter};
use serde_json::json;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = format!("./liven_demo_{}", std::process::id());
    let db = Liven::open(&dir)?;

    // Insert records
    db.insert("events", "e1", json!({"type": "click", "value": 10}))?;
    db.insert("events", "e2", json!({"type": "purchase", "value": 50}))?;
    db.insert("events", "e3", json!({"type": "click", "value": 20}))?;

    // Count clicks
    let count = db.filter("events", Filter::field("type").eq("click"))?;
    println!("Clicks: {:?}", count);

    // Pipeline query
    let results = db.run(
        Pipeline::from("events")
            .filter(Filter::field("value").gt(15))
            .sort("value", true)
            .limit(5)
    )?;
    println!("Top results: {:?}", results);

    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}
```

### Wire — async client

```rust
use liven::client::LivenClient;
use liven::query::{Pipeline, Filter};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = LivenClient::connect("127.0.0.1:43121").await?;

    // Insert
    client.insert("events", "e1", json!({"type": "click"})).await?;

    // Query with filter
    let results = client.filter("events", Filter::field("type").eq("click")).await?;
    println!("Results: {:?}", results);

    // Pipeline query
    let results = client.run(
        &Pipeline::from("events")
            .filter(Filter::field("value").gt(10))
            .limit(10)
            .build()
    ).await?;

    Ok(())
}
```

### Mixed — embedded with subscriptions

```rust
use liven::Liven;
use liven::query::{Pipeline, Filter};
use serde_json::json;
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Liven::open("./liven_data")?;

    // Subscribe to live updates in a background thread
    let subscriber = db.engine();
    std::thread::spawn(move || {
        let mut rx = subscriber.subscribe();
        loop {
            if let Ok(record) = rx.recv() {
                println!("[LIVE] {} -> {:?}", record.key, record.value);
            }
        }
    });

    // Insert some data (triggers subscription)
    db.insert("sensors", "s1", json!({"temp": 22.5}))?;
    db.insert("sensors", "s2", json!({"temp": 23.1}))?;

    // Query
    let hot = db.filter("sensors", Filter::field("temp").gte(23.0))?;
    println!("Hot sensors: {:?}", hot);

    // Compact
    db.compact()?;

    Ok(())
}
```
