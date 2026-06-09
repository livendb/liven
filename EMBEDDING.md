# Embedding LIVEN

LIVEN can be used as an embedded library inside your Rust application.
No separate process, no network, no configuration files required.

## Add to your project

```toml
[dependencies]
liven = { path = "../liven", default-features = false }
```

For a minimal embedded build (no server, no TUI, no TLS):
```toml
[dependencies]
liven = { path = "../liven", default-features = false }
```

For embedded with TLS support:
```toml
[dependencies]
liven = { path = "../liven", default-features = false, features = ["tls"] }
```

## Basic usage

```rust
use liven::Liven;

fn main() -> Result<(), String> {
    // Open or create a database
    let db = Liven::open("./myapp_data")?;

    // Insert a record
    db.query(r#"from("events").insert("e1", {type: "click", user: "u1"})"#)?;

    // Query records
    let results = db.query(r#"from("events") | filter(type == "click")"#)?;
    println!("Found {} records", results.len());

    Ok(())
}
```

## Custom configuration

```rust
use liven::embed::{Liven, LivenConfig};

let db = Liven::open_with_config("./data", LivenConfig {
    max_streams: 16,
    max_index_ram_mb: 8,
    max_segment_mb: 8,
    max_open_fds: 32,
    broadcast_capacity: 1024,
})?;
```

## Real-time subscriptions (async)

```rust
use liven::Liven;
use tokio::sync::broadcast;

#[tokio::main]
async fn main() -> Result<(), String> {
    let db = Liven::open("./data")?;
    let mut rx = db.subscribe();

    // Write in background
    let db2 = db.engine();
    tokio::spawn(async move {
        // writes trigger broadcast automatically
    });

    // Receive records as they are written
    while let Ok(record) = rx.recv().await {
        println!("New record: {:?}", record);
    }

    Ok(())
}
```

## Real-time subscriptions (sync)

```rust
use liven::Liven;
use std::time::Duration;

let db = Liven::open("./data")?;

loop {
    if let Ok(Some(record)) = db.subscribe_sync(Duration::from_millis(100)) {
        println!("New record: {:?}", record);
    }
}
```

## Binary size

Build with only the features you need:

| Feature set | Approximate binary size |
|---|---|
| Embedded only (default-features = false) | ~1.5MB |
| Embedded + TLS | ~2MB |
| Full server + TUI | ~7.6MB |

## Use cases

### AI memory linking
```rust
let db = Liven::open("./agent_memory")?;
db.query(r#"from("prompts").insert("p1", {text: "Hello", model: "gpt-4"})"#)?;
db.query(r#"from("prompts") | chain("responses", "prompt_id") | chain("memory", "response_id")"#)?;
```

### Fraud detection
```rust
let db = Liven::open("./transactions")?;
db.query(r#"from("transactions") | correlate("logins", "user_id", within: 60000) | filter(login_count == 0)"#)?;
```

### Telemetry and predictive failure
```rust
let db = Liven::open("./telemetry")?;
db.query(r#"from("metrics") | sequence(event == "cpu_spike", then: event == "memory_leak", within: 30000)"#)?;
```
