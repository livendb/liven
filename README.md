<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="./assets/logo.svg">
    <img src="./assets/logo.svg" alt="LivenDB" width="80" height="80">
  </picture>
  <br/>
  <strong>LivenDB</strong>
</p>

<p align="center">
  <a href="https://github.com/livendb/liven/actions">
    <img src="https://img.shields.io/github/actions/workflow/status/livendb/liven/ci.yml?branch=main&label=CI&logo=github" alt="CI">
  </a>
  <a href="https://hub.docker.com/r/liven/liven">
    <img src="https://img.shields.io/docker/pulls/liven/liven?logo=docker&label=Docker" alt="Docker">
  </a>
  <a href="https://crates.io/crates/liven">
    <img src="https://img.shields.io/crates/v/liven?logo=rust&label=crates.io" alt="crates.io">
  </a>
  <br/>
  <a href="https://github.com/livendb/liven/blob/main/LICENSE-SSPL">
    <img src="https://img.shields.io/badge/license-SSPL%201.0%20OR%20Commercial-blue" alt="License">
  </a>
</p>

---

> **Stream. Process. Store. One Engine.**

Liven is a database built for data that moves. It ingests streaming data, transforms it on the fly, and stores it durably — all with a single pipeline query language. One binary.

```sh
# One-liner install
curl --proto '=https' --tlsv1.2 -sSf https://livendb.com/install | sh

# Or from source
cargo build --release && ./target/release/liven start
```

---

## Why Liven?

Databases today make you choose: batch or stream? Historical or real-time? Key-value or vector? Liven was built to erase those lines.

**One query language, two modes:**
- Historical queries against stored data
- Real-time subscriptions on the same pipeline — just add `.listen()`

**One engine, three deployment models:**
- Embedded library (~1.5 MB) — runs inside your Rust process
- Network server — TCP + WebSocket, thousands of clients
- Interactive TUI or web dashboard — for ad-hoc queries and monitoring

**Built-in capabilities that usually require separate systems:**
- Vector similarity search (int8 quantized, cosine similarity)
- Stream joins (time-bounded correlate, multi-hop chain)
- Event pattern detection (sequence FSM)
- Time-windowed aggregations
- Full-text substring matching

---

## Quick Start

```bash
# Launch the server
cargo build --release
./target/release/liven start
# → Open http://localhost:43120
# → Admin auth key printed on first start — save it

# Insert and query
liven query 'from("events").insert("e1", {type: "click"})'
liven query 'from("events") | filter(type == "click") | count()'

# Live subscription
liven query 'from("events") | filter(priority == "high") .listen()'
```

### Embedded in Rust
```toml
[dependencies]
liven = { path = "../liven", default-features = false }
```
```rust
use liven::Liven;

let db = Liven::open("./data")?;
db.query(r#"from("events").insert("e1", {type: "click"})"#)?;
let results = db.query(r#"from("events") | filter(type == "click")"#)?;
```

**[Full documentation &rarr;](/docs)**

---

## How It Works (at a glance)

```
Client → Pipeline Query Engine → Append-Only Storage
                                  ↑
                             In-Memory Index
                       (SkipMap + broadcast channel)
```

- **Writes** are appended to segment files. A background flusher batches them for throughput without sacrificing durability.
- **Reads** go through a lock-free in-memory index. Point lookups resolve in microseconds.
- **Subscriptions** broadcast every write to all listeners. The server evaluates pipeline filters before delivery.
- **Compaction** reclaims space from deleted records automatically.
- **Recovery** replays segments on startup. CRC32 checksums catch corruption.

**[Architecture deep dive &rarr;](../docs/architecture.md)**

---

## Installation

```sh
# One-liner (Linux & macOS)
curl --proto '=https' --tlsv1.2 -sSf https://livendb.com/install | sh

# Docker
docker run -p 43121:43121 -p 43120:43120 liven/liven

# From source
git clone https://github.com/livendb/liven && cd liven && cargo build --release
```

[**Installation guide &rarr;](/docs/installation)

---

## Logs

Liven logs to stdout/stderr. View logs based on your platform:

| Platform | Command |
|----------|---------|
| Linux (systemd) | `sudo journalctl -u liven -f` |
| Linux (manual) | `liven start > liven.log 2>&1` |
| macOS (launchd) | `tail -f /usr/local/var/log/liven/stdout.log` |
| macOS (manual) | `liven start > liven.log 2>&1` |
| Windows | `liven.exe start > liven.log 2>&1` |

**Advanced:** Set `RUST_LOG=debug` or `RUST_LOG=trace` for verbose output.

---

## Security

- **Auth-key mode** (default): symmetric keys with BLAKE3 hashing. Roles: read-only, write-only, admin, root. Revoke keys at runtime.
- **mTLS / ZTNA**: mutual TLS with X.509 certificates. Client CN maps to capabilities.
- **Master key**: stored in `./liven.key` (mode 0600). Override with `LIVEN_SECURITY_MASTER_KEY`.

---

## Licensing

**SSPL 1.0 OR Commercial**

- **SSPL** — Free for self-hosting, development, and personal use.
- **Commercial** — Required for managed services or proprietary embedding.

Contact `team@livendb.com` for commercial licensing.

[**Full license &rarr;**](./LICENSE-SSPL)
