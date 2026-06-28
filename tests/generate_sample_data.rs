#[allow(unused_imports)]
use fs2::FileExt;
use liven::storage::StorageEngine;
use liven::types::{DataValue, parse_json_to_datavalue};
use std::fs;

fn post_http_json(addr: &str, path: &str, json_payload: &str) -> Result<(), String> {
    use std::io::{Read, Write};
    use std::net::TcpStream;
    use std::time::Duration;

    let mut stream = TcpStream::connect(addr).map_err(|e| format!("Connection failed: {}", e))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .map_err(|e| e.to_string())?;
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .map_err(|e| e.to_string())?;

    let request = format!(
        "POST {} HTTP/1.1\r\n\
         Host: {}\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\r\n\
         {}",
        path,
        addr,
        json_payload.len(),
        json_payload
    );

    stream
        .write_all(request.as_bytes())
        .map_err(|e| format!("Failed to send request: {}", e))?;

    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .map_err(|e| format!("Failed to read response: {}", e))?;

    if response.contains("200 OK") {
        Ok(())
    } else {
        Err(format!("Server returned error: {}", response))
    }
}

enum AppendTarget {
    Engine(StorageEngine),
    Http {
        addr: String,
        batch: Vec<serde_json::Value>,
    },
}

impl AppendTarget {
    fn append(
        &mut self,
        stream: &str,
        key: &str,
        value: DataValue,
    ) -> Result<(), liven::error::LivenError> {
        match self {
            AppendTarget::Engine(engine) => engine.append(stream, key, value, false).map(|_| ()),
            AppendTarget::Http { batch, .. } => {
                let val_json = match value {
                    DataValue::Null => serde_json::Value::Null,
                    DataValue::Bool(b) => serde_json::Value::Bool(b),
                    DataValue::Int(i) => serde_json::Value::Number(serde_json::Number::from(i)),
                    DataValue::UInt(u) => serde_json::Value::Number(serde_json::Number::from(u)),
                    DataValue::Float(f) => serde_json::Value::Number(
                        serde_json::Number::from_f64(*f).unwrap_or(serde_json::Number::from(0)),
                    ),
                    DataValue::String(s) => {
                        if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&s) {
                            parsed
                        } else {
                            serde_json::Value::String(s)
                        }
                    }
                    DataValue::Binary(b) => serde_json::Value::String(format!("{:?}", b)),
                    DataValue::Array(_) => serde_json::Value::Array(vec![]),
                    DataValue::Object(obj) => {
                        let mut obj_map = serde_json::Map::new();
                        for (k, v) in obj {
                            // Recursively convert nested DataValue to JSON
                            let json_val = match v {
                                DataValue::Null => serde_json::Value::Null,
                                DataValue::Bool(b) => serde_json::Value::Bool(b),
                                DataValue::Int(i) => {
                                    serde_json::Value::Number(serde_json::Number::from(i))
                                }
                                DataValue::UInt(u) => {
                                    serde_json::Value::Number(serde_json::Number::from(u))
                                }
                                DataValue::Float(f) => serde_json::Value::Number(
                                    serde_json::Number::from_f64(*f)
                                        .unwrap_or(serde_json::Number::from(0)),
                                ),
                                DataValue::String(s) => serde_json::Value::String(s.clone()),
                                DataValue::Binary(b) => {
                                    serde_json::Value::String(format!("{:?}", b))
                                }
                                DataValue::Array(_) => serde_json::Value::Array(vec![]),
                                DataValue::Object(_) => {
                                    serde_json::Value::Object(serde_json::Map::new())
                                }
                                DataValue::Vector(v) => serde_json::Value::Array(
                                    v.iter()
                                        .map(|x| {
                                            serde_json::Value::Number(serde_json::Number::from(*x))
                                        })
                                        .collect(),
                                ),
                            };
                            obj_map.insert(k.clone(), json_val);
                        }
                        serde_json::Value::Object(obj_map)
                    }
                    DataValue::Vector(v) => serde_json::Value::Array(
                        v.into_iter()
                            .map(|x| serde_json::Value::Number(serde_json::Number::from(x)))
                            .collect(),
                    ),
                };
                batch.push(serde_json::json!({
                    "stream": stream,
                    "key": key,
                    "value": val_json,
                }));
                Ok(())
            }
        }
    }

    fn flush(&mut self) -> Result<(), liven::error::LivenError> {
        if let AppendTarget::Http { addr, batch } = self
            && !batch.is_empty()
        {
            let json_str = serde_json::to_string(batch).map_err(|e| e.to_string())?;
            post_http_json(addr, "/api/ingest", &json_str)
                .map_err(liven::error::LivenError::Storage)?;
            batch.clear();
        }
        Ok(())
    }

    fn compact(&mut self) -> Result<(), liven::error::LivenError> {
        match self {
            AppendTarget::Engine(engine) => engine.compact().map(|_| ()),
            AppendTarget::Http { .. } => {
                // HTTP compaction endpoint has been removed - compaction is now automatic
                // For testing purposes, we'll just return Ok since we can't trigger it via HTTP
                Ok(())
            }
        }
    }
}

#[test]
fn test_generate_sample_data() {
    let target_dir = std::env::var("LIVEN_DATA_DIR").unwrap_or_else(|_| "./data".to_string());

    // Detect if the directory is locked by trying to obtain try_lock_shared on any existing .liven segments
    let is_locked = if let Ok(entries) = fs::read_dir(&target_dir) {
        let mut locked = false;
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file()
                && path.extension().and_then(|s| s.to_str()) == Some("liven")
                && let Ok(file) = fs::File::open(&path)
                && file.try_lock_shared().is_err()
            {
                locked = true;
                break;
            }
        }
        locked
    } else {
        false
    };

    let mut target = if is_locked {
        println!(
            "Active database directory '{}' is locked by a running server. Pushing sample data directly to server HTTP endpoint.",
            target_dir
        );
        AppendTarget::Http {
            addr: "127.0.0.1:43120".to_string(),
            batch: Vec::new(),
        }
    } else {
        println!(
            "Writing sample data directly to active database directory: {}",
            target_dir
        );
        let engine = StorageEngine::new(&target_dir, 10 * 1024 * 1024)
            .expect("Failed to create storage engine");
        AppendTarget::Engine(engine)
    };

    println!("Injecting rich AI Agents datasets...");

    // 1. Ingest realistic AI Agents logs and telemetry
    let ai_agents_data = vec![
        (
            "nexus-9-init",
            r#"{"agent_id": "nexus-9", "model": "gemini-1.5-pro", "task": "anomaly_detection", "event": "initialize", "timestamp": 1780245000, "status": "active"}"#,
        ),
        (
            "nexus-9-step1",
            r#"{"agent_id": "nexus-9", "model": "gemini-1.5-pro", "task": "anomaly_detection", "event": "analyze_stream", "stream_monitored": "iot_telemetry", "reasoning_steps": ["load_history", "compare_deltas", "flag_outlier"], "status": "processing", "tokens_processed": 14205}"#,
        ),
        (
            "nexus-9-completed",
            r#"{"agent_id": "nexus-9", "model": "gemini-1.5-pro", "task": "anomaly_detection", "event": "finish", "findings": {"anomalies_detected": 1, "target_key": "sensor_power_99"}, "status": "completed", "tokens_processed": 18450}"#,
        ),
        (
            "clippy-max-refactor",
            r#"{"agent_id": "clippy-max", "model": "gpt-4o", "task": "refactoring", "event": "ast_scan", "file": "src/storage.rs", "reasoning_steps": ["scan_ast", "apply_linter_rules"], "status": "active", "tokens_processed": 8590}"#,
        ),
        (
            "clippy-max-success",
            r#"{"agent_id": "clippy-max", "model": "gpt-4o", "task": "refactoring", "event": "apply_patch", "lines_changed": 15, "test_status": "passed", "status": "completed", "tokens_processed": 12100}"#,
        ),
        (
            "sentinel-alpha-sentiment",
            r#"{"agent_id": "sentinel-alpha", "model": "claude-3.5-sonnet", "task": "market_sentiment_analysis", "event": "fetch_socials", "sources": ["twitter", "reddit", "farcaster"], "sentiment_score": 0.82, "status": "completed", "tokens_processed": 25400}"#,
        ),
        (
            "sentinel-alpha-trade",
            r#"{"agent_id": "sentinel-alpha", "model": "claude-3.5-sonnet", "task": "defi_arbitrage", "event": "execute_trade", "trade_details": {"dex": "uniswap_v3", "token_in": "USDC", "token_out": "ETH", "amount_in": 100000.0, "expected_profit_usd": 450.0}, "status": "pending"}"#,
        ),
        (
            "hector-overflow",
            r#"{"agent_id": "hector", "model": "claude-3-opus", "task": "semantic_embedding", "event": "vectorize_corpus", "status": "failed", "error": "context_window_overflow", "tokens_processed": 200000}"#,
        ),
    ];

    for (key, json_str) in ai_agents_data {
        // Convert JSON string to native Object datatype
        let obj_value = parse_json_to_datavalue(json_str);
        target
            .append("ai_agents", key, obj_value)
            .expect("Failed to append AI agent telemetry");
    }

    println!("Injecting rich Peer-to-Peer Prediction Markets data...");

    // 2. Ingest realistic Peer-to-Peer Social Prediction Markets telemetry
    let prediction_markets_data = vec![
        (
            "market_create_agi",
            r#"{"market_id": "pm_agi_2026", "market_title": "Will AGI be achieved in 2026?", "creator": "0x88fA...9921", "initial_yes_odds": 0.50, "initial_no_odds": 0.50, "liquidity_pool_usdc": 100000.0, "status": "open"}"#,
        ),
        (
            "bet_placed_user_1",
            r#"{"market_id": "pm_agi_2026", "event": "bet_placed", "trader": "@whale_watcher", "side": "yes", "shares_bought": 50000, "total_cost_usdc": 25000.0, "post_bet_yes_odds": 0.58, "post_bet_no_odds": 0.42}"#,
        ),
        (
            "bet_placed_user_2",
            r#"{"market_id": "pm_agi_2026", "event": "bet_placed", "trader": "@skeptic_sam", "side": "no", "shares_bought": 30000, "total_cost_usdc": 12600.0, "post_bet_yes_odds": 0.53, "post_bet_no_odds": 0.47}"#,
        ),
        (
            "market_create_superbowl",
            r#"{"market_id": "pm_superbowl_2026", "market_title": "Will Chiefs win Super Bowl LX?", "creator": "0x4421...aa89", "initial_yes_odds": 0.45, "initial_no_odds": 0.55, "liquidity_pool_usdc": 250000.0, "status": "open"}"#,
        ),
        (
            "bet_placed_user_3",
            r#"{"market_id": "pm_superbowl_2026", "event": "bet_placed", "trader": "@chiefs_fanatic", "side": "yes", "shares_bought": 100000, "total_cost_usdc": 45000.0, "post_bet_yes_odds": 0.52, "post_bet_no_odds": 0.48}"#,
        ),
        (
            "market_create_fed",
            r#"{"market_id": "pm_fed_rates_sept", "market_title": "Will US FED cut interest rates by 50bps in Sept?", "creator": "0x91e2...cd56", "initial_yes_odds": 0.70, "initial_no_odds": 0.30, "liquidity_pool_usdc": 500000.0, "status": "open"}"#,
        ),
        (
            "bet_placed_user_4",
            r#"{"market_id": "pm_fed_rates_sept", "event": "bet_placed", "trader": "@macro_trader", "side": "no", "shares_bought": 200000, "total_cost_usdc": 60000.0, "post_bet_yes_odds": 0.55, "post_bet_no_odds": 0.45}"#,
        ),
        (
            "market_resolve_fed",
            r#"{"market_id": "pm_fed_rates_sept", "event": "resolved", "resolved_outcome": "no", "total_payout_distributed_usdc": 782410.50, "status": "resolved"}"#,
        ),
    ];

    for (key, json_str) in prediction_markets_data {
        // Convert JSON string to native Object datatype
        let obj_value = parse_json_to_datavalue(json_str);
        target
            .append("prediction_markets", key, obj_value)
            .expect("Failed to append prediction markets data");
    }

    println!("Injecting rich IoT Telemetry records...");

    // 3. Ingest realistic environmental/sensor IoT telemetry
    let iot_telemetry_data = vec![
        (
            "sensor_env_12",
            r#"{"device_id": "env_12_temp", "domain": "environmental", "metrics": {"temperature_c": 24.8, "humidity_pct": 58.2}, "battery_mv": 3280, "rssi_dbm": -68, "status": "normal"}"#,
        ),
        (
            "sensor_gps_04",
            r#"{"device_id": "gps_04_tracker", "domain": "mobility", "metrics": {"latitude": 37.7749, "longitude": -122.4194, "altitude_m": 12.4, "speed_kmh": 45.2}, "satellites": 9, "status": "normal"}"#,
        ),
        (
            "sensor_power_99",
            r#"{"device_id": "power_99_grid", "domain": "smart_grid", "metrics": {"voltage_v": 230.4, "current_a": 12.85, "active_power_kw": 2.96, "frequency_hz": 50.02}, "status": "overload", "warning_flags": ["current_spike"]}"#,
        ),
        (
            "substation_transformer_3",
            r#"{"device_id": "substation_3", "domain": "smart_grid", "metrics": {"oil_temperature_c": 72.1, "load_pct": 84.5, "cooling_fans_on": true}, "status": "normal"}"#,
        ),
        (
            "bio_wearable_ring_128",
            r#"{"device_id": "ring_128_wearable", "domain": "health", "metrics": {"heart_rate_bpm": 62, "respiration_rate": 14.5, "hrv_ms": 78.0, "blood_oxygen_pct": 98.5}, "status": "normal"}"#,
        ),
        (
            "amazon_prime_drone_10",
            r#"{"device_id": "drone_prime_10", "domain": "autonomous_delivery", "metrics": {"latitude": 47.6062, "longitude": -122.3321, "altitude_m": 125.0, "airspeed_knots": 28.5, "battery_current_a": 42.1}, "payload_secured": true, "status": "en_route"}"#,
        ),
    ];

    for (key, json_str) in iot_telemetry_data {
        // Convert JSON string to native Object datatype
        let obj_value = parse_json_to_datavalue(json_str);
        target
            .append("iot_telemetry", key, obj_value)
            .expect("Failed to append IoT telemetry");
    }

    println!("Injecting rich Football Livescore Matches...");

    // 4. Ingest realistic Football Live Scores and match events
    let football_livescore_data = vec![
        (
            "match_el_clasico_kickoff",
            r#"{"match_id": "real_barca_2026", "home_team": "Real Madrid", "away_team": "Barcelona", "home_score": 0, "away_score": 0, "minute": 1, "event": "kickoff", "status": "live"}"#,
        ),
        (
            "match_el_clasico_goal_1",
            r#"{"match_id": "real_barca_2026", "home_team": "Real Madrid", "away_team": "Barcelona", "home_score": 1, "away_score": 0, "minute": 12, "event": "goal", "goal_scorer": "Vinicius Jr.", "assist": "Bellingham", "status": "live"}"#,
        ),
        (
            "match_el_clasico_yellow_1",
            r#"{"match_id": "real_barca_2026", "home_team": "Real Madrid", "away_team": "Barcelona", "home_score": 1, "away_score": 0, "minute": 45, "event": "yellow_card", "player": "Gavi", "reason": "tactical_foul", "status": "live"}"#,
        ),
        (
            "match_el_clasico_goal_2",
            r#"{"match_id": "real_barca_2026", "home_team": "Real Madrid", "away_team": "Barcelona", "home_score": 1, "away_score": 1, "minute": 62, "event": "goal", "goal_scorer": "Lewandowski", "penalty": true, "status": "live"}"#,
        ),
        (
            "match_el_clasico_goal_3",
            r#"{"match_id": "real_barca_2026", "home_team": "Real Madrid", "away_team": "Barcelona", "home_score": 2, "away_score": 1, "minute": 74, "event": "goal", "goal_scorer": "Mbappe", "assist": "Valverde", "status": "live"}"#,
        ),
        (
            "match_el_clasico_fulltime",
            r#"{"match_id": "real_barca_2026", "home_team": "Real Madrid", "away_team": "Barcelona", "home_score": 2, "away_score": 1, "minute": 90, "event": "fulltime", "possession_pct": {"real_madrid": 48, "barcelona": 52}, "shots_on_target": {"real_madrid": 7, "barcelona": 4}, "status": "finished"}"#,
        ),
        (
            "match_derby_mancunian_goal_1",
            r#"{"match_id": "city_united_2026", "home_team": "Man City", "away_team": "Man United", "home_score": 0, "away_score": 1, "minute": 4, "event": "goal", "goal_scorer": "Rashford", "status": "live"}"#,
        ),
        (
            "match_derby_mancunian_red_card",
            r#"{"match_id": "city_united_2026", "home_team": "Man City", "away_team": "Man United", "home_score": 0, "away_score": 1, "minute": 70, "event": "red_card", "player": "Rodri", "reason": "violent_conduct", "status": "live"}"#,
        ),
    ];

    for (key, json_str) in football_livescore_data {
        // Convert JSON string to native Object datatype
        let obj_value = parse_json_to_datavalue(json_str);
        target
            .append("football_livescore", key, obj_value)
            .expect("Failed to append football livescore");
    }

    // Flush any buffered HTTP batches
    target
        .flush()
        .expect("Failed to flush HTTP batch data to active server");

    println!("Injecting rich relationship data covering enrich, correlate, chain, and sequence...");

    // =======================================================================
    // RELATIONSHIP DATA — 60+ records per stream, designed to match
    // the sample queries in ui/src/constants/samples.ts
    //
    // CORRELATE:
    //   from("transactions") | correlate("logins", "user_id", within: 60000)
    //   → Both streams share user_id with close timestamps
    //
    // SEQUENCE:
    //   from("telemetry") | sequence(event == "cpu_spike", then: event == "memory_leak", within: 30000)
    //   from("auth") | sequence(action == "login_fail", then: action == "login_fail", then: action == "login_fail", within: 60000)
    //
    // CHAIN:
    //   from("prompts") | chain("responses", "prompt_id")
    //   from("prompts") | chain("responses", "prompt_id") | chain("memory", "response_id")
    //   from("orders") | chain("shipments", "order_id") | chain("deliveries", "shipment_id")
    //
    // ENRICH:
    //   from("prompts") | enrich("users", "user_id")
    //
    // Key design principle for chain:
    //   chain(target_stream, join_key) does engine.get(target_stream, join_key_value)
    //   So the target record's KEY must equal the join_key_value from the source
    // =======================================================================

    // ── 5. Users (enrich source) ────────────────────────────────────────────
    let users: Vec<(String, String)> = (1..=20)
        .map(|i| {
            let id = format!("user_{:03}", i);
            let user = format!(
                r#"{{"user_id": "{id}", "full_name": "User {i}", "email": "user{i}@example.com", "tier": "{}", "country": "{}", "signup_ts": {}}}"#,
                ["free", "silver", "gold", "platinum"][i % 4],
                ["US", "UK", "CA", "DE", "JP", "AU"][i % 6],
                1000 + i * 100
            );
            (id, user)
        })
        .collect();
    for (key, json) in &users {
        target
            .append("users", key, parse_json_to_datavalue(json))
            .expect("Failed to append users data");
    }

    // ── 6. Transactions (correlate partner with logins) ─────────────────────
    // Each transaction has user_id, amount, timestamp, merchant
    let merchants = [
        "Amazon",
        "Netflix",
        "Apple",
        "Uber",
        "Delta",
        "Walmart",
        "Target",
        "Starbucks",
        "Best Buy",
        "Spotify",
    ];
    let mut transactions: Vec<(String, String)> = Vec::with_capacity(65);
    for i in 1..=65 {
        let user_idx = ((i * 7) % 20) + 1;
        let user_id = format!("user_{:03}", user_idx);
        let merchant = merchants[i % merchants.len()];
        let amount = ((i as f64 * 37.7) % 500.0 + 10.0) * 100.0; // cents
        let amount_f = amount.round() / 100.0;
        let ts = 1000 + i * 150; // millisecond timestamps, spread out
        let status = if i % 7 == 0 {
            "pending"
        } else if i % 13 == 0 {
            "failed"
        } else {
            "completed"
        };
        let key = format!("tx_{:03}", i);
        let json = format!(
            r#"{{"transaction_id": "{key}", "user_id": "{user_id}", "amount": {amount_f}, "currency": "USD", "merchant": "{merchant}", "status": "{status}", "timestamp": {ts}}}"#
        );
        transactions.push((key, json));
    }
    for (key, json) in &transactions {
        target
            .append("transactions", key, parse_json_to_datavalue(json))
            .expect("Failed to append transactions data");
    }

    // ── 7. Logins (correlate source with transactions) ──────────────────────
    // Logins share user_ids with transactions and have close timestamps (±30000ms)
    let devices = [
        "iPhone 15",
        "MacBook Pro",
        "Pixel 9",
        "Windows PC",
        "iPad Air",
        "Linux Workstation",
    ];
    let ips = [
        "192.168.1.10",
        "192.168.1.20",
        "10.0.0.1",
        "172.16.0.1",
        "192.168.2.50",
    ];
    let mut logins: Vec<(String, String)> = Vec::with_capacity(65);
    for i in 1..=65 {
        let user_idx = ((i * 7) % 20) + 1;
        let user_id = format!("user_{:03}", user_idx);
        let device = devices[i % devices.len()];
        let ip = ips[(i * 3) % ips.len()];
        // Login timestamp within ±25000ms of the matching transaction
        let base_ts = 1000i64 + i as i64 * 150;
        let offset: i64 = if i % 3 == 0 {
            -500
        } else if i % 3 == 1 {
            200
        } else {
            800
        };
        let ts = base_ts + offset;
        let status = if i % 11 == 0 { "failed" } else { "success" };
        let key = format!("login_{:03}", i);
        let json = format!(
            r#"{{"login_id": "{key}", "user_id": "{user_id}", "timestamp": {ts}, "ip": "{ip}", "device": "{device}", "status": "{status}"}}"#
        );
        logins.push((key, json));
    }
    for (key, json) in &logins {
        target
            .append("logins", key, parse_json_to_datavalue(json))
            .expect("Failed to append logins data");
    }

    // ── 8. Telemetry (sequence: cpu_spike → memory_leak) ────────────────────
    let mut telemetry: Vec<(String, String)> = Vec::with_capacity(66);
    for batch in 0..11 {
        // Each batch: normal events, then cpu_spike, then memory_leak
        let base_ts = 100000 + batch * 40000;
        let device = format!("server_{:02}", batch + 1);
        telemetry.push((
            format!("tel_{}_norm1", batch),
            format!(r#"{{"device_id": "{device}", "event": "heartbeat", "cpu_pct": 23.0, "mem_pct": 45.0, "timestamp": {}}}"#, base_ts),
        ));
        telemetry.push((
            format!("tel_{}_norm2", batch),
            format!(r#"{{"device_id": "{device}", "event": "heartbeat", "cpu_pct": 28.0, "mem_pct": 48.0, "timestamp": {}}}"#, base_ts + 5000),
        ));
        telemetry.push((
            format!("tel_{}_spike", batch),
            format!(r#"{{"device_id": "{device}", "event": "cpu_spike", "cpu_pct": 97.0, "mem_pct": 72.0, "timestamp": {}}}"#, base_ts + 10000),
        ));
        telemetry.push((
            format!("tel_{}_norm3", batch),
            format!(r#"{{"device_id": "{device}", "event": "heartbeat", "cpu_pct": 88.0, "mem_pct": 76.0, "timestamp": {}}}"#, base_ts + 15000),
        ));
        telemetry.push((
            format!("tel_{}_leak", batch),
            format!(r#"{{"device_id": "{device}", "event": "memory_leak", "cpu_pct": 65.0, "mem_pct": 94.0, "timestamp": {}}}"#, base_ts + 20000),
        ));
        telemetry.push((
            format!("tel_{}_oom", batch),
            format!(r#"{{"device_id": "{device}", "event": "oom_kill", "cpu_pct": 12.0, "mem_pct": 99.0, "timestamp": {}}}"#, base_ts + 25000),
        ));
    }
    for (key, json) in &telemetry {
        target
            .append("telemetry", key, parse_json_to_datavalue(json))
            .expect("Failed to append telemetry data");
    }

    // ── 9. Auth events (sequence: 3x login_fail) ────────────────────────────
    let mut auth_events: Vec<(String, String)> = Vec::with_capacity(66);
    for batch in 0..10 {
        let base_ts = 200000 + batch * 70000;
        let user_id = format!("user_{:03}", (batch % 10) + 1);
        // 3 consecutive failures, then a success
        auth_events.push((
            format!("auth_{}_fail1", batch),
            format!(r#"{{"user_id": "{user_id}", "action": "login_fail", "reason": "wrong_password", "ip": "10.0.0.{}", "timestamp": {}}}"#, batch + 10, base_ts),
        ));
        auth_events.push((
            format!("auth_{}_fail2", batch),
            format!(r#"{{"user_id": "{user_id}", "action": "login_fail", "reason": "wrong_password", "ip": "10.0.0.{}", "timestamp": {}}}"#, batch + 10, base_ts + 5000),
        ));
        auth_events.push((
            format!("auth_{}_fail3", batch),
            format!(r#"{{"user_id": "{user_id}", "action": "login_fail", "reason": "invalid_token", "ip": "10.0.0.{}", "timestamp": {}}}"#, batch + 10, base_ts + 10000),
        ));
        auth_events.push((
            format!("auth_{}_success", batch),
            format!(r#"{{"user_id": "{user_id}", "action": "login_success", "ip": "10.0.0.{}", "timestamp": {}}}"#, batch + 10, base_ts + 15000),
        ));
        // Extra random events
        for j in 0..3 {
            auth_events.push((
                format!("auth_{}_extra_{}", batch, j),
                format!(r#"{{"user_id": "user_{:03}", "action": "login_success", "ip": "192.168.1.{}", "timestamp": {}}}"#,
                    ((batch + j * 3) % 20) + 1,
                    20 + j * 10,
                    base_ts + 20000 + j * 3000
                ),
            ));
        }
    }
    for (key, json) in &auth_events {
        target
            .append("auth", key, parse_json_to_datavalue(json))
            .expect("Failed to append auth events");
    }

    // ── 10. Prompts (chain root for prompts→responses→memory) ──────────────
    let mut prompts: Vec<(String, String)> = Vec::with_capacity(65);
    for i in 1..=65 {
        let key = format!("prompt_{:03}", i);
        let prompt_id = &key;
        let user_idx = ((i * 3) % 20) + 1;
        let user_id = format!("user_{:03}", user_idx);
        let text = match i % 5 {
            0 => format!("Explain quantum computing in simple terms"),
            1 => format!("Write a Rust function to parse CSV"),
            2 => format!("Summarize the latest AI research papers"),
            3 => format!("Debug this code: fn broken(x) {{ x + }}"),
            _ => format!("Generate a SQL query for user analytics"),
        };
        let json = format!(
            r#"{{"prompt_id": "{prompt_id}", "user_id": "{user_id}", "text": "{text}", "tokens": {}, "timestamp": {}}}"#,
            50 + (i * 13) % 400,
            300000 + i * 200
        );
        prompts.push((key, json));
    }
    for (key, json) in &prompts {
        target
            .append("prompts", key, parse_json_to_datavalue(json))
            .expect("Failed to append prompts data");
    }

    // ── 11. Responses (chain hop 1: prompts → responses via prompt_id) ─────
    // Key = prompt_id value so chain("responses", "prompt_id") resolves
    let mut responses: Vec<(String, String)> = Vec::with_capacity(65);
    for i in 1..=65 {
        let key = format!("prompt_{:03}", i); // key = prompt_id
        let response_id = format!("response_{:03}", i);
        let model = ["gpt-4o", "claude-3.5-sonnet", "gemini-2.0-pro"][i % 3];
        let json = format!(
            r#"{{"response_id": "{response_id}", "prompt_id": "{key}", "model": "{model}", "text": "Response to prompt {i}", "tokens": {}, "latency_ms": {}, "timestamp": {}}}"#,
            100 + (i * 17) % 900,
            200 + (i * 11) % 3000,
            300000 + i * 200 + 1000
        );
        responses.push((key, json));
    }
    for (key, json) in &responses {
        target
            .append("responses", key, parse_json_to_datavalue(json))
            .expect("Failed to append responses data");
    }

    // ── 12. Memory (chain hop 2: prompts→responses→memory via response_id) ─
    // Key = response_id value so chain("memory", "response_id") resolves
    let mut memories: Vec<(String, String)> = Vec::with_capacity(65);
    for i in 1..=65 {
        let key = format!("response_{:03}", i); // key = response_id
        let memory_id = format!("memory_{:03}", i);
        let json = format!(
            r#"{{"memory_id": "{memory_id}", "response_id": "{key}", "summary": "Embedding summary for prompt {i}", "vector_dim": 768, "ttl_days": 30, "created_at": {}}}"#,
            300000 + i * 200 + 2000
        );
        memories.push((key, json));
    }
    for (key, json) in &memories {
        target
            .append("memory", key, parse_json_to_datavalue(json))
            .expect("Failed to append memory data");
    }

    // ── 13. Orders (chain root for orders→shipments→deliveries) ────────────
    let mut orders: Vec<(String, String)> = Vec::with_capacity(65);
    for i in 1..=65 {
        let key = format!("order_{:03}", i);
        let user_idx = ((i * 7) % 20) + 1;
        let user_id = format!("user_{:03}", user_idx);
        let items = (i % 5) + 1;
        let amount_f = (i as f64 * 29.99) % 500.0 + 15.0;
        let ts = 400000 + i * 300;
        let json = format!(
            r#"{{"order_id": "{key}", "user_id": "{user_id}", "total": {amount_f:.2}, "currency": "USD", "items": {items}, "timestamp": {ts}}}"#
        );
        orders.push((key, json));
    }
    for (key, json) in &orders {
        target
            .append("orders", key, parse_json_to_datavalue(json))
            .expect("Failed to append orders data");
    }

    // ── 14. Shipments (chain hop 1: orders → shipments via order_id) ───────
    // Key = order_id value so chain("shipments", "order_id") resolves
    let carriers = ["FedEx", "UPS", "USPS", "DHL", "Amazon Logistics"];
    let mut shipments: Vec<(String, String)> = Vec::with_capacity(65);
    for i in 1..=65 {
        let key = format!("order_{:03}", i); // key = order_id
        let shipment_id = format!("ship_{:03}", i);
        let carrier = carriers[i % carriers.len()];
        let tracking = format!(
            "{}-{:06}-{}",
            carrier.to_uppercase(),
            i * 12345 % 999999,
            if i % 2 == 0 { "USA" } else { "INTL" }
        );
        let status = if i <= 50 { "delivered" } else { "in_transit" };
        let est_days = (i % 7) + 2;
        let ts = 400000 + i * 300 + 5000;
        let json = format!(
            r#"{{"shipment_id": "{shipment_id}", "order_id": "{key}", "carrier": "{carrier}", "tracking": "{tracking}", "status": "{status}", "estimated_days": {est_days}, "timestamp": {ts}}}"#
        );
        shipments.push((key, json));
    }
    for (key, json) in &shipments {
        target
            .append("shipments", key, parse_json_to_datavalue(json))
            .expect("Failed to append shipments data");
    }

    // ── 15. Deliveries (chain hop 2: orders→shipments→deliveries via shipment_id) ─
    // Key = shipment_id value so chain("deliveries", "shipment_id") resolves
    let mut deliveries: Vec<(String, String)> = Vec::with_capacity(65);
    for i in 1..=65 {
        let key = format!("ship_{:03}", i); // key = shipment_id
        let delivery_id = format!("del_{:03}", i);
        let recipient = format!("Customer {}", i);
        let ts = 400000 + i * 300 + 15000;
        let json = format!(
            r#"{{"delivery_id": "{delivery_id}", "shipment_id": "{key}", "signed_by": "{recipient}", "status": "delivered", "timestamp": {ts}}}"#
        );
        deliveries.push((key, json));
    }
    for (key, json) in &deliveries {
        target
            .append("deliveries", key, parse_json_to_datavalue(json))
            .expect("Failed to append deliveries data");
    }

    // Flush any buffered HTTP batches
    target
        .flush()
        .expect("Failed to flush HTTP batch data to active server");

    println!("Defragmenting and compacting sample segments to establish a premium clean state...");
    target
        .compact()
        .expect("Failed to compact generated sample database segments");

    println!("\n🔍 Relationship data summary:");
    println!(
        "   users ........... {:>3} records  (enrich source)",
        users.len()
    );
    println!(
        "   transactions ... {:>3} records  (correlate partner with logins)",
        transactions.len()
    );
    println!(
        "   logins ......... {:>3} records  (correlate source)",
        logins.len()
    );
    println!(
        "   telemetry ...... {:>3} records  (sequence: cpu_spike → memory_leak)",
        telemetry.len()
    );
    println!(
        "   auth ........... {:>3} records  (sequence: 3x login_fail)",
        auth_events.len()
    );
    println!(
        "   prompts ........ {:>3} records  (chain root → responses → memory)",
        prompts.len()
    );
    println!(
        "   responses ...... {:>3} records  (chain hop 1)",
        responses.len()
    );
    println!(
        "   memory ......... {:>3} records  (chain hop 2)",
        memories.len()
    );
    println!(
        "   orders ......... {:>3} records  (chain root → shipments → deliveries)",
        orders.len()
    );
    println!(
        "   shipments ...... {:>3} records  (chain hop 1)",
        shipments.len()
    );
    println!(
        "   deliveries ..... {:>3} records  (chain hop 2)",
        deliveries.len()
    );

    println!("✨ Test sample data generation completed successfully! Premium datasets online.");

    // Close target to flush all remaining segments to disk
    drop(target);
    println!(
        "✨ Generated sample database segments successfully persisted at '{}'.",
        target_dir
    );
}
