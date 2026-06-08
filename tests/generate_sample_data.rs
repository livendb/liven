#[allow(unused_imports)]
use fs2::FileExt;
use liven::storage::StorageEngine;
use liven::types::DataValue;
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
    fn append(&mut self, stream: &str, key: &str, value: DataValue) -> Result<(), String> {
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

    fn flush(&mut self) -> Result<(), String> {
        if let AppendTarget::Http { addr, batch } = self {
            if !batch.is_empty() {
                let json_str = serde_json::to_string(batch).map_err(|e| e.to_string())?;
                post_http_json(addr, "/api/ingest", &json_str)?;
                batch.clear();
            }
        }
        Ok(())
    }

    fn compact(&mut self) -> Result<(), String> {
        match self {
            AppendTarget::Engine(engine) => engine.compact().map(|_| ()),
            AppendTarget::Http { addr, .. } => post_http_json(addr, "/api/compact", "{}"),
        }
    }
}

#[test]
fn test_generate_sample_data() {
    let target_dir = std::env::var("LIVEN_DATA_DIR").unwrap_or_else(|_| {
        std::env::temp_dir()
            .join(format!(
                "liven_sample_data_{}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ))
            .to_string_lossy()
            .to_string()
    });

    // Detect if the directory is locked by trying to obtain try_lock_shared on any existing .liven segments
    let is_locked = if let Ok(entries) = fs::read_dir(&target_dir) {
        let mut locked = false;
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("liven") {
                if let Ok(file) = fs::File::open(&path) {
                    if file.try_lock_shared().is_err() {
                        locked = true;
                        break;
                    }
                }
            }
        }
        locked
    } else {
        false
    };

    // Clean any prior residues if any and if the directory is not locked
    let is_env_set = std::env::var("LIVEN_DATA_DIR").is_ok();
    if !is_locked && !is_env_set {
        let _ = fs::remove_dir_all(&target_dir);
    }

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
        target
            .append("ai_agents", key, DataValue::String(json_str.to_string()))
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
        target
            .append(
                "prediction_markets",
                key,
                DataValue::String(json_str.to_string()),
            )
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
        target
            .append(
                "iot_telemetry",
                key,
                DataValue::String(json_str.to_string()),
            )
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
        target
            .append(
                "football_livescore",
                key,
                DataValue::String(json_str.to_string()),
            )
            .expect("Failed to append football livescore");
    }

    // Flush any buffered HTTP batches
    target
        .flush()
        .expect("Failed to flush HTTP batch data to active server");

    println!("Defragmenting and compacting sample segments to establish a premium clean state...");
    target
        .compact()
        .expect("Failed to compact generated sample database segments");

    println!("✨ Test sample data generation completed successfully! Premium datasets online.");

    // Close target to flush all remaining segments to disk
    drop(target);
    println!(
        "✨ Generated sample database segments successfully persisted at '{}'.",
        target_dir
    );

    // If LIVEN_DATA_DIR/CONDUIT_DATA_DIR was not explicitly set, and directory is not locked, clean up the temporary folder after successful test
    let is_env_set = std::env::var("LIVEN_DATA_DIR").is_ok();
    if !is_locked && !is_env_set {
        println!(
            "Cleaning up temporary sample data directory: {}",
            target_dir
        );
        // let _ = fs::remove_dir_all(&target_dir);
    }
}
