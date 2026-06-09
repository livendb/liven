#[cfg(feature = "server")]
use liven::server;
use liven::{storage, types::DataValue};
use std::io::Write;
use std::path::Path;
use std::process::{Command, exit};
#[cfg(feature = "server")]
use std::sync::Arc;
use std::{env, fs};
#[cfg(feature = "server")]
use tracing::info;
#[cfg(feature = "server")]
use tracing_subscriber::EnvFilter;

fn get_data_dir() -> String {
    if let Ok(config) = liven::config::AppConfig::load() {
        config.storage.data_directory
    } else {
        "./data".to_string()
    }
}

fn get_pid_file() -> String {
    format!("{}/liven.pid", get_data_dir())
}

fn print_usage() {
    print!("\x1b[36m");
    println!(
        r#"
  _      _____     _______ _   _
 | |    |_ _\ \   / / ____| \ | |
 | |     | | \ \ / /|  _| |  \| |
 | |___  | |  \ V / | |___| |\  |
 |_____|___|  \_/  |_____|_| \_|
        "#
    );
    println!("\x1b[0m");
    println!("\x1b[36m LIVEN CLI\x1b[0m");
    println!("Usage:");
    println!("  liven start [--no-ui]   Start the database server");
    println!("  liven stop              Stop the running database server");
    println!("  liven status            Check the status of the database server");
    println!("  liven list [--auth-key <value>] List all available database streams");
    println!("  liven vibe [--auth-key <value>] Start interactive TUI query shell");
    println!("  liven tail <stream>     Tail real-time records from a stream");
    println!("                         Options: [--format <text|json>] [--auth-key <value>]");
    println!("  liven import            Import CSV or JSONL records into a stream");
    println!(
        "                         Options: --stream <name> --format <csv|jsonl> --path <path> [--auth-key <value>]"
    );
    println!("  liven export            Export stream records as CSV or JSONL");
    println!(
        "                         Options: [--stream <name>] --format <csv|jsonl> [--path <path>] [--auth-key <value>]"
    );
    println!(
        "  liven reset-key         Reset and remove all administrative credentials, generating a new root key"
    );
    println!();
    println!("Options:");
    println!("  --no-ui                Disable starting the Web UI");
}

struct PidCleanup;

impl Drop for PidCleanup {
    fn drop(&mut self) {
        let _ = fs::remove_file(get_pid_file());
    }
}

fn read_pid() -> Result<u32, String> {
    let content = fs::read_to_string(get_pid_file()).map_err(|e| e.to_string())?;
    content.trim().parse::<u32>().map_err(|e| e.to_string())
}

pub fn is_server_running() -> bool {
    if let Ok(pid) = read_pid() {
        // Run kill -0 <pid> to see if the process exists and is active
        let output = Command::new("kill").arg("-0").arg(pid.to_string()).output();

        if let Ok(out) = output {
            return out.status.success();
        }
    }
    false
}

pub async fn run_cli() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        print_usage();
        exit(1);
    }

    match args[1].as_str() {
        #[cfg(feature = "server")]
        "reset-key" => {
            // Check if server is running
            let running_pid = if is_server_running() {
                read_pid().ok()
            } else {
                None
            };

            if let Some(pid) = running_pid {
                println!(
                    "LIVEN is currently running (PID {}). Stopping it first...",
                    pid
                );

                // Send SIGTERM (termination signal)
                let _ = Command::new("kill")
                    .arg("-15")
                    .arg(pid.to_string())
                    .output();

                // Wait up to 10 seconds for the server to gracefully terminate
                let mut attempts = 0;
                while is_server_running() && attempts < 20 {
                    std::thread::sleep(std::time::Duration::from_millis(500));
                    attempts += 1;
                }

                // If still running, force-terminate (SIGKILL)
                if is_server_running() {
                    println!("LIVEN did not stop gracefully. Attempting force-kill (kill -9)...");
                    let _ = Command::new("kill").arg("-9").arg(pid.to_string()).output();

                    let mut force_attempts = 0;
                    while is_server_running() && force_attempts < 10 {
                        std::thread::sleep(std::time::Duration::from_millis(200));
                        force_attempts += 1;
                    }
                }

                // Clean up pid file
                let _ = fs::remove_file(get_pid_file());
                println!("\x1b[32m✔ LIVEN stopped.\x1b[0m");
            }

            let max_segment_size = match liven::config::AppConfig::load() {
                Ok(cfg) => cfg.storage.max_segment_size_mb * 1024 * 1024,
                Err(_) => 10 * 1024 * 1024,
            };

            let raw_key = {
                // Open direct engine (scoped block ensures lock is released before restart)
                let engine = storage::StorageEngine::new(get_data_dir(), max_segment_size as u64)?;

                // 1. Remove all keys by listing keys and appending tombstones
                let keys = engine.list_keys("auth_keys");
                for key_id in keys {
                    let _ = engine.append("auth_keys", &key_id, DataValue::Null, true);
                }

                // 2. Generate a fresh default root administrative key
                let mut raw_key_bytes = [0u8; 32];
                rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut raw_key_bytes);
                let generated_key = liven::security::hex_encode(&raw_key_bytes);

                let hash = blake3::hash(generated_key.as_bytes());
                let hash_hex = liven::security::hex_encode(hash.as_bytes());

                let auth_rec = server::AuthKeyRecord {
                    key_id: "default-admin".to_string(),
                    role: "admin".to_string(),
                    auth_key: hash_hex,
                    status: "active".to_string(),
                    allowed_tags: Vec::new(),
                };

                let json_val = serde_json::to_string(&auth_rec)?;
                engine.append(
                    "auth_keys",
                    "default-admin",
                    DataValue::String(json_val),
                    false,
                )?;
                generated_key
            };

            println!("\x1b[1;31m");
            println!("########################################################################");
            println!("#                      LIVENDB ROOT KEY RESET SUCCESS                  #");
            println!("########################################################################");
            println!("\x1b[0m");
            println!("  A FRESH DEFAULT ROOT ADMINISTRATIVE AUTH KEY HAS BEEN GENERATED:");
            println!();
            println!("    \x1b[1;33m{}\x1b[0m", raw_key);
            println!();
            println!("  Please save this key securely! It will never be shown again.");
            println!("\x1b[1;31m");
            println!("########################################################################");
            println!("\x1b[0m");

            if running_pid.is_some() {
                println!("Restarting LIVEN server in background...");
                let current_exe = env::current_exe()?;
                let mut cmd = Command::new(current_exe);
                cmd.arg("start");
                cmd.stdout(std::process::Stdio::null());
                cmd.stderr(std::process::Stdio::null());
                cmd.spawn()?;

                // Give it a brief moment to boot and write its PID
                std::thread::sleep(std::time::Duration::from_millis(1500));
                if is_server_running() {
                    if let Ok(new_pid) = read_pid() {
                        println!(
                            "\x1b[32m✔ LIVEN has been restarted successfully in the background (PID {}).\x1b[0m",
                            new_pid
                        );
                    } else {
                        println!(
                            "\x1b[32m✔ LIVEN has been restarted successfully in the background.\x1b[0m"
                        );
                    }
                } else {
                    println!(
                        "\x1b[33m⚠ LIVEN was spawned but hasn't started yet. Please run `liven start` manually if it failed.\x1b[0m"
                    );
                }
            }
        }
        #[cfg(not(feature = "server"))]
        "reset-key" => {
            println!(
                "This command requires the 'server' feature.\nRecompile with: cargo build --features server"
            );
            exit(1);
        }
        #[cfg(feature = "server")]
        "start" => {
            // Check if already running
            if is_server_running()
                && let Ok(pid) = read_pid()
            {
                println!(
                    "\x1b[31mError: LIVEN is already running with PID {}\x1b[0m",
                    pid
                );
                exit(1);
            }

            let run_ui = !args.contains(&"--no-ui".to_string());

            // Initialize tracing subscriber
            tracing_subscriber::fmt()
                .with_env_filter(
                    EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
                )
                .init();

            info!("⚡ Starting LIVEN Server...");

            let config = liven::config::AppConfig::load()
                .map_err(|e| format!("Failed to load config: {}", e))?;

            // Create data directory if it doesn't exist
            let data_directory = &config.storage.data_directory;
            fs::create_dir_all(data_directory)?;

            // Save PID of the current process
            let current_pid = std::process::id();
            fs::write(get_pid_file(), current_pid.to_string())?;
            info!(
                "Registered process PID {} under {}",
                current_pid,
                get_pid_file()
            );

            // Setup a clean deletion of the PID file on exit
            let _pid_cleanup = PidCleanup;

            // Initialize StorageEngine with threshold from config
            let max_segment_size = config.storage.max_segment_size_mb * 1024 * 1024;
            let engine = Arc::new(storage::StorageEngine::new(
                data_directory,
                max_segment_size as u64,
            )?);

            // Start embedded Axum web servers
            server::run_server(engine, config, run_ui).await?;
        }
        #[cfg(not(feature = "server"))]
        "start" => {
            println!(
                "This command requires the 'server' feature.\nRecompile with: cargo build --features server"
            );
            exit(1);
        }
        "stop" => {
            if !Path::new(&get_pid_file()).exists() {
                println!(
                    "\x1b[33mLIVEN is not running (no PID file found at {}).\x1b[0m",
                    get_pid_file()
                );
                exit(0);
            }

            let pid_str = fs::read_to_string(get_pid_file())?;
            let pid = pid_str
                .trim()
                .parse::<u32>()
                .map_err(|e| format!("Invalid PID file: {}", e))?;

            println!("Sending termination signal to LIVEN (PID {})...", pid);

            // Use 'kill' command to terminate the process
            let output = Command::new("kill")
                .arg("-15") // SIGTERM
                .arg(pid.to_string())
                .output();

            match output {
                Ok(out) if out.status.success() => {
                    println!(
                        "\x1b[32m✔ LIVEN (PID {}) has been successfully terminated.\x1b[0m",
                        pid
                    );
                    let _ = fs::remove_file(get_pid_file());
                }
                _ => {
                    // Try force-kill if SIGTERM failed or returned error
                    println!("SIGTERM failed. Attempting SIGKILL (kill -9)...");
                    let force_output = Command::new("kill")
                        .arg("-9") // SIGKILL
                        .arg(pid.to_string())
                        .output();

                    if force_output.is_ok() && force_output.unwrap().status.success() {
                        println!(
                            "\x1b[32m✔ LIVEN (PID {}) has been force-terminated.\x1b[0m",
                            pid
                        );
                    } else {
                        println!(
                            "\x1b[31m❌ Failed to stop process {}. It may have already exited.\x1b[0m",
                            pid
                        );
                    }
                    let _ = fs::remove_file(get_pid_file());
                }
            }
        }
        "status" => {
            if is_server_running() {
                if let Ok(pid) = read_pid() {
                    print!("\x1b[36m");
                    println!(
                        r#"
                        _  __                 _        ____  ____
                       | |/ /___  _ __   _ __| | __ _ |  _ \| __ )
                       | ' // _ \| '_ \ / _` | |/ _` || | | |  _ \
                       | . \ (_) | | | | (_| | | (_| || |_| | |_) |
                       |_|\_\___/|_| |_|\__,_|_|\__,_||____/|____/
"#
                    );
                    println!("\x1b[0m");
                    println!("\x1b[32m● LIVEN is running (PID: {})\x1b[0m", pid);
                    let (host, db_p, web_p) = match liven::config::AppConfig::load() {
                        Ok(cfg) => (cfg.server.host, cfg.server.db_port, cfg.server.webui_port),
                        Err(_) => ("127.0.0.1".to_string(), 43121, 43120),
                    };
                    println!("  Engine Host: {}", host);
                    println!("  Database Port: {}", db_p);
                    println!("  Web UI Port: {}", web_p);
                    if let Ok(meta) = fs::metadata(get_pid_file())
                        && let Ok(modified) = meta.modified()
                        && let Ok(duration) = modified.elapsed()
                    {
                        println!("  Uptime: {}s", duration.as_secs());
                    }
                }
            } else if Path::new(&get_pid_file()).exists() {
                println!(
                    "\x1b[33m● LIVEN status: Stale PID file found (process is not active).\x1b[0m"
                );
                let _ = fs::remove_file(get_pid_file());
            } else {
                println!("\x1b[37m● LIVEN is stopped.\x1b[0m");
            }
        }
        "list" => {
            let mut auth_key: Option<String> = None;
            let mut idx = 2;
            while idx < args.len() {
                match args[idx].as_str() {
                    "--auth-key" | "-k" => {
                        if idx + 1 < args.len() {
                            auth_key = Some(args[idx + 1].clone());
                            idx += 2;
                        } else {
                            println!("\x1b[31m❌ Error: --auth-key requires a value.\x1b[0m");
                            exit(1);
                        }
                    }
                    _ => {
                        idx += 1;
                    }
                }
            }

            let config = match liven::config::AppConfig::load() {
                Ok(c) => c,
                Err(e) => {
                    println!("\x1b[31m❌ Failed to load configuration: {}\x1b[0m", e);
                    exit(1);
                }
            };
            let db_addr = format!("{}:{}", config.server.host, config.server.db_port);

            let streams = if is_server_running() {
                let mut client = if let Some(ref key) = auth_key {
                    let full_addr = format!("{}?auth_key={}", db_addr, key);
                    match liven::client::LivenClient::connect_with_id(&full_addr, "default_client")
                        .await
                    {
                        Ok(c) => c,
                        Err(e) => {
                            println!("\x1b[31m❌ Connection failed: {}\x1b[0m", e);
                            exit(1);
                        }
                    }
                } else {
                    match liven::client::LivenClient::connect(&db_addr).await {
                        Ok(c) => c,
                        Err(e) => {
                            println!("\x1b[31m❌ Connection failed: {}\x1b[0m", e);
                            exit(1);
                        }
                    }
                };
                match client.query("streams()").await {
                    Ok(records) => records
                        .into_iter()
                        .map(|r| match r.value {
                            DataValue::String(s) => s,
                            _ => r.key.to_string(),
                        })
                        .collect::<Vec<_>>(),
                    Err(e) => {
                        println!("\x1b[31m❌ Query failed: {}\x1b[0m", e);
                        exit(1);
                    }
                }
            } else {
                let max_segment_size = config.storage.max_segment_size_mb * 1024 * 1024;
                let engine = storage::StorageEngine::new(get_data_dir(), max_segment_size as u64)?;
                engine.list_streams()
            };

            if streams.is_empty() {
                println!("\x1b[33mNo streams found.\x1b[0m");
            } else {
                println!("\x1b[36mAvailable Streams:\x1b[0m");
                for stream in streams {
                    println!("  \x1b[32m- {}\x1b[0m", stream);
                }
            }
        }
        "tail" => {
            let mut stream_name: Option<String> = None;
            let mut format = "text".to_string();
            let mut auth_key: Option<String> = None;

            let mut idx = 2;
            while idx < args.len() {
                match args[idx].as_str() {
                    "--format" | "-f" => {
                        if idx + 1 < args.len() {
                            format = args[idx + 1].to_lowercase();
                            idx += 2;
                        } else {
                            println!(
                                "\x1b[31m❌ Error: --format requires 'text' or 'json'.\x1b[0m"
                            );
                            exit(1);
                        }
                    }
                    "--auth-key" | "-k" => {
                        if idx + 1 < args.len() {
                            auth_key = Some(args[idx + 1].clone());
                            idx += 2;
                        } else {
                            println!("\x1b[31m❌ Error: --auth-key requires a value.\x1b[0m");
                            exit(1);
                        }
                    }
                    val => {
                        if stream_name.is_none() && !val.starts_with('-') {
                            stream_name = Some(val.to_string());
                            idx += 1;
                        } else {
                            idx += 1;
                        }
                    }
                }
            }

            let stream_name = match stream_name {
                Some(name) => name,
                None => {
                    println!(
                        "\x1b[31m❌ Error: Missing stream name. Usage: liven tail <stream_name> [--format <text|json>] [--auth-key <value>]\x1b[0m"
                    );
                    exit(1);
                }
            };

            if format != "text" && format != "json" {
                println!(
                    "\x1b[31m❌ Error: Invalid format '{}'. Choose 'text' or 'json'.\x1b[0m",
                    format
                );
                exit(1);
            }

            let config = match liven::config::AppConfig::load() {
                Ok(c) => c,
                Err(e) => {
                    println!("\x1b[31m❌ Failed to load configuration: {}\x1b[0m", e);
                    exit(1);
                }
            };
            let db_addr = format!("{}:{}", config.server.host, config.server.db_port);
            println!(
                "⚡ Connecting to liven://{} and tailing stream '{}' in {} format...",
                db_addr, stream_name, format
            );
            let client = if let Some(ref key) = auth_key {
                let full_addr = format!("{}?auth_key={}", db_addr, key);
                match liven::client::LivenClient::connect_with_id(&full_addr, "default_client")
                    .await
                {
                    Ok(c) => c,
                    Err(e) => {
                        println!("\x1b[31m❌ Connection failed: {}\x1b[0m", e);
                        exit(1);
                    }
                }
            } else {
                match liven::client::LivenClient::connect(&db_addr).await {
                    Ok(c) => c,
                    Err(e) => {
                        println!("\x1b[31m❌ Connection failed: {}\x1b[0m", e);
                        exit(1);
                    }
                }
            };
            if let Err(e) = client.tail_stream(&stream_name, &format).await {
                println!("\x1b[31m❌ Stream tail error: {}\x1b[0m", e);
                exit(1);
            }
        }
        #[cfg(feature = "tui")]
        "vibe" => {
            let mut auth_key: Option<String> = None;
            let mut idx = 2;
            while idx < args.len() {
                match args[idx].as_str() {
                    "--auth-key" | "-k" => {
                        if idx + 1 < args.len() {
                            auth_key = Some(args[idx + 1].clone());
                            idx += 2;
                        } else {
                            println!("\x1b[31m❌ Error: --auth-key requires a value.\x1b[0m");
                            exit(1);
                        }
                    }
                    _ => {
                        idx += 1;
                    }
                }
            }
            crate::tui::run_shell(auth_key).await?;
        }
        #[cfg(not(feature = "tui"))]
        "vibe" => {
            println!(
                "This command requires the 'tui' feature.\nRecompile with: cargo build --features tui"
            );
            exit(1);
        }
        "import" => {
            let mut stream_name: Option<String> = None;
            let mut format: Option<String> = None;
            let mut file_path: Option<String> = None;
            let mut auth_key: Option<String> = None;

            let mut idx = 2;
            while idx < args.len() {
                match args[idx].as_str() {
                    "--stream" => {
                        if idx + 1 < args.len() {
                            stream_name = Some(args[idx + 1].clone());
                            idx += 2;
                        } else {
                            println!("\x1b[31m❌ Error: --stream requires a stream name.\x1b[0m");
                            exit(1);
                        }
                    }
                    "--format" => {
                        if idx + 1 < args.len() {
                            format = Some(args[idx + 1].to_lowercase());
                            idx += 2;
                        } else {
                            println!(
                                "\x1b[31m❌ Error: --format requires 'csv' or 'jsonl'.\x1b[0m"
                            );
                            exit(1);
                        }
                    }
                    "--path" => {
                        if idx + 1 < args.len() {
                            file_path = Some(args[idx + 1].clone());
                            idx += 2;
                        } else {
                            println!("\x1b[31m❌ Error: --path requires a file path.\x1b[0m");
                            exit(1);
                        }
                    }
                    "--auth-key" | "-k" => {
                        if idx + 1 < args.len() {
                            auth_key = Some(args[idx + 1].clone());
                            idx += 2;
                        } else {
                            println!("\x1b[31m❌ Error: --auth-key requires a value.\x1b[0m");
                            exit(1);
                        }
                    }
                    _ => {
                        idx += 1;
                    }
                }
            }

            let stream_name = stream_name.unwrap_or_else(|| {
                println!("\x1b[31m❌ Error: Missing required --stream <name> option.\x1b[0m");
                exit(1);
            });
            let format = format.unwrap_or_else(|| {
                println!("\x1b[31m❌ Error: Missing required --format <csv|jsonl> option.\x1b[0m");
                exit(1);
            });

            let file_path = file_path.unwrap_or_else(|| {
                println!("\x1b[31m❌ Error: Missing required --path <path> option.\x1b[0m");
                exit(1);
            });

            if format != "csv" && format != "jsonl" {
                println!(
                    "\x1b[31m❌ Error: Invalid format '{}'. Choose 'csv' or 'jsonl'.\x1b[0m",
                    format
                );
                exit(1);
            }

            if !Path::new(&file_path).exists() {
                println!(
                    "\x1b[31m❌ Error: File '{}' does not exist.\x1b[0m",
                    file_path
                );
                exit(1);
            }

            println!(
                "Importing from '{}' into stream '{}' using format '{}'...",
                file_path, stream_name, format
            );

            let config = match liven::config::AppConfig::load() {
                Ok(c) => c,
                Err(e) => {
                    println!("\x1b[31m❌ Failed to load configuration: {}\x1b[0m", e);
                    exit(1);
                }
            };
            let db_addr = format!("{}:{}", config.server.host, config.server.db_port);
            let mut client = if let Some(key) = auth_key {
                let full_addr = format!("{}?auth_key={}", db_addr, key);
                match liven::client::LivenClient::connect_with_id(&full_addr, "default_client")
                    .await
                {
                    Ok(c) => c,
                    Err(e) => {
                        println!("\x1b[31m❌ Connection failed: {}\x1b[0m", e);
                        exit(1);
                    }
                }
            } else {
                match liven::client::LivenClient::connect(&db_addr).await {
                    Ok(c) => c,
                    Err(e) => {
                        println!("\x1b[31m❌ Connection failed: {}\x1b[0m", e);
                        exit(1);
                    }
                }
            };

            let mut count = 0;

            if format == "jsonl" {
                let file = fs::File::open(&file_path)?;
                let reader = std::io::BufReader::new(file);
                use std::io::BufRead;
                let mut pairs: Vec<String> = vec![];

                for line_res in reader.lines() {
                    let line = line_res?;
                    if line.trim().is_empty() {
                        continue;
                    }
                    let parsed: serde_json::Value = serde_json::from_str(&line)?;

                    let (key, payload) = if let serde_json::Value::Object(map) = &parsed {
                        let k = if let Some(serde_json::Value::String(kv)) = map.get("key") {
                            kv.clone()
                        } else if let Some(serde_json::Value::String(id_v)) = map.get("id") {
                            id_v.clone()
                        } else if let Some(id_num) = map.get("id").and_then(|v| v.as_i64()) {
                            id_num.to_string()
                        } else {
                            format!("import_{}", count + 1)
                        };

                        let v = if map.contains_key("value") {
                            map.get("value").unwrap().clone()
                        } else {
                            parsed.clone()
                        };
                        (k, v)
                    } else {
                        (format!("import_{}", count + 1), parsed.clone())
                    };

                    pairs.push(format!("[\"{}\", {}]", key, payload));
                    count += 1;
                }
                let batch_str = pairs.join(",");
                let query_str = format!("from(\"{}\").insert([{}])", stream_name, batch_str);
                match client.query(&query_str).await {
                    Ok(_) => {
                        println!(
                            "\x1b[32m✔ Successfully imported {} records into stream '{}'.\x1b[0m",
                            count, stream_name
                        );
                    }
                    Err(e) => {
                        println!("\x1b[31m❌ Query failed: {}\x1b[0m", e);
                        exit(1);
                    }
                }
            } else {
                let mut rdr = csv::Reader::from_reader(fs::File::open(&file_path)?);
                let headers = rdr.headers()?.clone();
                let mut pairs: Vec<String> = vec![];

                for result in rdr.records() {
                    let record = result?;

                    let mut key_idx = None;
                    for (idx, h) in headers.iter().enumerate() {
                        if h == "key" || h == "id" {
                            key_idx = Some(idx);
                            break;
                        }
                    }

                    let key = if let Some(idx) = key_idx {
                        record.get(idx).unwrap_or("").to_string()
                    } else {
                        format!("import_{}", count + 1)
                    };

                    let mut map = serde_json::Map::new();
                    for (idx, h) in headers.iter().enumerate() {
                        if Some(idx) == key_idx {
                            continue;
                        }
                        let cell_val = record.get(idx).unwrap_or("");
                        let json_val = if let Ok(n) = cell_val.parse::<i64>() {
                            serde_json::Value::Number(n.into())
                        } else if let Ok(f) = cell_val.parse::<f64>() {
                            if let Some(num) = serde_json::Number::from_f64(f) {
                                serde_json::Value::Number(num)
                            } else {
                                serde_json::Value::String(cell_val.to_string())
                            }
                        } else if let Ok(b) = cell_val.parse::<bool>() {
                            serde_json::Value::Bool(b)
                        } else if cell_val.starts_with('{') && cell_val.ends_with('}') {
                            serde_json::from_str(cell_val)
                                .unwrap_or(serde_json::Value::String(cell_val.to_string()))
                        } else {
                            serde_json::Value::String(cell_val.to_string())
                        };

                        map.insert(h.to_string(), json_val);
                    }

                    let payload = serde_json::Value::Object(map);
                    pairs.push(format!("[\"{}\", {}]", key, payload));
                    count += 1;
                }
                let batch_str = pairs.join(",");
                let query_str = format!("from(\"{}\").insert([{}])", stream_name, batch_str);
                match client.query(&query_str).await {
                    Ok(_) => {
                        println!(
                            "\x1b[32m✔ Successfully imported {} records into stream '{}'.\x1b[0m",
                            count, stream_name
                        );
                    }
                    Err(e) => {
                        println!("\x1b[31m❌ Query failed: {}\x1b[0m", e);
                        exit(1);
                    }
                }
            }
        }
        "export" => {
            let mut stream_name: Option<String> = None;
            let mut format: Option<String> = None;
            let mut file_path: Option<String> = None;
            let mut auth_key: Option<String> = None;

            let mut idx = 2;
            while idx < args.len() {
                match args[idx].as_str() {
                    "--stream" => {
                        if idx + 1 < args.len() {
                            stream_name = Some(args[idx + 1].clone());
                            idx += 2;
                        } else {
                            println!("\x1b[31m❌ Error: --stream requires a stream name.\x1b[0m");
                            exit(1);
                        }
                    }
                    "--format" => {
                        if idx + 1 < args.len() {
                            format = Some(args[idx + 1].to_lowercase());
                            idx += 2;
                        } else {
                            println!(
                                "\x1b[31m❌ Error: --format requires 'csv' or 'jsonl'.\x1b[0m"
                            );
                            exit(1);
                        }
                    }
                    "--path" => {
                        if idx + 1 < args.len() {
                            file_path = Some(args[idx + 1].clone());
                            idx += 2;
                        } else {
                            println!("\x1b[31m❌ Error: --path requires a file path.\x1b[0m");
                            exit(1);
                        }
                    }
                    "--auth-key" | "-k" => {
                        if idx + 1 < args.len() {
                            auth_key = Some(args[idx + 1].clone());
                            idx += 2;
                        } else {
                            println!("\x1b[31m❌ Error: --auth-key requires a value.\x1b[0m");
                            exit(1);
                        }
                    }
                    _ => {
                        idx += 1;
                    }
                }
            }

            let format = format.unwrap_or_else(|| {
                println!("\x1b[31m❌ Error: Missing required --format <csv|jsonl> option.\x1b[0m");
                exit(1);
            });

            if format != "csv" && format != "jsonl" {
                println!(
                    "\x1b[31m❌ Error: Invalid format '{}'. Choose 'csv' or 'jsonl'.\x1b[0m",
                    format
                );
                exit(1);
            }

            let config = match liven::config::AppConfig::load() {
                Ok(c) => c,
                Err(e) => {
                    println!("\x1b[31m❌ Failed to load configuration: {}\x1b[0m", e);
                    exit(1);
                }
            };
            let db_addr = format!("{}:{}", config.server.host, config.server.db_port);
            let mut client = if let Some(ref key) = auth_key {
                let full_addr = format!("{}?auth_key={}", db_addr, key);
                match liven::client::LivenClient::connect_with_id(&full_addr, "default_client")
                    .await
                {
                    Ok(c) => c,
                    Err(e) => {
                        println!("\x1b[31m❌ Connection failed: {}\x1b[0m", e);
                        exit(1);
                    }
                }
            } else {
                match liven::client::LivenClient::connect(&db_addr).await {
                    Ok(c) => c,
                    Err(e) => {
                        println!("\x1b[31m❌ Connection failed: {}\x1b[0m", e);
                        exit(1);
                    }
                }
            };

            let streams_to_export = if let Some(ref name) = stream_name {
                let all_streams = match client.query("streams()").await {
                    Ok(records) => records
                        .into_iter()
                        .map(|r| match r.value {
                            DataValue::String(s) => s,
                            _ => r.key.to_string(),
                        })
                        .collect::<Vec<_>>(),
                    Err(e) => {
                        println!("\x1b[31m❌ Failed to list streams: {}\x1b[0m", e);
                        exit(1);
                    }
                };
                if !all_streams.contains(name) {
                    println!("\x1b[31m❌ Error: Stream '{}' does not exist.\x1b[0m", name);
                    exit(1);
                }
                vec![name.clone()]
            } else {
                let all_streams = match client.query("streams()").await {
                    Ok(records) => records
                        .into_iter()
                        .map(|r| match r.value {
                            DataValue::String(s) => s,
                            _ => r.key.to_string(),
                        })
                        .collect::<Vec<_>>(),
                    Err(e) => {
                        println!("\x1b[31m❌ Failed to list streams: {}\x1b[0m", e);
                        exit(1);
                    }
                };
                if all_streams.is_empty() {
                    println!("\x1b[33mNo streams found to export.\x1b[0m");
                    exit(0);
                }
                all_streams
            };

            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);

            for target_stream in &streams_to_export {
                let stream_file_path = if let Some(ref path) = file_path {
                    if stream_name.is_some() {
                        path.clone()
                    } else {
                        let path_obj = Path::new(path);
                        let parent = path_obj.parent().unwrap_or_else(|| Path::new("."));
                        let file_stem = path_obj
                            .file_stem()
                            .and_then(|s| s.to_str())
                            .unwrap_or("export");
                        let extension = path_obj
                            .extension()
                            .and_then(|e| e.to_str())
                            .unwrap_or(&format);
                        parent
                            .join(format!("{}_{}.{}", file_stem, target_stream, extension))
                            .to_string_lossy()
                            .to_string()
                    }
                } else {
                    let download_dir = if let Ok(home) = env::var("HOME") {
                        format!("{}/Downloads", home)
                    } else {
                        ".".to_string()
                    };
                    let _ = fs::create_dir_all(&download_dir);
                    format!(
                        "{}/{}_{}.{}",
                        download_dir, target_stream, timestamp, format
                    )
                };

                println!(
                    "Exporting stream '{}' data to '{}' in '{}' format...",
                    target_stream, stream_file_path, format
                );

                let query_str = format!("from(\"{}\")", target_stream);
                let filtered_records = match client.query(&query_str).await {
                    Ok(recs) => recs,
                    Err(e) => {
                        println!(
                            "\x1b[31m❌ Failed to query stream '{}': {}\x1b[0m",
                            target_stream, e
                        );
                        exit(1);
                    }
                };

                if format == "jsonl" {
                    let mut file = fs::File::create(&stream_file_path)?;
                    for r in &filtered_records {
                        let val_json = match &r.value {
                            DataValue::String(s) => serde_json::from_str::<serde_json::Value>(s)
                                .unwrap_or(serde_json::Value::String(s.clone())),
                            other => serde_json::to_value(other).unwrap_or(serde_json::Value::Null),
                        };
                        let line_obj = serde_json::json!({
                            "sequence_id": r.sequence_id,
                            "timestamp": r.timestamp,
                            "key": r.key,
                            "value": val_json,
                        });
                        writeln!(file, "{}", line_obj)?;
                    }
                } else {
                    let mut writer = csv::Writer::from_writer(fs::File::create(&stream_file_path)?);
                    writer.write_record(["sequence_id", "timestamp", "key", "value"])?;
                    for r in &filtered_records {
                        let val_str = match &r.value {
                            DataValue::String(s) => s.clone(),
                            other => other.to_string(),
                        };
                        writer.write_record([
                            r.sequence_id.to_string(),
                            r.timestamp.to_string(),
                            r.key.to_string(),
                            val_str,
                        ])?;
                    }
                    writer.flush()?;
                }

                println!(
                    "\x1b[32m✔ Successfully exported {} records to {}.\x1b[0m",
                    filtered_records.len(),
                    stream_file_path
                );
            }
        }
        _ => {
            print_usage();
            exit(1);
        }
    }

    Ok(())
}
