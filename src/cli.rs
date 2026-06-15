#[cfg(feature = "server")]
use liven::server;
use liven::{storage, types::DataValue};

use std::path::Path;
use std::process::{Command, exit};
#[cfg(feature = "server")]
use std::sync::Arc;
use std::{env, fs};
use tracing::{info, warn};

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
    println!("  liven import            Import JSONL or binary records");
    println!("                         Options: --path <path> [--dry-run] [--auth-key <value>]");
    println!("  liven export            Export stream records as JSONL or binary");
    println!(
        "                         Options: [--stream <name> | --all] [--path <path>] [--auth-key <value>]"
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

    info!("Starting Liven CLI with arguments: {:?}", args);

    if args.len() < 2 {
        warn!("No command provided");
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

            info!("Starting LIVEN Server...");

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
            let mut engine = storage::StorageEngine::new(data_directory, max_segment_size as u64)?;

            // Apply configuration limits
            engine.set_max_streams(config.limits.max_concurrent_streams);
            engine.set_max_index_ram_bytes(config.limits.max_index_ram_mb as u64 * 1024 * 1024);
            engine.set_max_fds(config.limits.max_open_file_descriptors);
            engine.set_max_scan_results(config.limits.max_scan_results);

            let engine = Arc::new(engine);

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
                    println!("\x1b[32m● LIVEN is running (PID: {})\x1b[0m", pid);
                    let (host, db_p, web_p) = match liven::config::AppConfig::load() {
                        Ok(cfg) => (cfg.server.host, cfg.server.db_port, cfg.server.webui_port),
                        Err(_) => ("127.0.0.1".to_string(), 43121, 43120),
                    };
                    println!("  Engine Host: {}", host);
                    println!("  Database Port: {}", db_p);
                    println!("  Web UI Port: {}", web_p);

                    // Read and display status file if available
                    let status_file = "liven.status";
                    if let Ok(status_content) = fs::read_to_string(status_file) {
                        for line in status_content.lines() {
                            if let Some((key, value)) = line.split_once('=') {
                                match key {
                                    "connections" => println!("  Active Connections: {}", value),
                                    "subscribers" => println!("  Broadcast Subscribers: {}", value),
                                    _ => {}
                                }
                            }
                        }
                    }

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
                "Connecting to liven://{} and tailing stream '{}' in {} format...",
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
            let mut file_path: Option<String> = None;
            let mut dry_run = false;
            let mut auth_key: Option<String> = None;

            let mut idx = 2;
            while idx < args.len() {
                match args[idx].as_str() {
                    "--path" => {
                        if idx + 1 < args.len() {
                            file_path = Some(args[idx + 1].clone());
                            idx += 2;
                        } else {
                            println!("\x1b[31m❌ Error: --path requires a file path.\x1b[0m");
                            exit(1);
                        }
                    }
                    "--dry-run" => {
                        dry_run = true;
                        idx += 1;
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

            let file_path = file_path.unwrap_or_else(|| {
                println!("\x1b[31m❌ Error: Missing required --path <path> option.\x1b[0m");
                exit(1);
            });

            if !Path::new(&file_path).exists() {
                println!(
                    "\x1b[31m❌ Error: File '{}' does not exist.\x1b[0m",
                    file_path
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

            // Determine file format from extension
            let path_obj = Path::new(&file_path);
            let format = path_obj
                .extension()
                .and_then(|ext| ext.to_str())
                .map(|ext| ext.to_lowercase())
                .unwrap_or_else(|| {
                    println!("\x1b[31m❌ Error: Cannot determine format from file extension. Use .jsonl or .liven\x1b[0m");
                    exit(1);
                });

            if format != "jsonl" && format != "liven" {
                println!(
                    "\x1b[31m❌ Error: Unsupported file format '{}'. Use .jsonl or .liven\x1b[0m",
                    format
                );
                exit(1);
            }

            println!(
                "Importing from '{}' (format: {}){}",
                file_path,
                format,
                if dry_run { " — DRY RUN" } else { "" }
            );

            if format == "jsonl" {
                // Validate the file first
                let validation =
                    match liven::import_export::validate_jsonl_file(Path::new(&file_path)) {
                        Ok(report) => report,
                        Err(e) => {
                            println!("\x1b[31m❌ Validation failed: {}\x1b[0m", e);
                            exit(1);
                        }
                    };

                println!("\nValidation results:");
                println!("  Records found: {}", validation.record_count);
                println!("  Streams referenced: {}", validation.streams.join(", "));

                // Check for stream conflicts
                let conflicts = match liven::import_export::check_stream_conflicts(
                    &mut client,
                    &validation.streams,
                )
                .await
                {
                    Ok(conflicts) => conflicts,
                    Err(e) => {
                        println!("\x1b[31m❌ Stream conflict check failed: {}\x1b[0m", e);
                        exit(1);
                    }
                };

                if !conflicts.is_empty() {
                    println!("\n\x1b[31m❌ Stream conflicts detected:\x1b[0m");
                    for stream in &conflicts {
                        println!("  ✗ {}: EXISTS with data", stream);
                    }
                    println!("\nTo resolve conflicts:");
                    for stream in &conflicts {
                        println!(
                            "  - Drop stream '{}': liven query \"drop('{}')\"",
                            stream, stream
                        );
                        println!(
                            "  - Export existing: liven export --path {}_backup.jsonl --stream {}",
                            stream, stream
                        );
                    }
                    println!("  - Or remove records from file and retry");
                    exit(1);
                }

                if dry_run {
                    println!(
                        "\n\x1b[32m✔ Dry run completed successfully. No data was written.\x1b[0m"
                    );
                    println!("\nTo perform the actual import, run:");
                    println!("  liven import --path {}", file_path);
                    exit(0);
                }

                // Perform the actual import
                match liven::import_export::import_jsonl_file(&mut client, Path::new(&file_path))
                    .await
                {
                    Ok(stats) => {
                        println!(
                            "\n\x1b[32m✔ Successfully imported {} records.\x1b[0m",
                            stats.imported_count
                        );
                        if stats.skipped_count > 0 {
                            println!("  Skipped: {}", stats.skipped_count);
                        }
                    }
                    Err(e) => {
                        println!("\x1b[31m❌ Import failed: {}\x1b[0m", e);
                        exit(1);
                    }
                }
            } else {
                // binary format
                if dry_run {
                    println!(
                        "\x1b[32m✔ Dry run completed successfully. No data was written.\x1b[0m"
                    );
                    println!("\nTo perform the actual import, run:");
                    println!("  liven import --path {}", file_path);
                    exit(0);
                }

                match liven::import_export::import_binary_file(&mut client, Path::new(&file_path))
                    .await
                {
                    Ok(stats) => {
                        println!(
                            "\n\x1b[32m✔ Successfully imported {} records.\x1b[0m",
                            stats.imported_count
                        );
                        if stats.skipped_count > 0 {
                            println!("  Skipped: {}", stats.skipped_count);
                        }
                    }
                    Err(e) => {
                        println!("\x1b[31m❌ Import failed: {}\x1b[0m", e);
                        exit(1);
                    }
                }
            }
        }
        "export" => {
            let mut stream_name: Option<String> = None;
            let mut file_path: Option<String> = None;
            let mut all_streams = false;
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
                    "--path" => {
                        if idx + 1 < args.len() {
                            file_path = Some(args[idx + 1].clone());
                            idx += 2;
                        } else {
                            println!("\x1b[31m❌ Error: --path requires a file path.\x1b[0m");
                            exit(1);
                        }
                    }
                    "--all" => {
                        all_streams = true;
                        idx += 1;
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

            if stream_name.is_none() && !all_streams {
                println!("\x1b[31m❌ Error: Must specify either --stream <name> or --all.\x1b[0m");
                exit(1);
            }

            if stream_name.is_some() && all_streams {
                println!("\x1b[31m❌ Error: Cannot specify both --stream and --all.\x1b[0m");
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
                            .unwrap_or("jsonl");
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
                    format!("{}/{}_{}.jsonl", download_dir, target_stream, timestamp)
                };

                // Determine format from file extension
                let path_obj = Path::new(&stream_file_path);
                let format = path_obj
                    .extension()
                    .and_then(|ext| ext.to_str())
                    .map(|ext| ext.to_lowercase())
                    .unwrap_or_else(|| "jsonl".to_string());

                if format != "jsonl" && format != "liven" {
                    println!(
                        "\x1b[31m❌ Error: Unsupported export format '{}'. Use .jsonl or .liven\x1b[0m",
                        format
                    );
                    exit(1);
                }

                println!(
                    "Exporting stream '{}' data to '{}' in '{}' format...",
                    target_stream, stream_file_path, format
                );

                if format == "jsonl" {
                    match liven::import_export::export_jsonl_file(
                        &mut client,
                        target_stream,
                        Path::new(&stream_file_path),
                    )
                    .await
                    {
                        Ok(count) => {
                            println!(
                                "\x1b[32m✔ Successfully exported {} records to {}.\x1b[0m",
                                count, stream_file_path
                            );
                        }
                        Err(e) => {
                            println!("\x1b[31m❌ Export failed: {}\x1b[0m", e);
                            exit(1);
                        }
                    }
                } else {
                    // binary format
                    match liven::import_export::export_binary_file(
                        &mut client,
                        target_stream,
                        Path::new(&stream_file_path),
                    )
                    .await
                    {
                        Ok(count) => {
                            println!(
                                "\x1b[32m✔ Successfully exported {} records to {}.\x1b[0m",
                                count, stream_file_path
                            );
                        }
                        Err(e) => {
                            println!("\x1b[31m❌ Export failed: {}\x1b[0m", e);
                            exit(1);
                        }
                    }
                }
            }
        }
        _ => {
            print_usage();
            exit(1);
        }
    }

    Ok(())
}
