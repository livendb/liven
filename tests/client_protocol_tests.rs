use liven::client::LivenClient;
use liven::config::{AppConfig, LimitsConfig, SecurityConfig, ServerConfig, StorageConfig};
use liven::server::run_server;
use liven::storage::StorageEngine;
use std::fs;
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn test_client_protocol_prefixes() {
    let test_dir = format!(
        "./data_liven_prefix_test_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let _ = fs::remove_dir_all(&test_dir);
    fs::create_dir_all(&test_dir).unwrap();

    let port = 45145;

    // Build configuration with no auth to make prefix testing trivial
    let config = AppConfig {
        server: ServerConfig {
            environment: "test".to_string(),
            host: "127.0.0.1".to_string(),
            db_port: port,
            webui_port: port - 1,
        },
        storage: StorageConfig {
            data_directory: test_dir.to_string(),
            max_segment_size_mb: 10,
            sync_mode: "always".to_string(),
            sync_interval_ms: 10,
        },
        limits: LimitsConfig {
            max_concurrent_streams: 10,
            max_open_file_descriptors: 10,
            max_index_ram_mb: 10,
            max_segment_size_mb: 10,
        },
        security: SecurityConfig {
            mode: "none".to_string(),
            auth_key: None,
            master_key: None,
            ztna: None,
        },
    };

    // Spin up storage engine
    let engine = Arc::new(StorageEngine::new(&config.storage.data_directory, 1024 * 1024).unwrap());

    // Run server in background
    let engine_clone = engine.clone();
    let config_clone = config.clone();
    tokio::spawn(async move {
        let _ = run_server(engine_clone, config_clone, false).await;
    });

    // Wait a brief moment for the server to start listening
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Test connecting with raw and liven:// prefixes
    let addresses = vec![
        format!("127.0.0.1:{}", port),
        format!("liven://127.0.0.1:{}", port),
    ];

    for addr in addresses {
        // Connect explicitly with "none" mode to align with the server's mode and avoid loading liven.toml's auth_key mode
        let client_res = LivenClient::connect_with_auth_mode(&addr, "default_client", "none").await;
        assert!(
            client_res.is_ok(),
            "Failed to connect to the instance using: {} (Error: {:?})",
            addr,
            client_res.err()
        );
    }

    // Clean up
    let _ = fs::remove_dir_all(&test_dir);
}
