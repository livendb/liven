#[cfg(feature = "server")]
use liven::client::LivenClient;
#[cfg(feature = "server")]
use liven::config::{AppConfig, LimitsConfig, SecurityConfig, ServerConfig, StorageConfig};
#[cfg(feature = "server")]
use liven::server::run_server;
#[cfg(feature = "server")]
use liven::storage::StorageEngine;
#[cfg(feature = "server")]
use std::fs;
#[cfg(feature = "server")]
use std::sync::Arc;
#[cfg(feature = "server")]
use std::time::Duration;

#[cfg(feature = "server")]
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
            max_connections: 10000,
            broadcast_capacity: 4096,
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

#[cfg(feature = "server")]
#[tokio::test]
async fn test_client_listen_stream() {
    use futures_util::StreamExt;

    let test_dir = format!(
        "./data_liven_listen_test_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let _ = fs::remove_dir_all(&test_dir);
    fs::create_dir_all(&test_dir).unwrap();

    let port = 45147;

    let config = AppConfig {
        server: ServerConfig {
            environment: "test".to_string(),
            host: "127.0.0.1".to_string(),
            db_port: port,
            webui_port: port - 1,
            max_connections: 10000,
            broadcast_capacity: 4096,
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

    let engine = Arc::new(StorageEngine::new(&config.storage.data_directory, 1024 * 1024).unwrap());

    let engine_clone = engine.clone();
    let config_clone = config.clone();
    tokio::spawn(async move {
        let _ = run_server(engine_clone, config_clone, false).await;
    });

    tokio::time::sleep(Duration::from_millis(300)).await;

    let addr = format!("127.0.0.1:{}", port);
    let client = LivenClient::connect_with_auth_mode(&addr, "default_client", "none")
        .await
        .unwrap();

    let mut stream = client.listen("test_stream").await.unwrap();

    // Append some data in the background
    let engine_append = engine.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        engine_append
            .append(
                "test_stream",
                "listening_key",
                liven::types::DataValue::String("listening_value".to_string()),
                false,
            )
            .unwrap();
    });

    // Receive the appended item via the stream
    if let Some(res) = stream.next().await {
        let record = res.unwrap();
        assert_eq!(record.stream_name, "test_stream");
        assert_eq!(record.key.to_string(), "listening_key");
        assert_eq!(
            record.value,
            liven::types::DataValue::String("listening_value".to_string())
        );
    } else {
        panic!("Stream ended before receiving any elements");
    }

    let _ = fs::remove_dir_all(&test_dir);
}
