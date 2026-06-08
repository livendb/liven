use liven::client::LivenClient;
use liven::config::{
    AppConfig, LimitsConfig, SecurityConfig, ServerConfig, StorageConfig, ZtnaConfig,
};
use liven::server::run_server;
use liven::storage::StorageEngine;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

fn generate_test_certs(dir: &Path) {
    // Write CA config file
    let ca_conf_path = dir.join("ca.conf");
    let ca_conf = r#"[ req ]
distinguished_name = req_distinguished_name
x509_extensions = v3_ca
prompt = no

[ req_distinguished_name ]
CN = MyTestCA

[ v3_ca ]
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid:always,issuer
basicConstraints = critical, CA:true
keyUsage = critical, digitalSignature, cRLSign, keyCertSign
"#;
    fs::write(&ca_conf_path, ca_conf).expect("Failed to write ca.conf");

    // 1. Generate CA key and certificate
    let status = std::process::Command::new("openssl")
        .args(&[
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            &dir.join("ca.key").to_string_lossy(),
            "-out",
            &dir.join("ca.crt").to_string_lossy(),
            "-days",
            "365",
            "-config",
            &ca_conf_path.to_string_lossy(),
        ])
        .status()
        .expect("Failed to execute openssl for CA generation");
    assert!(status.success(), "openssl CA generation failed");

    // 2. Generate Server key and CSR
    let status = std::process::Command::new("openssl")
        .args(&[
            "genrsa",
            "-out",
            &dir.join("server.key").to_string_lossy(),
            "2048",
        ])
        .status()
        .expect("Failed to generate server key");
    assert!(status.success());

    let status = std::process::Command::new("openssl")
        .args(&[
            "req",
            "-new",
            "-key",
            &dir.join("server.key").to_string_lossy(),
            "-out",
            &dir.join("server.csr").to_string_lossy(),
            "-subj",
            "/CN=127.0.0.1",
        ])
        .status()
        .expect("Failed to generate server CSR");
    assert!(status.success());

    // Write server extensions file
    let server_ext_path = dir.join("server_ext.conf");
    let server_ext = r#"[ v3_req ]
basicConstraints = CA:FALSE
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = DNS:127.0.0.1,IP:127.0.0.1
"#;
    fs::write(&server_ext_path, server_ext).expect("Failed to write server_ext.conf");

    // 3. Sign Server CSR with CA
    let status = std::process::Command::new("openssl")
        .args(&[
            "x509",
            "-req",
            "-in",
            &dir.join("server.csr").to_string_lossy(),
            "-CA",
            &dir.join("ca.crt").to_string_lossy(),
            "-CAkey",
            &dir.join("ca.key").to_string_lossy(),
            "-CAcreateserial",
            "-out",
            &dir.join("server.crt").to_string_lossy(),
            "-days",
            "365",
            "-extfile",
            &server_ext_path.to_string_lossy(),
            "-extensions",
            "v3_req",
        ])
        .status()
        .expect("Failed to sign server CSR");
    assert!(status.success());

    // 4. Generate Client key and CSR for CN "alice"
    let status = std::process::Command::new("openssl")
        .args(&[
            "genrsa",
            "-out",
            &dir.join("client.key").to_string_lossy(),
            "2048",
        ])
        .status()
        .expect("Failed to generate client key");
    assert!(status.success());

    let status = std::process::Command::new("openssl")
        .args(&[
            "req",
            "-new",
            "-key",
            &dir.join("client.key").to_string_lossy(),
            "-out",
            &dir.join("client.csr").to_string_lossy(),
            "-subj",
            "/CN=alice",
        ])
        .status()
        .expect("Failed to generate client CSR");
    assert!(status.success());

    // Write client extensions file
    let client_ext_path = dir.join("client_ext.conf");
    let client_ext = r#"[ v3_req ]
basicConstraints = CA:FALSE
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = clientAuth
"#;
    fs::write(&client_ext_path, client_ext).expect("Failed to write client_ext.conf");

    // 5. Sign Client CSR with CA
    let status = std::process::Command::new("openssl")
        .args(&[
            "x509",
            "-req",
            "-in",
            &dir.join("client.csr").to_string_lossy(),
            "-CA",
            &dir.join("ca.crt").to_string_lossy(),
            "-CAkey",
            &dir.join("ca.key").to_string_lossy(),
            "-CAcreateserial",
            "-out",
            &dir.join("client.crt").to_string_lossy(),
            "-days",
            "365",
            "-extfile",
            &client_ext_path.to_string_lossy(),
            "-extensions",
            "v3_req",
        ])
        .status()
        .expect("Failed to sign client CSR");
    assert!(status.success());
}

#[tokio::test]
async fn test_ztna_development_mode() {
    let test_dir = std::env::temp_dir().join(format!(
        "liven_ztna_dev_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = fs::remove_dir_all(&test_dir);
    fs::create_dir_all(&test_dir).unwrap();

    let port = 45221;

    let config = AppConfig {
        server: ServerConfig {
            environment: "development".to_string(),
            host: "127.0.0.1".to_string(),
            db_port: port,
            webui_port: port - 1,
        },
        storage: StorageConfig {
            data_directory: test_dir.to_string_lossy().to_string(),
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

    // Start server
    let engine_clone = engine.clone();
    let config_clone = config.clone();
    tokio::spawn(async move {
        let _ = run_server(engine_clone, config_clone, false).await;
    });

    tokio::time::sleep(Duration::from_millis(300)).await;

    // Connect with a native client (cleartext raw connection succeeds)
    let client_res =
        LivenClient::connect_with_auth_mode(&format!("127.0.0.1:{}", port), "test_client", "none")
            .await;
    assert!(
        client_res.is_ok(),
        "Dev mode raw connection failed: {:?}",
        client_res.err()
    );

    let mut client = client_res.unwrap();
    let query_res = client.query("select 1").await;
    assert!(query_res.is_ok(), "Failed to execute query in dev mode");

    let _ = fs::remove_dir_all(&test_dir);
}

#[tokio::test]
async fn test_ztna_production_mode_cleartext_rejected() {
    let test_dir = std::env::temp_dir().join(format!(
        "liven_ztna_prod_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = fs::remove_dir_all(&test_dir);
    fs::create_dir_all(&test_dir).unwrap();

    // Generate real self-signed certificates
    generate_test_certs(&test_dir);

    let port = 45223;

    let config = AppConfig {
        server: ServerConfig {
            environment: "production".to_string(),
            host: "127.0.0.1".to_string(),
            db_port: port,
            webui_port: port - 1,
        },
        storage: StorageConfig {
            data_directory: test_dir.to_string_lossy().to_string(),
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
            ztna: Some(ZtnaConfig {
                enabled: true,
                cert_path: Some(test_dir.join("server.crt").to_string_lossy().to_string()),
                key_path: Some(test_dir.join("server.key").to_string_lossy().to_string()),
                client_ca_path: Some(test_dir.join("ca.crt").to_string_lossy().to_string()),
            }),
        },
    };

    let engine = Arc::new(StorageEngine::new(&config.storage.data_directory, 1024 * 1024).unwrap());

    // Start server
    let engine_clone = engine.clone();
    let config_clone = config.clone();
    tokio::spawn(async move {
        let _ = run_server(engine_clone, config_clone, false).await;
    });

    tokio::time::sleep(Duration::from_millis(300)).await;

    // Verify raw TCP connection attempts without TLS are dropped/terminated
    let client_res =
        LivenClient::connect_with_auth_mode(&format!("127.0.0.1:{}", port), "test_client", "none")
            .await;

    if let Ok(mut client) = client_res {
        let query_res = client.query("select 1").await;
        assert!(
            query_res.is_err(),
            "Cleartext raw TCP query must be rejected/fail under production mTLS mode"
        );
    }

    let _ = fs::remove_dir_all(&test_dir);
}

#[tokio::test]
async fn test_ztna_production_mode_mtls_success_with_filtering() {
    use futures_util::{SinkExt, StreamExt};
    use liven::codec::{LivenCodec, LivenFrame};
    use liven::server::AuthKeyRecord;
    use liven::types::DataValue;
    use tokio::net::TcpStream;
    use tokio_util::codec::Framed;

    let test_dir = std::env::temp_dir().join(format!(
        "liven_ztna_mtls_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = fs::remove_dir_all(&test_dir);
    fs::create_dir_all(&test_dir).unwrap();

    // 1. Generate real self-signed certificates
    generate_test_certs(&test_dir);

    // 2. Pre-populate StorageEngine with query data and auth_keys record for Alice
    let engine = Arc::new(StorageEngine::new(&test_dir, 1024 * 1024).unwrap());

    // Alice has admin role but restricted allowed_tags to [5] (DataValue::String type)
    let auth_rec = AuthKeyRecord {
        key_id: "alice".to_string(),
        role: "admin".to_string(),
        auth_key: "alice_hash".to_string(),
        status: "active".to_string(),
        allowed_tags: vec![5],
    };
    let json_val = serde_json::to_string(&auth_rec).unwrap();
    engine
        .append("auth_keys", "alice", DataValue::String(json_val), false)
        .unwrap();

    // Ingest data: sensor_1 is type_tag 5 (String), sensor_2 is type_tag 2 (Int)
    engine
        .append(
            "sensor_stream",
            "sensor_1",
            DataValue::String("25.5".to_string()),
            false,
        )
        .unwrap();
    engine
        .append("sensor_stream", "sensor_2", DataValue::Int(100), false)
        .unwrap();

    let port = 45225;

    let config = AppConfig {
        server: ServerConfig {
            environment: "production".to_string(),
            host: "127.0.0.1".to_string(),
            db_port: port,
            webui_port: port - 1,
        },
        storage: StorageConfig {
            data_directory: test_dir.to_string_lossy().to_string(),
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
            ztna: Some(ZtnaConfig {
                enabled: true,
                cert_path: Some(test_dir.join("server.crt").to_string_lossy().to_string()),
                key_path: Some(test_dir.join("server.key").to_string_lossy().to_string()),
                client_ca_path: Some(test_dir.join("ca.crt").to_string_lossy().to_string()),
            }),
        },
    };

    // Start server
    let engine_clone = engine.clone();
    let config_clone = config.clone();
    tokio::spawn(async move {
        let _ = run_server(engine_clone, config_clone, false).await;
    });

    tokio::time::sleep(Duration::from_millis(300)).await;

    // 3. Connect as Alice via mTLS using rustls ClientConfig
    let certs = {
        let f = fs::File::open(test_dir.join("client.crt")).unwrap();
        let mut r = std::io::BufReader::new(f);
        rustls_pemfile::certs(&mut r)
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    };
    let key = {
        let f = fs::File::open(test_dir.join("client.key")).unwrap();
        let mut r = std::io::BufReader::new(f);
        rustls_pemfile::private_key(&mut r).unwrap().unwrap()
    };
    let ca_certs = {
        let f = fs::File::open(test_dir.join("ca.crt")).unwrap();
        let mut r = std::io::BufReader::new(f);
        rustls_pemfile::certs(&mut r)
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    };

    let mut roots = tokio_rustls::rustls::RootCertStore::empty();
    for ca in ca_certs {
        roots.add(ca).unwrap();
    }

    let client_config = tokio_rustls::rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_auth_cert(certs, key)
        .unwrap();

    let connector = tokio_rustls::TlsConnector::from(Arc::new(client_config));
    let tcp_stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();
    tcp_stream.set_nodelay(true).unwrap();

    let server_name = tokio_rustls::rustls::pki_types::ServerName::try_from("127.0.0.1")
        .unwrap()
        .to_owned();
    let tls_stream = connector.connect(server_name, tcp_stream).await.unwrap();

    let mut framed = Framed::new(tls_stream, LivenCodec::new(true));

    // Send query to fetch sensor data
    framed
        .send(LivenFrame::Query("from(\"sensor_stream\")".to_string()))
        .await
        .unwrap();

    // Expect Records frame
    match framed.next().await {
        Some(Ok(LivenFrame::Records(records))) => {
            // Check that only sensor_1 (String) is returned, and sensor_2 (Int) is filtered out
            assert_eq!(
                records.len(),
                1,
                "Only one record should be returned due to tag filtering"
            );
            assert_eq!(records[0].key.as_str(), "sensor_1");
            assert_eq!(records[0].type_tag, 5); // String type tag
        }
        Some(Ok(other)) => {
            panic!("Expected Records frame, got: {:?}", other);
        }
        Some(Err(e)) => {
            panic!("Error receiving response: {:?}", e);
        }
        None => {
            panic!("Connection closed unexpectedly");
        }
    }

    let _ = fs::remove_dir_all(&test_dir);
}
