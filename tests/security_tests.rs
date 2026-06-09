use liven::client::LivenClient;
use liven::config::{
    AppConfig, AuthKeyConfig, LimitsConfig, SecurityConfig, ServerConfig, StorageConfig,
};
#[cfg(feature = "server")]
use liven::server::{AuthKeyRecord, run_server};
use liven::storage::StorageEngine;
use liven::types::DataValue;
use std::fs;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

// HTTP helper to send raw requests to local server
async fn send_http_request(
    port: u16,
    method: &str,
    path: &str,
    headers: Vec<(String, String)>,
    body: Option<&str>,
) -> (u16, Vec<(String, String)>, String) {
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();
    let content_len = body.map(|b| b.len()).unwrap_or(0);
    let mut req = format!(
        "{} {} HTTP/1.1\r\n\
         Host: 127.0.0.1:{}\r\n\
         Connection: close\r\n",
        method, path, port
    );
    for (k, v) in headers {
        req.push_str(&format!("{}: {}\r\n", k, v));
    }
    if body.is_some() {
        req.push_str(&format!("Content-Length: {}\r\n", content_len));
    }
    req.push_str("\r\n");
    if let Some(b) = body {
        req.push_str(b);
    }
    stream.write_all(req.as_bytes()).await.unwrap();

    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();

    let resp_str = String::from_utf8(response).unwrap();
    let parts: Vec<&str> = resp_str.split("\r\n\r\n").collect();
    let headers_part = parts[0];
    let body_part = parts.get(1).unwrap_or(&"").to_string();

    let header_lines: Vec<&str> = headers_part.split("\r\n").collect();
    let status_line = header_lines[0];
    let status_code: u16 = status_line.split_whitespace().collect::<Vec<&str>>()[1]
        .parse()
        .unwrap();

    let mut parsed_headers = Vec::new();
    for line in header_lines.iter().skip(1) {
        if let Some(idx) = line.find(':') {
            let k = line[..idx].trim().to_lowercase();
            let v = line[idx + 1..].trim().to_string();
            parsed_headers.push((k, v));
        }
    }

    (status_code, parsed_headers, body_part)
}

#[cfg(feature = "server")]
#[tokio::test]
async fn test_auth_key_handshake_lifecycle() {
    let test_dir = std::env::temp_dir().join(format!(
        "liven_security_test_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = fs::remove_dir_all(&test_dir);
    fs::create_dir_all(&test_dir).unwrap();

    let port = 45124;
    let master_key_str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    // Build configuration
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
            mode: "auth_key".to_string(),
            auth_key: Some(AuthKeyConfig {
                system_stream: "test_keys".to_string(),
                allow_local_auto_generation: true,
            }),
            master_key: Some(master_key_str.to_string()),
            ztna: None,
        },
    };

    // Spin up storage engine
    let engine = Arc::new(StorageEngine::new(&config.storage.data_directory, 1024 * 1024).unwrap());

    // Pre-populate a known administrative root auth key
    let raw_key = "a0b1c2d3e4f5a0b1c2d3e4f5a0b1c2d3e4f5a0b1c2d3e4f5a0b1c2d3e4f51234";
    let hash = blake3::hash(raw_key.as_bytes());
    let hash_hex = liven::security::hex_encode(hash.as_bytes());

    let auth_rec = AuthKeyRecord {
        key_id: "default-admin".to_string(),
        role: "admin".to_string(),
        auth_key: hash_hex,
        status: "active".to_string(),
        allowed_tags: Vec::new(),
    };
    let json_val = serde_json::to_string(&auth_rec).unwrap();
    engine
        .append(
            "auth_keys",
            "default-admin",
            DataValue::String(json_val),
            false,
        )
        .unwrap();

    // Run server in background
    let engine_clone = engine.clone();
    let config_clone = config.clone();
    tokio::spawn(async move {
        let _ = run_server(engine_clone, config_clone, false).await;
    });

    // Wait a brief moment for the server to start listening
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Connect with a native client using the pre-populated key
    let client_res = LivenClient::connect_with_auth_mode(
        &format!("127.0.0.1:{}?auth_key={}", port, raw_key),
        "default_client",
        "auth_key",
    )
    .await;
    assert!(
        client_res.is_ok(),
        "Client failed to connect and authenticate: {:?}",
        client_res.err()
    );

    let mut client = client_res.unwrap();

    // Perform simple query to verify communication works
    let query_res = client.query("select 1").await;
    assert!(query_res.is_ok());

    // Attempt to connect with an invalid key and ensure it gets rejected
    let invalid_client_res = LivenClient::connect_with_auth_mode(
        &format!("127.0.0.1:{}?auth_key=invalidkey12345", port),
        "default_client",
        "auth_key",
    )
    .await;
    assert!(
        invalid_client_res.is_err(),
        "Invalid key should have been rejected"
    );

    // Clean up files and directory
    let _ = fs::remove_dir_all(&test_dir);
}

#[cfg(feature = "server")]
#[tokio::test]
async fn test_rest_auth_key_challenge_login_lifecycle() {
    let test_dir = std::env::temp_dir().join(format!(
        "liven_rest_security_test_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = fs::remove_dir_all(&test_dir);
    fs::create_dir_all(&test_dir).unwrap();

    let port = 45134;
    let webui_port = port - 1;
    let master_key_str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    // Build configuration
    let config = AppConfig {
        server: ServerConfig {
            environment: "test".to_string(),
            host: "127.0.0.1".to_string(),
            db_port: port,
            webui_port,
            max_connections: 10000,
            broadcast_capacity: 4096,
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
            mode: "auth_key".to_string(),
            auth_key: Some(AuthKeyConfig {
                system_stream: "test_keys".to_string(),
                allow_local_auto_generation: true,
            }),
            master_key: Some(master_key_str.to_string()),
            ztna: None,
        },
    };

    // Spin up storage engine
    let engine = Arc::new(StorageEngine::new(&config.storage.data_directory, 1024 * 1024).unwrap());

    // Pre-populate a known administrative root auth key
    let raw_key = "test_rest_root_key_67890_test_rest_root_key_67890_test_rest_key_6";
    let hash = blake3::hash(raw_key.as_bytes());
    let hash_hex = liven::security::hex_encode(hash.as_bytes());

    let auth_rec = AuthKeyRecord {
        key_id: "default-admin".to_string(),
        role: "admin".to_string(),
        auth_key: hash_hex,
        status: "active".to_string(),
        allowed_tags: Vec::new(),
    };
    let json_val = serde_json::to_string(&auth_rec).unwrap();
    engine
        .append(
            "auth_keys",
            "default-admin",
            DataValue::String(json_val),
            false,
        )
        .unwrap();

    // Run server in background
    let engine_clone = engine.clone();
    let config_clone = config.clone();
    tokio::spawn(async move {
        let _ = run_server(engine_clone, config_clone, true).await;
    });

    // Wait a brief moment for the server to start listening
    tokio::time::sleep(Duration::from_millis(300)).await;

    // 1. Verify unauthenticated status
    let (code, _, body) =
        send_http_request(webui_port, "GET", "/api/system/auth/status", vec![], None).await;
    assert_eq!(code, 200);
    let status_json: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert!(!status_json["authenticated"].as_bool().unwrap());

    // 2. Post to login endpoint with the pre-populated key
    let login_payload = serde_json::json!({ "token": raw_key }).to_string();
    let (code, headers, body) = send_http_request(
        webui_port,
        "POST",
        "/api/system/auth/login",
        vec![("content-type".to_string(), "application/json".to_string())],
        Some(&login_payload),
    )
    .await;
    assert_eq!(code, 200);

    let login_json: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(login_json["status"].as_str().unwrap(), "success");
    assert_eq!(login_json["user_id"].as_str().unwrap(), "default-admin");

    // Extract liven_session cookie
    let mut session_id = String::new();
    for (k, v) in &headers {
        if k == "set-cookie" && v.starts_with("liven_session=") {
            let end_idx = v.find(';').unwrap_or(v.len());
            session_id = v["liven_session=".len()..end_idx].to_string();
        }
    }
    assert!(
        !session_id.is_empty(),
        "Session ID should be returned in Set-Cookie header"
    );

    // 3. Verify authenticated status using the cookie
    let (code, _, body) = send_http_request(
        webui_port,
        "GET",
        "/api/system/auth/status",
        vec![(
            "cookie".to_string(),
            format!("liven_session={}", session_id),
        )],
        None,
    )
    .await;
    assert_eq!(code, 200);
    let status_json: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert!(status_json["authenticated"].as_bool().unwrap());
    assert_eq!(status_json["user_id"].as_str().unwrap(), "default-admin");

    // 4. Generate a new key identity using the authenticated session
    let gen_payload = serde_json::json!({
        "key_id": "operator_alice",
        "role": "write-only"
    })
    .to_string();

    let (code, _, body) = send_http_request(
        webui_port,
        "POST",
        "/api/system/auth/keys",
        vec![
            (
                "cookie".to_string(),
                format!("liven_session={}", session_id),
            ),
            ("content-type".to_string(), "application/json".to_string()),
        ],
        Some(&gen_payload),
    )
    .await;
    assert_eq!(code, 200);

    let gen_json: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(gen_json["key_id"].as_str().unwrap(), "operator_alice");
    assert_eq!(gen_json["role"].as_str().unwrap(), "write-only");
    let raw_key_alice = gen_json["raw_key"].as_str().unwrap().to_string();
    assert_eq!(raw_key_alice.len(), 64); // 32 bytes in hex is 64 chars

    // 5. List keys and verify "operator_alice" is listed
    let (code, _, body) = send_http_request(
        webui_port,
        "GET",
        "/api/system/auth/keys",
        vec![(
            "cookie".to_string(),
            format!("liven_session={}", session_id),
        )],
        None,
    )
    .await;
    assert_eq!(code, 200);
    let keys_list: serde_json::Value = serde_json::from_str(&body).unwrap();
    let keys_array = keys_list.as_array().unwrap();
    let mut found_alice = false;
    for key_rec in keys_array {
        if key_rec["key_id"].as_str().unwrap() == "operator_alice" {
            found_alice = true;
            assert_eq!(key_rec["role"].as_str().unwrap(), "write-only");
            assert_eq!(key_rec["status"].as_str().unwrap(), "active");
        }
    }
    assert!(found_alice);

    // 6. Revoke the generated key "operator_alice"
    let revoke_payload = serde_json::json!({ "key_id": "operator_alice" }).to_string();
    let (code, _, body) = send_http_request(
        webui_port,
        "POST",
        "/api/system/auth/keys/revoke",
        vec![
            (
                "cookie".to_string(),
                format!("liven_session={}", session_id),
            ),
            ("content-type".to_string(), "application/json".to_string()),
        ],
        Some(&revoke_payload),
    )
    .await;
    assert_eq!(code, 200);
    let revoke_json: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(revoke_json["status"].as_str().unwrap(), "success");

    // 7. Verify listing keys again shows "operator_alice" is "revoked"
    let (code, _, body) = send_http_request(
        webui_port,
        "GET",
        "/api/system/auth/keys",
        vec![(
            "cookie".to_string(),
            format!("liven_session={}", session_id),
        )],
        None,
    )
    .await;
    assert_eq!(code, 200);
    let keys_list2: serde_json::Value = serde_json::from_str(&body).unwrap();
    let keys_array2 = keys_list2.as_array().unwrap();
    let mut found_alice_revoked = false;
    for key_rec in keys_array2 {
        if key_rec["key_id"].as_str().unwrap() == "operator_alice" {
            found_alice_revoked = true;
            assert_eq!(key_rec["status"].as_str().unwrap(), "revoked");
        }
    }
    assert!(found_alice_revoked);

    // 8. Logout the authenticated session
    let (code, headers, body) = send_http_request(
        webui_port,
        "POST",
        "/api/system/auth/logout",
        vec![(
            "cookie".to_string(),
            format!("liven_session={}", session_id),
        )],
        None,
    )
    .await;
    assert_eq!(code, 200);
    let logout_json: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(logout_json["status"].as_str().unwrap(), "success");

    // Verify Set-Cookie header with Max-Age=0 or clearing is returned
    let mut cleared_cookie = false;
    for (k, v) in &headers {
        if k == "set-cookie" && v.contains("liven_session=") && v.contains("Max-Age=0") {
            cleared_cookie = true;
        }
    }
    assert!(cleared_cookie, "Should return an expired cookie header");

    // 9. Verify session is no longer authenticated on subsequent check
    let (code, _, body) = send_http_request(
        webui_port,
        "GET",
        "/api/system/auth/status",
        vec![(
            "cookie".to_string(),
            format!("liven_session={}", session_id),
        )],
        None,
    )
    .await;
    assert_eq!(code, 200);
    let status_json2: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert!(!status_json2["authenticated"].as_bool().unwrap());

    // Clean up directory
    let _ = fs::remove_dir_all(&test_dir);
}

#[cfg(feature = "server")]
#[test]
fn test_continuous_edge_check_query_capabilities() {
    use liven::security::{CAP_ADMIN, CAP_NONE, CAP_READ, CAP_ROOT, CAP_WRITE};
    use liven::server::check_query_capabilities;

    // CAP_ROOT can do everything
    assert!(check_query_capabilities("from(\"stream\")", CAP_ROOT));
    assert!(check_query_capabilities(
        "from(\"stream\").insert(\"key\", {val: 1})",
        CAP_ROOT
    ));
    assert!(check_query_capabilities("drop(\"stream\")", CAP_ROOT));

    // CAP_READ can read but not write or admin
    assert!(check_query_capabilities("from(\"stream\")", CAP_READ));
    assert!(check_query_capabilities("tail(\"stream\")", CAP_READ));
    assert!(!check_query_capabilities(
        "from(\"stream\").insert(\"key\", {val: 1})",
        CAP_READ
    ));
    assert!(!check_query_capabilities("drop(\"stream\")", CAP_READ));

    // CAP_WRITE can write but not admin or list/select/tail
    assert!(check_query_capabilities(
        "from(\"stream\").insert(\"key\", {val: 1})",
        CAP_WRITE
    ));
    assert!(!check_query_capabilities("from(\"stream\")", CAP_WRITE));
    assert!(!check_query_capabilities("drop(\"stream\")", CAP_WRITE));

    // CAP_ADMIN can drop but not read or write on its own
    assert!(check_query_capabilities("drop(\"stream\")", CAP_ADMIN));
    assert!(!check_query_capabilities("from(\"stream\")", CAP_ADMIN));
    assert!(!check_query_capabilities(
        "from(\"stream\").insert(\"key\", {val: 1})",
        CAP_ADMIN
    ));

    // CAP_NONE can do nothing
    assert!(!check_query_capabilities("from(\"stream\")", CAP_NONE));
    assert!(!check_query_capabilities(
        "from(\"stream\").insert(\"key\", {val: 1})",
        CAP_NONE
    ));
    assert!(!check_query_capabilities("drop(\"stream\")", CAP_NONE));
}
