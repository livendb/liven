// Rebuild trigger for embedded static UI assets
use crate::codec::{LivenCodec, LivenFrame};
use crate::error::LivenError;
use crate::executor::{apply_pipeline_stages_to_vec, execute_query, execute_query_stream};
use crate::parser::{parse_pipeline, parse_query};
use crate::storage::StorageEngine;
use crate::types::{DataValue, ExportFormat, PipelineStage, Query, Record};
use axum::{
    Json, Router,
    body::Body,
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use futures_util::{sink::SinkExt, stream::StreamExt};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;
use std::time::Duration;
use tower_http::cors::CorsLayer;
use tracing::info;

#[derive(rust_embed::RustEmbed)]
#[folder = "ui/dist/"]
struct Assets;

use axum::extract::Query as AxumQuery;
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthKeyRecord {
    pub key_id: String,
    pub role: String,
    pub auth_key: String,
    pub status: String,
    #[serde(default)]
    pub allowed_tags: Vec<u32>,
}

pub struct SessionInfo {
    pub user_id: String,
    /// Role captured at login time. Not re-validated against the current
    /// auth_keys record on every request — a role change on an existing
    /// key (if that feature is ever added) won't take effect until the
    /// session is recreated.
    pub role: String,
    pub last_seen: std::time::Instant,
}

use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::server::TlsStream;

pub enum LivenStream {
    Cleartext(TcpStream),
    Encrypted(TlsStream<TcpStream>),
}

impl AsyncRead for LivenStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            LivenStream::Cleartext(s) => Pin::new(s).poll_read(cx, buf),
            LivenStream::Encrypted(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for LivenStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            LivenStream::Cleartext(s) => Pin::new(s).poll_write(cx, buf),
            LivenStream::Encrypted(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            LivenStream::Cleartext(s) => Pin::new(s).poll_flush(cx),
            LivenStream::Encrypted(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            LivenStream::Cleartext(s) => Pin::new(s).poll_shutdown(cx),
            LivenStream::Encrypted(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientIdentity {
    pub client_cn: String,
    pub capabilities: u8,
    pub allowed_tags: Vec<u32>,
    pub auth_key_hash: String,
}

pub struct ActiveNativeConnection {
    pub auth_key_hash: String,
    pub close_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

pub struct ServerState {
    pub engine: Arc<StorageEngine>,
    pub config: crate::config::AppConfig,
    pub sessions: Mutex<HashMap<String, SessionInfo>>,
    pub active_connections: Mutex<Vec<ActiveNativeConnection>>,
    pub identity_cache: std::sync::RwLock<HashMap<String, ClientIdentity>>,
}

impl ServerState {
    /// Get the number of active connections
    pub fn active_connection_count(&self) -> usize {
        self.active_connections.lock().unwrap().len()
    }

    /// Get the number of active broadcast subscribers
    pub fn broadcast_subscriber_count(&self) -> usize {
        self.engine.broadcast_subscriber_count()
    }

    /// Write current status to a file for CLI status command
    pub fn write_status_file(&self) -> std::io::Result<()> {
        use std::io::Write;

        let status_file = "liven.status";
        let mut file = std::fs::File::create(status_file)?;

        let status = format!(
            "connections={}\nsubscribers={}",
            self.active_connection_count(),
            self.broadcast_subscriber_count()
        );

        file.write_all(status.as_bytes())?;
        Ok(())
    }
}

pub fn lookup_key_by_hash(engine: &StorageEngine, incoming_hash: &str) -> Option<AuthKeyRecord> {
    let keys = engine.list_keys("auth_keys");
    for k in keys {
        if let Ok(Some(rec)) = engine.get("auth_keys", &k)
            && let DataValue::String(json_str) = rec.value
            && let Ok(auth_rec) = serde_json::from_str::<AuthKeyRecord>(&json_str)
            && auth_rec.auth_key == incoming_hash
        {
            return Some(auth_rec);
        }
    }
    None
}

pub fn kill_active_connections_for_hash(state: &ServerState, hash: &str) {
    let mut conns = state.active_connections.lock().unwrap();
    let before_len = conns.len();
    conns.retain_mut(|conn| {
        if conn.auth_key_hash == hash {
            if let Some(tx) = conn.close_tx.take() {
                let _ = tx.send(());
            }
            false
        } else {
            true
        }
    });
    let after_len = conns.len();
    if before_len > after_len {
        tracing::info!(
            "Killed {} active native connections for hash {}",
            before_len - after_len,
            hash
        );
    }
}

pub fn bootstrap_auth_keys_table(
    engine: &StorageEngine,
    config_master_key: Option<&str>,
) -> crate::error::Result<()> {
    let keys = engine.list_keys("auth_keys");
    let mut root_admin_exists = false;

    for key_name in &keys {
        if let Ok(Some(record)) = engine.get("auth_keys", key_name)
            && let DataValue::String(json_str) = record.value
            && let Ok(auth_rec) = serde_json::from_str::<AuthKeyRecord>(&json_str)
            && auth_rec.role == "admin"
        {
            root_admin_exists = true;
            break;
        }
    }

    if root_admin_exists {
        // Complete the boot sequence completely silently regarding credentials.
        return Ok(());
    }

    // First-Time Initialization Only
    let raw_key = if let Some(mkey) = config_master_key {
        mkey.to_string()
    } else {
        let mut raw_key_bytes = [0u8; 32];
        rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut raw_key_bytes);
        crate::security::hex_encode(&raw_key_bytes)
    };

    let hash = blake3::hash(raw_key.as_bytes());
    let hash_hex = crate::security::hex_encode(hash.as_bytes());

    let auth_rec = AuthKeyRecord {
        key_id: "default-admin".to_string(),
        role: "admin".to_string(),
        auth_key: hash_hex,
        status: "active".to_string(),
        allowed_tags: Vec::new(),
    };

    let json_val = serde_json::to_string(&auth_rec).map_err(|e| e.to_string())?;
    engine.append(
        "auth_keys",
        "default-admin",
        DataValue::String(json_val),
        false,
    )?;

    println!("\x1b[1;31m");
    println!("########################################################################");
    println!("#                      LIVENDB INITIALIZATION WARNING                  #");
    println!("########################################################################");
    println!("\x1b[0m");
    println!("  A DEFAULT ROOT ADMINISTRATIVE AUTH KEY HAS BEEN GENERATED:");
    println!();
    println!("    \x1b[1;33m{}\x1b[0m", raw_key);
    println!();
    println!("  Please save this key securely! It will never be shown again.");
    println!("  All database endpoints on Port 43120 and 43121 are now SECURE BY DEFAULT.");
    println!("\x1b[1;31m");
    println!("########################################################################");
    println!("\x1b[0m");

    Ok(())
}

fn load_certs(
    path: &str,
) -> Result<Vec<tokio_rustls::rustls::pki_types::CertificateDer<'static>>, String> {
    let file = File::open(path).map_err(|e| format!("Failed to open cert file {}: {}", path, e))?;
    let mut reader = BufReader::new(file);
    let certs = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("Failed to parse certificates: {}", e))?;
    Ok(certs)
}

fn load_private_key(
    path: &str,
) -> Result<tokio_rustls::rustls::pki_types::PrivateKeyDer<'static>, String> {
    let file = File::open(path).map_err(|e| format!("Failed to open key file {}: {}", path, e))?;
    let mut reader = BufReader::new(file);
    let key = rustls_pemfile::private_key(&mut reader)
        .map_err(|e| format!("Failed to parse private key: {}", e))?
        .ok_or_else(|| "Private key not found in PEM".to_string())?;
    Ok(key)
}

fn build_tls_config(
    cert_path: &str,
    key_path: &str,
    ca_path: &str,
) -> Result<Arc<tokio_rustls::rustls::ServerConfig>, String> {
    let certs = load_certs(cert_path)?;
    let key = load_private_key(key_path)?;

    // Load Client Root CAs
    let ca_file =
        File::open(ca_path).map_err(|e| format!("Failed to open CA file {}: {}", ca_path, e))?;
    let mut ca_reader = BufReader::new(ca_file);
    let ca_certs = rustls_pemfile::certs(&mut ca_reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("Failed to parse CA certificates: {}", e))?;

    let mut roots = tokio_rustls::rustls::RootCertStore::empty();
    for ca_cert in ca_certs {
        roots
            .add(ca_cert)
            .map_err(|e| format!("Failed to add root certificate: {}", e))?;
    }

    let client_verifier =
        tokio_rustls::rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .map_err(|e| format!("Failed to build client verifier: {}", e))?;

    let config = tokio_rustls::rustls::ServerConfig::builder()
        .with_client_cert_verifier(client_verifier)
        .with_single_cert(certs, key)
        .map_err(|e| format!("Failed to configure TLS server: {}", e))?;

    Ok(Arc::new(config))
}

fn extract_cn_from_der(der: &[u8]) -> Option<String> {
    use x509_parser::prelude::FromDer;
    let (_, cert) = x509_parser::certificate::X509Certificate::from_der(der).ok()?;
    for rdn in cert.subject().iter_common_name() {
        if let Ok(cn) = rdn.as_str() {
            return Some(cn.to_string());
        }
    }
    None
}

pub async fn run_server(
    engine: Arc<StorageEngine>,
    config: crate::config::AppConfig,
    run_ui: bool,
) -> crate::error::Result<()> {
    let state = Arc::new(ServerState {
        engine: engine.clone(),
        config: config.clone(),
        sessions: Mutex::new(HashMap::new()),
        active_connections: Mutex::new(Vec::new()),
        identity_cache: std::sync::RwLock::new(HashMap::new()),
    });

    if config.security.mode == "none" {
        println!("\x1b[1;31m");
        println!("########################################################################");
        println!("# WARNING: SECURITY IS EXPLICITLY DISABLED.                            #");
        println!("# LIVENDB IS OPEN TO UNAUTHENTICATED TRAFFIC.                          #");
        println!("########################################################################");
        println!("\x1b[0m");
    } else {
        // Bootstrap default-admin key if necessary
        bootstrap_auth_keys_table(&engine, config.security.master_key.as_deref())?;
    }

    // P12: Development mode warning banner
    if config.security.mode == "none"
        && (config.server.environment == "development" || config.server.environment == "test")
    {
        println!("┌─────────────────────────────────────────────┐");
        println!("│  🔓 WARNING: Authentication is disabled.       │");
        println!("│  LIVEN accepts all connections.             │");
        println!("│  Set security.mode = auth_key in liven.toml │");
        println!("│  before deploying to production.            │");
        println!("└─────────────────────────────────────────────┘");
    }

    // Populate identity_cache on startup from database stream keys
    {
        let keys = engine.list_keys("auth_keys");
        let mut identity_cache = state.identity_cache.write().unwrap();
        for k in keys {
            if let Ok(Some(rec)) = engine.get("auth_keys", &k)
                && let DataValue::String(json_str) = rec.value
                && let Ok(auth_rec) = serde_json::from_str::<AuthKeyRecord>(&json_str)
                && auth_rec.status == "active"
            {
                let capabilities = crate::server::capabilities_for_role(&auth_rec.role);
                let client_cn = auth_rec.key_id.clone();
                identity_cache.insert(
                    client_cn.clone(),
                    ClientIdentity {
                        client_cn,
                        capabilities,
                        allowed_tags: auth_rec.allowed_tags.clone(),
                        auth_key_hash: auth_rec.auth_key.clone(),
                    },
                );
            }
        }
    }

    // Background checker task to instantly locate and close connections when keys are revoked
    let engine_for_check = engine.clone();
    let state_for_check = state.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            let hashes_to_check: Vec<String> = {
                let conns = state_for_check.active_connections.lock().unwrap();
                conns.iter().map(|c| c.auth_key_hash.clone()).collect()
            };

            for hash in hashes_to_check {
                let is_active = if state_for_check.config.security.mode == "none" {
                    true
                } else {
                    match lookup_key_by_hash(&engine_for_check, &hash) {
                        Some(rec) => rec.status == "active",
                        None => false,
                    }
                };

                if !is_active {
                    kill_active_connections_for_hash(&state_for_check, &hash);
                }
            }
        }
    });

    // Auto-compaction background task — checks every 60 seconds
    let engine_compact = engine.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(60));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            if engine_compact.should_compact() {
                tracing::info!("Auto-compaction triggered");
                let eng = engine_compact.clone();
                if let Err(e) = tokio::task::spawn_blocking(move || eng.compact())
                    .await
                    .unwrap_or(Err(crate::error::LivenError::Internal(
                        "Compaction task panicked".to_string(),
                    )))
                {
                    tracing::warn!("Auto-compaction failed: {}", e);
                }
            }
        }
    });

    // Status update background task — updates status file every 5 seconds
    let state_for_status = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(5));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            if let Err(e) = state_for_status.write_status_file() {
                tracing::debug!("Failed to write status file: {}", e);
            }
        }
    });

    // UI app router serving embedded SPA static assets AND Web API
    let ui_app = Router::new()
        .route("/ws", get(ws_handler))
        .route("/api/live/stream", get(ws_handler))
        .route("/api/health", get(health_handler))
        .route("/api/heartbeat", get(heartbeat_handler))
        .route("/api/system/config", get(system_config_handler))
        .route("/api/system/auth/status", get(system_auth_status_handler))
        .route("/api/system/auth/login", post(system_auth_login_handler))
        .route("/api/system/auth/logout", post(system_auth_logout_handler))
        .route(
            "/api/system/auth/keys",
            get(system_auth_list_keys_handler).post(system_auth_generate_key_handler),
        )
        .route(
            "/api/system/auth/keys/revoke",
            post(system_auth_revoke_key_handler),
        )
        .route(
            "/api/system/auth/keys/role",
            post(system_auth_update_role_handler),
        )
        .route("/api/ingest", post(ingest_handler))
        .route("/api/query", post(query_handler))
        .route("/api/streams", get(list_streams_handler))
        .fallback(static_handler)
        .with_state(state.clone())
        .layer(CorsLayer::permissive());

    let env = config.server.environment.trim().to_lowercase();
    let ztna_enabled = config.security.ztna.as_ref().is_some_and(|z| z.enabled);
    let is_dev = env == "development" || env == "test" || !ztna_enabled;

    let tls_acceptor = if !is_dev {
        let ztna_config = config
            .security
            .ztna
            .clone()
            .ok_or_else(|| "ZTNA configuration is missing".to_string())?;
        let cert_path = ztna_config
            .cert_path
            .unwrap_or_else(|| "./certs/server.crt".to_string());
        let key_path = ztna_config
            .key_path
            .unwrap_or_else(|| "./certs/server.key".to_string());
        let ca_path = ztna_config
            .client_ca_path
            .unwrap_or_else(|| "./certs/ca.crt".to_string());

        let server_config = build_tls_config(&cert_path, &key_path, &ca_path)?;
        Some(tokio_rustls::TlsAcceptor::from(server_config))
    } else {
        None
    };

    let db_addr = if is_dev {
        format!("127.0.0.1:{}", config.server.db_port)
    } else {
        format!("{}:{}", config.server.host, config.server.db_port)
    };

    let db_listener = match tokio::net::TcpListener::bind(&db_addr).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!(
                "PORT COLLISION: Failed to bind DB protocol to {}: {}",
                db_addr,
                e
            );
            eprintln!(
                "PORT COLLISION: Failed to bind DB protocol to {}: {}",
                db_addr, e
            );
            std::process::exit(1);
        }
    };

    info!("LIVEN listening on native wire scheme liven://{}", db_addr);

    let state_for_tcp = state.clone();
    let tcp_handle = tokio::spawn(async move {
        loop {
            match db_listener.accept().await {
                Ok((stream, _)) => {
                    let state_clone = state_for_tcp.clone();
                    let acceptor_opt = tls_acceptor.clone();

                    match state_clone
                        .engine
                        .conn_semaphore
                        .clone()
                        .acquire_owned()
                        .await
                    {
                        Ok(permit) => {
                            tokio::spawn(async move {
                                let _ = stream.set_nodelay(true);

                                if is_dev {
                                    let liven_stream = LivenStream::Cleartext(stream);
                                    let is_auth_key =
                                        state_clone.config.security.mode == "auth_key";
                                    let caps = if is_auth_key {
                                        crate::security::CAP_NONE
                                    } else {
                                        crate::security::CAP_ROOT
                                    };
                                    handle_connection(
                                        liven_stream,
                                        state_clone,
                                        caps,
                                        Vec::new(),
                                        None,
                                        is_auth_key,
                                        permit,
                                    )
                                    .await;
                                } else if let Some(acceptor) = acceptor_opt {
                                    match acceptor.accept(stream).await {
                                        Ok(tls_stream) => {
                                            let cn_opt = {
                                                let (_, session) = tls_stream.get_ref();
                                                session
                                                    .peer_certificates()
                                                    .and_then(|certs| certs.first())
                                                    .and_then(|cert| {
                                                        extract_cn_from_der(cert.as_ref())
                                                    })
                                            };

                                            if let Some(cn) = cn_opt {
                                                let identity_opt = {
                                                    let cache =
                                                        state_clone.identity_cache.read().unwrap();
                                                    cache.get(&cn).cloned()
                                                };

                                                let liven_stream =
                                                    LivenStream::Encrypted(tls_stream);
                                                if let Some(identity) = identity_opt {
                                                    let (close_tx, close_rx) =
                                                        tokio::sync::oneshot::channel::<()>();
                                                    {
                                                        let mut conns = state_clone
                                                            .active_connections
                                                            .lock()
                                                            .unwrap();
                                                        conns.push(ActiveNativeConnection {
                                                            auth_key_hash: identity
                                                                .auth_key_hash
                                                                .clone(),
                                                            close_tx: Some(close_tx),
                                                        });
                                                    }
                                                    handle_connection(
                                                        liven_stream,
                                                        state_clone,
                                                        identity.capabilities,
                                                        identity.allowed_tags,
                                                        Some(close_rx),
                                                        false,
                                                        permit,
                                                    )
                                                    .await;
                                                } else {
                                                    handle_connection(
                                                        liven_stream,
                                                        state_clone,
                                                        crate::security::CAP_NONE,
                                                        Vec::new(),
                                                        None,
                                                        false,
                                                        permit,
                                                    )
                                                    .await;
                                                }
                                            } else {
                                                tracing::error!(
                                                    "mTLS handshake succeeded but no client CN was extracted. Terminating stream."
                                                );
                                            }
                                        }
                                        Err(e) => {
                                            tracing::error!("mTLS handshake failed: {}", e);
                                        }
                                    }
                                } else {
                                    tracing::error!(
                                        "Production mode but TLS acceptor is missing. Terminating stream."
                                    );
                                }
                            });
                        }
                        Err(e) => {
                            tracing::error!("Failed to acquire connection semaphore permit: {}", e);
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("TCP accept error: {}", e);
                }
            }
        }
    });

    async fn handle_connection<S>(
        stream: S,
        state: Arc<ServerState>,
        mut capabilities: u8,
        allowed_tags: Vec<u32>,
        close_rx: Option<tokio::sync::oneshot::Receiver<()>>,
        is_auth_key_mode: bool,
        _permit: tokio::sync::OwnedSemaphorePermit,
    ) where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        let engine_client = state.engine.clone();
        let mut _keep_alive_tx = None;
        let mut close_rx_fused = match close_rx {
            Some(rx) => rx,
            None => {
                let (tx, rx) = tokio::sync::oneshot::channel();
                _keep_alive_tx = Some(tx);
                rx
            }
        };

        let framed = tokio_util::codec::Framed::new(stream, LivenCodec::default());
        let (mut writer, mut reader) = framed.split();

        // P12: Warn on production connections with no security
        if state.config.server.environment == "production" && state.config.security.mode == "none" {
            tracing::warn!(
                "🚨 Production connection accepted with authentication disabled! Set security.mode = auth_key in liven.toml immediately!"
            );
        }

        loop {
            tokio::select! {
                frame_res = reader.next() => {
                    match frame_res {
                        Some(Ok(frame)) => {
                            match frame {
                                crate::codec::LivenFrame::Connect { client_id, .. } => {
                                    if is_auth_key_mode {
                                        let hash = blake3::hash(client_id.as_bytes());
                                        let hash_hex = crate::security::hex_encode(hash.as_bytes());
                                        if let Some(auth_rec) = lookup_key_by_hash(&state.engine, &hash_hex) {
                                            if auth_rec.status == "active" {
                                                capabilities = crate::server::capabilities_for_role(&auth_rec.role);
                                                    let (close_tx, new_close_rx) = tokio::sync::oneshot::channel::<()>();
                                                {
                                                    let mut conns = state.active_connections.lock().unwrap();
                                                    conns.push(ActiveNativeConnection {
                                                        auth_key_hash: auth_rec.auth_key.clone(),
                                                        close_tx: Some(close_tx),
                                                    });
                                                }
                                                close_rx_fused = new_close_rx;
                                                _keep_alive_tx = None;

                                                if writer.send(crate::codec::LivenFrame::Ok).await.is_err() {
                                                    break;
                                                }
                                            } else {
                                                let _ = writer.send(crate::codec::LivenFrame::Err("Authentication failed: Key has been revoked".to_string())).await;
                                                break;
                                            }
                                        } else {
                                            let _ = writer.send(crate::codec::LivenFrame::Err("Authentication failed".to_string())).await;
                                            break;
                                        }
                                    } else if writer.send(LivenFrame::Ok).await.is_err() {
                                        break;
                                    }
                                }
                                crate::codec::LivenFrame::Query(query_str) => {
                                    if !check_query_capabilities(&query_str, capabilities) {
                                        let error_record = Record {
                                            sequence_id: 0,
                                            timestamp: 0,
                                            type_tag: 0,
                                            flags: 0,
                                            stream_name: "error".to_string(),
                                            key: crate::storage::key::StreamKey::from_str_truncated("error"),
                                            value: DataValue::String(
                                                "SECURITY ERROR: Insufficient capabilities for this operation".to_string(),
                                            ),
                                        };
                                        let _ = writer.send(crate::codec::LivenFrame::Records(vec![error_record])).await;
                                        continue;
                                    }

                                    let query_trimmed = query_str.trim();
                                    let is_listen = query_trimmed.ends_with(").listen()")
                                        || query_trimmed.ends_with("').listen()");

                                    if is_listen {
                                        // Strip .listen() suffix to get the pipeline query
                                        let pipeline_str = if query_trimmed.ends_with(").listen()") {
                                            &query_trimmed[..query_trimmed.len() - ").listen()".len()]
                                        } else {
                                            &query_trimmed[..query_trimmed.len() - "').listen()".len()]
                                        };
                                        let engine_listen = engine_client.clone();
                                        let pipeline_stages = match parse_pipeline(pipeline_str) {
                                            Ok(stages) => stages,
                                            Err(e) => {
                                                let _ = writer.send(crate::codec::LivenFrame::Err(format!("Listen query parse error: {}", e))).await;
                                                break;
                                            }
                                        };
                                        // Send historical snapshot first
                                        if let Ok(records) = execute_query(&engine_client, &Query::Listen { pipeline: pipeline_stages.clone() }) {
                                            let filtered: Vec<Record> = records.into_iter().filter(|rec| {
                                                allowed_tags.is_empty() || allowed_tags.contains(&(rec.type_tag as u32))
                                            }).collect();
                                            if !filtered.is_empty() {
                                                let _ = writer.send(crate::codec::LivenFrame::Records(filtered)).await;
                                            }
                                        }
                                        // Subscribe to live records matching the pipeline
                                        let mut rx = engine_client.subscribe();
                                        loop {
                                            tokio::select! {
                                                rec_res = rx.recv() => {
                                                    match rec_res {
                                                        Ok(record) => {
                                                            let mut vec = vec![record];
                                                            apply_pipeline_stages_to_vec(&mut vec, &engine_listen, &pipeline_stages);
                                                            if !vec.is_empty() {
                                                                let is_tag_allowed = allowed_tags.is_empty() || allowed_tags.contains(&(vec[0].type_tag as u32));
                                                                if is_tag_allowed
                                                                    && writer.send(crate::codec::LivenFrame::Records(vec)).await.is_err() {
                                                                        break;
                                                                    }
                                                            }
                                                        }
                                                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                                                        Err(_) => break,
                                                    }
                                                }
                                                _ = &mut close_rx_fused => {
                                                    break;
                                                }
                                            }
                                        }
                                        break;
                                    }

                                    let is_tail = (query_trimmed.starts_with("tail(\"") && query_trimmed.ends_with("\")"))
                                        || (query_trimmed.starts_with("tail('") && query_trimmed.ends_with("')"));

                                    if is_tail {
                                        let stream_name = if query_trimmed.starts_with("tail(\"") {
                                            query_trimmed["tail(\"".len()..query_trimmed.len() - "\")".len()].to_string()
                                        } else {
                                            query_trimmed["tail('".len()..query_trimmed.len() - "')".len()].to_string()
                                        };

                                        let mut rx = engine_client.subscribe();
                                        let mut is_closed = false;
                                        loop {
                                            tokio::select! {
                                                rec_res = rx.recv() => {
                                                    match rec_res {
                                                        Ok(record) => {
                                                            if record.stream_name == stream_name || stream_name == "*" {
                                                                let is_tag_allowed = allowed_tags.is_empty() || allowed_tags.contains(&(record.type_tag as u32));
                                                                if is_tag_allowed
                                                                    && writer.send(crate::codec::LivenFrame::Records(vec![record])).await.is_err() {
                                                                        break;
                                                                    }
                                                            }
                                                        }
                                                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                                                        Err(_) => break,
                                                    }
                                                }
                                                _ = &mut close_rx_fused => {
                                                    is_closed = true;
                                                    break;
                                                }
                                            }
                                        }
                                        if is_closed {
                                            break;
                                        }
                                    } else {
                                        let result = match parse_query(&query_str) {
                                            Ok(query) => match execute_query(&engine_client, &query) {
                                                Ok(records) => {
                                                    records.into_iter().filter(|rec| {
                                                        allowed_tags.is_empty() || allowed_tags.contains(&(rec.type_tag as u32))
                                                    }).collect()
                                                }
                                                Err(e) => {
                                                    vec![Record {
                                                        sequence_id: 0,
                                                        timestamp: 0,
                                                        type_tag: 0,
                                                        flags: 0,
                                                        stream_name: "error".to_string(),
                                                        key: crate::storage::key::StreamKey::from_str_truncated("error"),
                                                        value: DataValue::String(format!("Execution error: {}", e)),
                                                    }]
                                                }
                                            },
                                            Err(e) => {
                                                vec![Record {
                                                    sequence_id: 0,
                                                    timestamp: 0,
                                                    type_tag: 0,
                                                    flags: 0,
                                                    stream_name: "error".to_string(),
                                                    key: crate::storage::key::StreamKey::from_str_truncated("error"),
                                                    value: DataValue::String(format!("Parser error: {}", e)),
                                                }]
                                            }
                                        };
                                        if writer.send(crate::codec::LivenFrame::Records(result)).await.is_err() {
                                            break;
                                        }
                                    }
                                }
                                _ => {}
                            }
                        }
                        _ => break,
                    }
                }
                _ = &mut close_rx_fused => {
                    break;
                }
            }
        }
    }

    if run_ui {
        let ui_addr = format!("{}:{}", config.server.host, config.server.webui_port);
        let ui_listener = match tokio::net::TcpListener::bind(&ui_addr).await {
            Ok(l) => l,
            Err(e) => {
                tracing::error!(
                    "PORT COLLISION: Failed to bind Web UI to {}: {}",
                    ui_addr,
                    e
                );
                eprintln!(
                    "PORT COLLISION: Failed to bind Web UI to {}: {}",
                    ui_addr, e
                );
                std::process::exit(1);
            }
        };

        info!("Liven Web UI listening on http://{}", ui_addr);
        let ui_server = axum::serve(ui_listener, ui_app);

        tokio::select! {
            res = ui_server => {
                if let Err(e) = res {
                    return Err(LivenError::Internal(format!("UI server error: {}", e)));
                }
            }
            res = tcp_handle => {
                let Err(e) = res;
                return Err(LivenError::Internal(format!("TCP server error: {}", e)));
            }
        }
    } else {
        let Err(e) = tcp_handle.await;
        return Err(LivenError::Internal(format!("TCP server error: {}", e)));
    }

    Ok(())
}

/// Map a role string to its capability bitmask.
///
/// Used throughout the server to convert the string role stored in
/// `AuthKeyRecord` (and replicated in `SessionInfo`) into a bitmask
/// suitable for `check_query_capabilities`.
pub fn capabilities_for_role(role: &str) -> u8 {
    crate::security::capabilities_for_role(role)
}

pub fn check_query_capabilities(query_str: &str, capabilities: u8) -> bool {
    if capabilities == crate::security::CAP_ROOT {
        return true;
    }

    let query_trimmed = query_str.trim();
    let is_tail = (query_trimmed.starts_with("tail(\"") && query_trimmed.ends_with("\")"))
        || (query_trimmed.starts_with("tail('") && query_trimmed.ends_with("')"));

    if is_tail {
        return (capabilities & crate::security::CAP_READ) != 0;
    }

    match crate::parser::parse_query(query_str) {
        Ok(query) => match query {
            crate::types::Query::Insert { .. }
            | crate::types::Query::InsertBatch { .. }
            | crate::types::Query::Upsert { .. }
            | crate::types::Query::UpsertBatch { .. }
            | crate::types::Query::Update { .. }
            | crate::types::Query::PipelineUpdate { .. } => {
                (capabilities & crate::security::CAP_INSERT) != 0
            }
            crate::types::Query::DeleteKey { .. }
            | crate::types::Query::Empty { .. }
            | crate::types::Query::PipelineDelete { .. } => {
                (capabilities & crate::security::CAP_DELETE) != 0
            }
            crate::types::Query::Drop { .. } => {
                (capabilities & crate::security::CAP_ROOT) == crate::security::CAP_ROOT
            }
            crate::types::Query::ListStreams => (capabilities & crate::security::CAP_READ) != 0,
            crate::types::Query::Status => (capabilities & crate::security::CAP_READ) != 0,
            crate::types::Query::Pipeline(stages) => {
                let mut requires_delete = false;
                for stage in &stages {
                    match stage {
                        crate::types::PipelineStage::Delete
                        | crate::types::PipelineStage::Trash => {
                            requires_delete = true;
                            break;
                        }
                        _ => {}
                    }
                }
                if requires_delete {
                    (capabilities & crate::security::CAP_DELETE) != 0
                } else {
                    (capabilities & crate::security::CAP_READ) != 0
                }
            }
            crate::types::Query::Listen { .. } => (capabilities & crate::security::CAP_READ) != 0,
            crate::types::Query::Explain { .. } => (capabilities & crate::security::CAP_READ) != 0,
        },
        Err(_) => true,
    }
}

/// Validates that the request originates from the same host as the server.
/// This prevents external access to REST endpoints that are intended only
/// for the embedded Web UI.
fn is_request_from_ui(headers: &HeaderMap, config: &crate::config::ServerConfig) -> bool {
    let host_port = format!("{}:{}", config.host, config.webui_port);
    let localhost = format!("127.0.0.1:{}", config.webui_port);
    let localhost6 = format!("[::1]:{}", config.webui_port);
    let localhost_name = format!("localhost:{}", config.webui_port);

    // Check Host header
    if let Some(host) = headers.get(header::HOST).and_then(|h| h.to_str().ok())
        && (host == host_port || host == localhost || host == localhost6 || host == localhost_name)
    {
        return true;
    }

    // Check Origin header (set by browsers)
    if let Some(origin) = headers.get(header::ORIGIN).and_then(|h| h.to_str().ok()) {
        let origin_clean = origin
            .trim_start_matches("http://")
            .trim_start_matches("https://");
        if origin_clean == host_port
            || origin_clean == localhost
            || origin_clean == localhost6
            || origin_clean == localhost_name
        {
            return true;
        }
    }

    // Check Referer header (fallback)
    if let Some(referer) = headers.get(header::REFERER).and_then(|h| h.to_str().ok())
        && let Some(after) = referer.split("://").nth(1)
    {
        let authority = after.split('/').next().unwrap_or("");
        if authority == host_port
            || authority == localhost
            || authority == localhost6
            || authority == localhost_name
        {
            return true;
        }
    }

    false
}

// WebSocket handler
#[derive(Deserialize)]
struct WsParams {
    token: Option<String>,
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<ServerState>>,
    AxumQuery(params): AxumQuery<WsParams>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if state.config.security.mode == "auth_key" {
        let mut authorized = if let Some(token) = params.token {
            let sessions = state.sessions.lock().unwrap();
            sessions.contains_key(&token)
        } else {
            false
        };

        if !authorized && let Some(cookie_val) = extract_session_cookie(&headers) {
            let sessions = state.sessions.lock().unwrap();
            if sessions.contains_key(&cookie_val) {
                authorized = true;
            }
        }

        if !authorized {
            return (StatusCode::UNAUTHORIZED, "Unauthorized").into_response();
        }
    }

    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

#[derive(Deserialize)]
#[serde(tag = "type")]
enum WsRequest {
    #[serde(rename = "query")]
    Query { query: String },
    #[serde(rename = "ping")]
    Ping,
}

#[derive(Serialize)]
#[serde(tag = "type")]
enum WsResponse {
    #[serde(rename = "query_result")]
    QueryResult { data: Record },
    #[serde(rename = "metrics")]
    Metrics {
        ram_usage: u64,
        disk_size: u64,
        segments: u64,
        sequence_id: u64,
        key_count: usize,
        total_streams: usize,
    },
    #[serde(rename = "pong")]
    Pong,
    #[serde(rename = "error")]
    Error { message: String },
}

async fn handle_socket(socket: WebSocket, state: Arc<ServerState>) {
    let (sender, mut receiver) = socket.split();
    let sender = Arc::new(tokio::sync::Mutex::new(sender));
    let sender_for_stats = sender.clone();
    let sender_for_query = sender.clone();
    let engine_metrics = state.engine.clone();

    // Spawn stats broadcast loop
    let mut stats_interval = tokio::time::interval(Duration::from_millis(1000));
    let (stats_cancel_tx, mut stats_cancel_rx) = tokio::sync::oneshot::channel::<()>();

    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = stats_interval.tick() => {
                    let metrics = engine_metrics.metrics().unwrap_or((0, 0, 0, 0));
                    let next_seq = engine_metrics.next_sequence_id();
                    let keys = engine_metrics.skipmap.len();

                    let resp = WsResponse::Metrics {
                        ram_usage: metrics.0,
                        disk_size: metrics.1,
                        segments: metrics.2,
                        sequence_id: next_seq,
                        key_count: keys,
                        total_streams: metrics.3,
                    };

                    if let Ok(msg_str) = serde_json::to_string(&resp) {
                        let mut sender_guard = sender_for_stats.lock().await;
                        if sender_guard.send(Message::Text(msg_str)).await.is_err() {
                            break;
                        }
                    }
                }
                _ = &mut stats_cancel_rx => {
                    break;
                }
            }
        }
    });

    let (query_tx, mut query_rx) = tokio::sync::mpsc::channel::<String>(5);
    let engine_query = state.engine.clone();

    tokio::spawn(async move {
        while let Some(query_str) = query_rx.recv().await {
            let query_trimmed = query_str.trim();
            let is_tail = (query_trimmed.starts_with("tail(\"") && query_trimmed.ends_with("\")"))
                || (query_trimmed.starts_with("tail('") && query_trimmed.ends_with("')"));

            if is_tail {
                let stream_name = if query_trimmed.starts_with("tail(\"") {
                    query_trimmed["tail(\"".len()..query_trimmed.len() - "\")".len()].to_string()
                } else {
                    query_trimmed["tail('".len()..query_trimmed.len() - "')".len()].to_string()
                };

                let mut rx = engine_query.subscribe();
                loop {
                    match rx.recv().await {
                        Ok(record) => {
                            if record.stream_name == stream_name || stream_name == "*" {
                                let resp = WsResponse::QueryResult { data: record };
                                if let Ok(msg_str) = serde_json::to_string(&resp) {
                                    let mut sender_guard = sender_for_query.lock().await;
                                    if sender_guard.send(Message::Text(msg_str)).await.is_err() {
                                        break;
                                    }
                                }
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(_) => break,
                    }
                }
            } else {
                match parse_query(&query_str) {
                    Ok(Query::Listen { pipeline }) => {
                        let stream = execute_query_stream(engine_query.clone(), pipeline);
                        let mut stream = Box::pin(stream);
                        while let Some(record) = stream.next().await {
                            let resp = WsResponse::QueryResult { data: record };
                            if let Ok(msg_str) = serde_json::to_string(&resp) {
                                let mut sender_guard = sender_for_query.lock().await;
                                if sender_guard.send(Message::Text(msg_str)).await.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                    Ok(_) => match parse_pipeline(&query_str) {
                        Ok(stages) => {
                            let stream = execute_query_stream(engine_query.clone(), stages);
                            let mut stream = Box::pin(stream);
                            while let Some(record) = stream.next().await {
                                let resp = WsResponse::QueryResult { data: record };
                                if let Ok(msg_str) = serde_json::to_string(&resp) {
                                    let mut sender_guard = sender_for_query.lock().await;
                                    if sender_guard.send(Message::Text(msg_str)).await.is_err() {
                                        break;
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            let resp = WsResponse::Error { message: e };
                            if let Ok(msg_str) = serde_json::to_string(&resp) {
                                let mut sender_guard = sender_for_query.lock().await;
                                let _ = sender_guard.send(Message::Text(msg_str)).await;
                            }
                        }
                    },
                    Err(e) => {
                        let resp = WsResponse::Error { message: e };
                        if let Ok(msg_str) = serde_json::to_string(&resp) {
                            let mut sender_guard = sender_for_query.lock().await;
                            let _ = sender_guard.send(Message::Text(msg_str)).await;
                        }
                    }
                }
            }
        }
    });

    while let Some(Ok(msg)) = receiver.next().await {
        if let Message::Text(txt) = msg
            && let Ok(req) = serde_json::from_str::<WsRequest>(&txt)
        {
            match req {
                WsRequest::Query { query } => {
                    let _ = query_tx.send(query).await;
                }
                WsRequest::Ping => {
                    let resp = WsResponse::Pong;
                    if let Ok(msg_str) = serde_json::to_string(&resp) {
                        let mut sender_guard = sender.lock().await;
                        if sender_guard.send(Message::Text(msg_str)).await.is_err() {
                            break;
                        }
                    }
                }
            }
        }
    }

    let _ = stats_cancel_tx.send(());
}

// REST endpoints
#[derive(Deserialize)]
struct RawIngestRecord {
    stream: String,
    key: String,
    value: serde_json::Value,
}

fn json_to_datavalue(val: serde_json::Value) -> DataValue {
    match val {
        serde_json::Value::Null => DataValue::Null,
        serde_json::Value::Bool(b) => DataValue::Bool(b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                DataValue::Int(i)
            } else if let Some(u) = n.as_u64() {
                DataValue::UInt(u)
            } else if let Some(f) = n.as_f64() {
                DataValue::Float(ordered_float::OrderedFloat(f))
            } else {
                DataValue::Null
            }
        }
        serde_json::Value::String(s) => DataValue::String(s),
        serde_json::Value::Array(arr) => {
            DataValue::String(serde_json::Value::Array(arr).to_string())
        }
        serde_json::Value::Object(obj) => {
            DataValue::String(serde_json::Value::Object(obj).to_string())
        }
    }
}

fn extract_session_cookie(headers: &HeaderMap) -> Option<String> {
    let cookie_header = headers.get(header::COOKIE)?.to_str().ok()?;
    for cookie in cookie_header.split(';') {
        let parts: Vec<&str> = cookie.split('=').collect();
        if parts.len() == 2 && parts[0].trim() == "liven_session" {
            return Some(parts[1].trim().to_string());
        }
    }
    None
}

pub fn validate_session_and_slide(
    state: &ServerState,
    headers: &HeaderMap,
) -> Result<(String, String, HeaderMap), StatusCode> {
    if state.config.security.mode == "none" {
        return Ok(("admin".to_string(), "admin".to_string(), HeaderMap::new()));
    }

    let session_id = if let Some(cookie_val) = extract_session_cookie(headers) {
        cookie_val
    } else if let Some(auth_header) = headers
        .get(header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
    {
        let token = if auth_header.starts_with("Bearer ") {
            &auth_header["Bearer ".len()..]
        } else {
            auth_header
        };
        token.trim().to_string()
    } else {
        return Err(StatusCode::UNAUTHORIZED);
    };

    let mut sessions = state.sessions.lock().unwrap();
    if let Some(session) = sessions.get_mut(&session_id) {
        // 30 minutes idle timeout (1800 seconds)
        if session.last_seen.elapsed() > Duration::from_secs(1800) {
            sessions.remove(&session_id);
            return Err(StatusCode::UNAUTHORIZED);
        }

        // 8 hours absolute timeout (28800 seconds)
        if session.last_seen.elapsed() > Duration::from_secs(28800) {
            sessions.remove(&session_id);
            return Err(StatusCode::UNAUTHORIZED);
        }

        session.last_seen = std::time::Instant::now();
        let user_id = session.user_id.clone();
        let role = session.role.clone();

        let mut response_headers = HeaderMap::new();
        let cookie = format!(
            "liven_session={}; HttpOnly; SameSite=Lax; Max-Age=28800; Path=/",
            session_id
        );
        if let Ok(val) = header::HeaderValue::from_str(&cookie) {
            response_headers.insert(header::SET_COOKIE, val);
        }
        Ok((user_id, role, response_headers))
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
}

#[derive(Deserialize)]
struct SystemLoginRequest {
    token: String,
}

async fn system_auth_login_handler(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Json(payload): Json<SystemLoginRequest>,
) -> Response {
    if !is_request_from_ui(&headers, &state.config.server) {
        return StatusCode::NOT_FOUND.into_response();
    }
    let token = payload.token.trim();
    if token.is_empty() {
        return (StatusCode::BAD_REQUEST, "Authorization key cannot be empty").into_response();
    }

    // Compute BLAKE3 hash of the pasted raw key
    let hash = blake3::hash(token.as_bytes());
    let hash_hex = crate::security::hex_encode(hash.as_bytes());

    // Look up key in `auth_keys` table/stream
    let opt_record = if state.config.security.mode == "none" {
        // In "none" mode, mock a default root admin record
        Some(AuthKeyRecord {
            key_id: "bypass_admin".to_string(),
            role: "admin".to_string(),
            auth_key: hash_hex.clone(),
            status: "active".to_string(),
            allowed_tags: Vec::new(),
        })
    } else {
        lookup_key_by_hash(&state.engine, &hash_hex)
    };

    match opt_record {
        Some(auth_rec) => {
            if auth_rec.status != "active" {
                return (
                    StatusCode::UNAUTHORIZED,
                    "Authentication failed: This key has been revoked.",
                )
                    .into_response();
            }

            let user_id = auth_rec.key_id.clone();
            let role = auth_rec.role.clone();
            let permissions = if role == "admin" {
                "root".to_string()
            } else {
                role.clone()
            };

            // Generate a secure random session ID
            let mut session_bytes = [0u8; 32];
            rand::RngCore::fill_bytes(&mut rand::rngs::OsRng, &mut session_bytes);
            let session_id = crate::security::hex_encode(&session_bytes);

            {
                let mut sessions = state.sessions.lock().unwrap();
                sessions.insert(
                    session_id.clone(),
                    SessionInfo {
                        user_id: user_id.clone(),
                        role: role.clone(),
                        last_seen: std::time::Instant::now(),
                    },
                );
            }

            let cookie = format!(
                "liven_session={}; HttpOnly; SameSite=Lax; Max-Age=28800; Path=/",
                session_id
            );

            Response::builder()
                .status(StatusCode::OK)
                .header(header::SET_COOKIE, cookie)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "status": "success",
                        "user_id": user_id,
                        "permissions": permissions,
                    })
                    .to_string(),
                ))
                .unwrap()
        }
        None => (
            StatusCode::UNAUTHORIZED,
            "Authentication failed: Invalid administrative auth key.",
        )
            .into_response(),
    }
}

#[derive(Serialize)]
struct SystemStatusResponse {
    authenticated: bool,
    user_id: Option<String>,
    role: Option<String>,
}

async fn system_auth_status_handler(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if !is_request_from_ui(&headers, &state.config.server) {
        return StatusCode::NOT_FOUND.into_response();
    }
    if let Some(cookie_val) = extract_session_cookie(&headers) {
        let mut sessions = state.sessions.lock().unwrap();
        if let Some(session) = sessions.get_mut(&cookie_val) {
            if session.last_seen.elapsed() > Duration::from_secs(300) {
                sessions.remove(&cookie_val);
            } else {
                session.last_seen = std::time::Instant::now();
                let user_id = session.user_id.clone();

                // Look up the user's role from the identity cache
                let role = state
                    .identity_cache
                    .read()
                    .unwrap()
                    .get(&user_id)
                    .map(|identity| {
                        if identity.capabilities == crate::security::CAP_ROOT {
                            "admin".to_string()
                        } else if (identity.capabilities & crate::security::CAP_DELETE) != 0 {
                            "write-delete".to_string()
                        } else if (identity.capabilities & crate::security::CAP_INSERT) != 0 {
                            "write".to_string()
                        } else if (identity.capabilities & crate::security::CAP_READ) != 0 {
                            "read-only".to_string()
                        } else {
                            "unknown".to_string()
                        }
                    });

                let cookie = format!(
                    "liven_session={}; HttpOnly; SameSite=Lax; Max-Age=300; Path=/",
                    cookie_val
                );
                return Response::builder()
                    .status(StatusCode::OK)
                    .header(header::SET_COOKIE, cookie)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!(SystemStatusResponse {
                            authenticated: true,
                            user_id: Some(user_id.clone()),
                            role,
                        })
                        .to_string(),
                    ))
                    .unwrap();
            }
        }
    }

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::json!(SystemStatusResponse {
                authenticated: false,
                user_id: None,
                role: None,
            })
            .to_string(),
        ))
        .unwrap()
}

async fn system_auth_logout_handler(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Some(cookie_val) = extract_session_cookie(&headers) {
        let mut sessions = state.sessions.lock().unwrap();
        sessions.remove(&cookie_val);
    }

    let cookie = "liven_session=; HttpOnly; SameSite=Lax; Max-Age=0; Path=/";

    Response::builder()
        .status(StatusCode::OK)
        .header(header::SET_COOKIE, cookie)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::json!({
                "status": "success",
                "message": "Logged out successfully"
            })
            .to_string(),
        ))
        .unwrap()
}

#[derive(Deserialize)]
struct GenerateKeyRequest {
    key_id: String,
    role: String, // 'admin', 'read-only', 'write-only'
}

#[derive(Serialize)]
struct GenerateKeyResponse {
    key_id: String,
    role: String,
    auth_key_hash: String,
    status: String,
    raw_key: String, // shown exactly once!
}

#[derive(Deserialize)]
struct RevokeKeyRequest {
    key_id: String,
}

#[derive(Deserialize)]
struct UpdateRoleRequest {
    key_id: String,
    role: String,
}

async fn system_auth_list_keys_handler(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let (_user_id, role, slide_headers) = match validate_session_and_slide(&state, &headers) {
        Ok(res) => res,
        Err(status) => return status.into_response(),
    };

    if role != "admin" {
        return (StatusCode::FORBIDDEN, "Forbidden: Admin role required").into_response();
    }

    let keys = state.engine.list_keys("auth_keys");
    let mut records = Vec::new();
    for k in keys {
        if let Ok(Some(rec)) = state.engine.get("auth_keys", &k)
            && let DataValue::String(json_str) = rec.value
            && let Ok(auth_rec) = serde_json::from_str::<AuthKeyRecord>(&json_str)
        {
            records.push(auth_rec);
        }
    }

    (StatusCode::OK, slide_headers, Json(records)).into_response()
}

async fn system_auth_generate_key_handler(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Json(payload): Json<GenerateKeyRequest>,
) -> impl IntoResponse {
    let (_user_id, role, slide_headers) = match validate_session_and_slide(&state, &headers) {
        Ok(res) => res,
        Err(status) => return status.into_response(),
    };

    if role != "admin" {
        return (StatusCode::FORBIDDEN, "Forbidden: Admin role required").into_response();
    }

    let payload_role = payload.role.trim().to_lowercase();
    if payload_role != "admin"
        && payload_role != "read-only"
        && payload_role != "write"
        && payload_role != "write-delete"
    {
        return (StatusCode::BAD_REQUEST, "Invalid role").into_response();
    }

    let trimmed_id = payload.key_id.trim();
    if trimmed_id.is_empty() {
        return (StatusCode::BAD_REQUEST, "Key ID cannot be empty").into_response();
    }

    // Generate cryptographically secure 256-bit (32-byte) key
    let mut raw_key_bytes = [0u8; 32];
    rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut raw_key_bytes);
    let raw_key = crate::security::hex_encode(&raw_key_bytes);

    let hash = blake3::hash(raw_key.as_bytes());
    let hash_hex = crate::security::hex_encode(hash.as_bytes());

    let auth_rec = AuthKeyRecord {
        key_id: trimmed_id.to_string(),
        role: payload_role.clone(),
        auth_key: hash_hex.clone(),
        status: "active".to_string(),
        allowed_tags: Vec::new(),
    };

    let json_val = match serde_json::to_string(&auth_rec) {
        Ok(s) => s,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };

    if let Err(e) = state.engine.append(
        "auth_keys",
        trimmed_id,
        DataValue::String(json_val),
        false, // do not overwrite if exists
    ) {
        return (
            StatusCode::BAD_REQUEST,
            format!("Key ID already exists or storage error: {}", e),
        )
            .into_response();
    }

    // Write-through to identity_cache
    {
        let capabilities = crate::server::capabilities_for_role(&payload_role);
        let mut cache = state.identity_cache.write().unwrap();
        cache.insert(
            trimmed_id.to_string(),
            ClientIdentity {
                client_cn: trimmed_id.to_string(),
                capabilities,
                allowed_tags: auth_rec.allowed_tags.clone(),
                auth_key_hash: hash_hex.clone(),
            },
        );
    }

    let response = GenerateKeyResponse {
        key_id: trimmed_id.to_string(),
        role: payload_role,
        auth_key_hash: hash_hex,
        status: "active".to_string(),
        raw_key,
    };

    (StatusCode::OK, slide_headers, Json(response)).into_response()
}

async fn system_auth_revoke_key_handler(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Json(payload): Json<RevokeKeyRequest>,
) -> impl IntoResponse {
    let (_user_id, role, slide_headers) = match validate_session_and_slide(&state, &headers) {
        Ok(res) => res,
        Err(status) => return status.into_response(),
    };

    if role != "admin" {
        return (StatusCode::FORBIDDEN, "Forbidden: Admin role required").into_response();
    }

    let trimmed_id = payload.key_id.trim();
    if trimmed_id.is_empty() {
        return (StatusCode::BAD_REQUEST, "Key ID cannot be empty").into_response();
    }

    // Get current record
    let opt_record = match state.engine.get("auth_keys", trimmed_id) {
        Ok(Some(rec)) => {
            if let DataValue::String(json_str) = rec.value {
                serde_json::from_str::<AuthKeyRecord>(&json_str).ok()
            } else {
                None
            }
        }
        _ => None,
    };

    let mut auth_rec = match opt_record {
        Some(rec) => rec,
        None => return (StatusCode::NOT_FOUND, "Key not found").into_response(),
    };

    auth_rec.status = "revoked".to_string();

    let json_val = match serde_json::to_string(&auth_rec) {
        Ok(s) => s,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };

    if let Err(e) = state.engine.append(
        "auth_keys",
        trimmed_id,
        DataValue::String(json_val),
        false, // update existing key (do not delete as tombstone)
    ) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to update storage: {}", e),
        )
            .into_response();
    }

    // Evict from identity_cache
    {
        let mut cache = state.identity_cache.write().unwrap();
        cache.remove(trimmed_id);
    }

    // Trigger real-time severing of TCP streams
    kill_active_connections_for_hash(&state, &auth_rec.auth_key);

    (
        StatusCode::OK,
        slide_headers,
        Json(serde_json::json!({ "status": "success" })),
    )
        .into_response()
}

async fn system_auth_update_role_handler(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Json(payload): Json<UpdateRoleRequest>,
) -> impl IntoResponse {
    let (_user_id, role, slide_headers) = match validate_session_and_slide(&state, &headers) {
        Ok(res) => res,
        Err(status) => return status.into_response(),
    };
    if role != "admin" {
        return (StatusCode::FORBIDDEN, "Forbidden: Admin role required").into_response();
    }

    let new_role = payload.role.trim().to_lowercase();
    if !["admin", "read-only", "write", "write-delete"].contains(&new_role.as_str()) {
        return (StatusCode::BAD_REQUEST, "Invalid role").into_response();
    }

    let trimmed_id = payload.key_id.trim();
    if trimmed_id.is_empty() {
        return (StatusCode::BAD_REQUEST, "Key ID cannot be empty").into_response();
    }

    // Fetch existing record — same lookup pattern as revoke handler
    let opt_record = match state.engine.get("auth_keys", trimmed_id) {
        Ok(Some(rec)) => {
            if let DataValue::String(json_str) = rec.value {
                serde_json::from_str::<AuthKeyRecord>(&json_str).ok()
            } else {
                None
            }
        }
        _ => None,
    };

    let mut auth_rec = match opt_record {
        Some(rec) => rec,
        None => return (StatusCode::NOT_FOUND, "Key not found").into_response(),
    };

    if auth_rec.role == new_role {
        return (
            StatusCode::OK,
            slide_headers,
            Json(serde_json::json!({
                "status": "success",
                "message": "Role unchanged"
            })),
        )
            .into_response();
    }

    let old_hash = auth_rec.auth_key.clone();
    auth_rec.role = new_role.clone();

    let json_val = match serde_json::to_string(&auth_rec) {
        Ok(s) => s,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };

    if let Err(e) = state
        .engine
        .append("auth_keys", trimmed_id, DataValue::String(json_val), false)
    {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to update storage: {}", e),
        )
            .into_response();
    }

    // Update identity_cache in place with new capabilities
    {
        let mut cache = state.identity_cache.write().unwrap();
        if let Some(identity) = cache.get_mut(trimmed_id) {
            identity.capabilities = crate::security::capabilities_for_role(&new_role);
        }
    }

    // Evict any cached REST session tied to this key_id
    {
        let mut sessions = state.sessions.lock().unwrap();
        sessions.retain(|_, s| s.user_id != trimmed_id);
    }

    // Force-disconnect any live native TCP connection using the OLD capabilities
    kill_active_connections_for_hash(&state, &old_hash);

    (
        StatusCode::OK,
        slide_headers,
        Json(serde_json::json!({
            "status": "success",
            "key_id": trimmed_id,
            "new_role": new_role
        })),
    )
        .into_response()
}

async fn ingest_handler(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let (_user_id, role, slide_headers) = match validate_session_and_slide(&state, &headers) {
        Ok(res) => res,
        Err(status) => return status.into_response(),
    };

    let capabilities = crate::server::capabilities_for_role(&role);
    if capabilities & crate::security::CAP_INSERT == 0 {
        return (
            StatusCode::FORBIDDEN,
            slide_headers,
            "Insufficient capabilities: write access required",
        )
            .into_response();
    }

    // Server only supports JSONL format for ingest (not MessagePack)
    let items: Vec<(String, String, DataValue)> =
        match serde_json::from_slice::<Vec<RawIngestRecord>>(&body) {
            Ok(recs) => rcs_to_batch(recs),
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    slide_headers,
                    format!("JSON error: {}", e),
                )
                    .into_response();
            }
        };

    if items.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            slide_headers,
            "Empty ingestion batch",
        )
            .into_response();
    }

    match state.engine.append_batch(items) {
        Ok(recs) => (StatusCode::OK, slide_headers, Json(recs)).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            slide_headers,
            format!("Storage error: {}", e),
        )
            .into_response(),
    }
}

fn rcs_to_batch(recs: Vec<RawIngestRecord>) -> Vec<(String, String, DataValue)> {
    recs.into_iter()
        .map(|r| (r.stream, r.key, json_to_datavalue(r.value)))
        .collect()
}

#[derive(Deserialize)]
struct QueryRequest {
    query: String,
}

async fn query_handler(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Json(payload): Json<QueryRequest>,
) -> impl IntoResponse {
    let (_user_id, role, slide_headers) = match validate_session_and_slide(&state, &headers) {
        Ok(res) => res,
        Err(status) => return status.into_response(),
    };

    let capabilities = crate::server::capabilities_for_role(&role);
    if !check_query_capabilities(&payload.query, capabilities) {
        return (
            StatusCode::FORBIDDEN,
            slide_headers,
            "Insufficient capabilities for this query",
        )
            .into_response();
    }

    // Server only supports JSON format (accept header ignored)

    match parse_query(&payload.query) {
        Ok(query) => match execute_query(&state.engine, &query) {
            Ok(results) => {
                let mut export_format = None;
                if let Query::Pipeline(ref stages) = query {
                    for stage in stages {
                        if let PipelineStage::Export { format } = stage {
                            export_format = Some(*format);
                            break;
                        }
                    }
                }

                if let Some(fmt) = export_format {
                    // Server only supports JSONL format for exports
                    if fmt != ExportFormat::Jsonl {
                        return (
                            StatusCode::BAD_REQUEST,
                            slide_headers.clone(),
                            format!(
                                "Server only supports JSONL export format, but got: {:?}",
                                fmt
                            ),
                        )
                            .into_response();
                    }

                    let mut out = String::new();
                    for r in results {
                        if let Ok(s) = serde_json::to_string(&r) {
                            out.push_str(&s);
                            out.push('\n');
                        }
                    }
                    let mut res_headers = slide_headers.clone();
                    res_headers.insert(
                        header::CONTENT_TYPE,
                        header::HeaderValue::from_static("text/plain"),
                    );
                    return (StatusCode::OK, res_headers, out).into_response();
                }

                (StatusCode::OK, slide_headers, Json(results)).into_response()
            }
            Err(e) => (
                StatusCode::BAD_REQUEST,
                slide_headers,
                format!("Query execution error: {}", e),
            )
                .into_response(),
        },
        Err(e) => (
            StatusCode::BAD_REQUEST,
            slide_headers,
            format!("Parser error: {}", e),
        )
            .into_response(),
    }
}

async fn list_streams_handler(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let (_user_id, role, slide_headers) = match validate_session_and_slide(&state, &headers) {
        Ok(res) => res,
        Err(status) => return status.into_response(),
    };

    let capabilities = crate::server::capabilities_for_role(&role);
    if capabilities & crate::security::CAP_READ == 0 {
        return (
            StatusCode::FORBIDDEN,
            slide_headers,
            "Insufficient capabilities: read access required",
        )
            .into_response();
    }

    let streams = state.engine.list_streams();
    (StatusCode::OK, slide_headers, Json(streams)).into_response()
}

async fn health_handler(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if !is_request_from_ui(&headers, &state.config.server) {
        return StatusCode::NOT_FOUND.into_response();
    }
    (
        StatusCode::OK,
        Json(serde_json::json!({ "status": "healthy" })),
    )
        .into_response()
}

async fn heartbeat_handler(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    // Validate the session and slide the timeout
    match validate_session_and_slide(&state, &headers) {
        Ok((_user_id, _role, slide_headers)) => {
            (
                StatusCode::OK,
                slide_headers,
                Json(serde_json::json!({ "status": "heartbeat_received", "message": "Session kept alive" })),
            )
                .into_response()
        }
        Err(status) => status.into_response(),
    }
}

async fn static_handler(uri: axum::http::Uri) -> impl IntoResponse {
    let mut path = uri.path().trim_start_matches('/').to_string();

    if path.is_empty() {
        path = "index.html".to_string();
    }

    match Assets::get(&path) {
        Some(content) => {
            let mime = mime_guess::from_path(&path).first_or_octet_stream();
            Response::builder()
                .header(header::CONTENT_TYPE, mime.as_ref())
                .body(Body::from(content.data))
                .unwrap()
        }
        None => match Assets::get("index.html") {
            Some(content) => Response::builder()
                .header(header::CONTENT_TYPE, "text/html")
                .body(Body::from(content.data))
                .unwrap(),
            None => Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::from("404 Not Found"))
                .unwrap(),
        },
    }
}

async fn system_config_handler(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    // Allow UI requests to fetch config without authentication
    // This is necessary for the UI to determine the security mode before authenticating
    if !is_request_from_ui(&headers, &state.config.server) {
        return StatusCode::NOT_FOUND.into_response();
    }

    let config = state.config.clone();

    // Return only essential configuration that UI needs (no sensitive data)
    let response = serde_json::json!({
        "webui_port": config.server.webui_port,
        "db_port": config.server.db_port,
        "security": {
            "mode": config.security.mode
        }
    });

    (StatusCode::OK, Json(response)).into_response()
}
