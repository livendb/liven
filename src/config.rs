use config::{Config, ConfigError, Environment, File};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Auto-detect system RAM and calculate appropriate budget
fn auto_detect_index_ram_budget() -> u64 {
    // Try to detect system RAM
    if let Some(system_ram) = crate::sysinfo::detect_system_ram_mb() {
        let budget = crate::sysinfo::calculate_auto_budget(system_ram);
        let capacity = crate::sysinfo::estimate_key_capacity(budget);

        tracing::info!(
            "Index RAM budget (charged estimate): {}MB (auto, 25% of {}MB) — ~{}M keys at ~{} B/key (compound key + 128)",
            budget,
            system_ram,
            capacity / 1_000_000,
            crate::sysinfo::ESTIMATE_BYTES_PER_KEY
        );
        return budget;
    }

    // Fallback if detection fails
    let fallback = 512; // 512MB fallback
    let capacity = crate::sysinfo::estimate_key_capacity(fallback);
    tracing::warn!(
        "System RAM detection failed, using fallback: {}MB — charged-budget estimate ~{}M keys at ~{} B/key",
        fallback,
        capacity / 1_000_000,
        crate::sysinfo::ESTIMATE_BYTES_PER_KEY
    );
    fallback
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ServerConfig {
    pub environment: String,
    pub host: String,
    pub db_port: u16,
    pub webui_port: u16,
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,
    #[serde(default = "default_broadcast_capacity")]
    pub broadcast_capacity: usize,
}

fn default_version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

fn default_max_connections() -> usize {
    10000
}

fn default_broadcast_capacity() -> usize {
    4096
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StorageConfig {
    pub data_directory: String,
    pub max_segment_size_mb: usize,
    #[serde(default = "default_sync_mode")]
    pub sync_mode: String,
}

fn default_sync_mode() -> String {
    "always".to_string()
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LimitsConfig {
    pub max_concurrent_streams: usize,
    pub max_open_file_descriptors: usize,
    pub max_index_ram_mb: usize,
    pub max_segment_size_mb: usize,
    #[serde(default = "default_max_scan_results")]
    pub max_scan_results: usize,
    /// Usage percentage at which the index high-water metric flips.
    #[serde(default = "default_index_high_water_percent")]
    pub index_high_water_percent: u8,
}

fn default_limits() -> LimitsConfig {
    LimitsConfig {
        max_concurrent_streams: 32,
        max_open_file_descriptors: 64,
        max_index_ram_mb: 16,
        max_segment_size_mb: 16,
        max_scan_results: 100_000,
        index_high_water_percent: 90,
    }
}

fn default_max_scan_results() -> usize {
    100_000
}

fn default_index_high_water_percent() -> u8 {
    90
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AuthKeyConfig {
    pub system_stream: String,
    pub allow_local_auto_generation: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ZtnaConfig {
    #[serde(default = "default_ztna_enabled")]
    pub enabled: bool,
    pub cert_path: Option<String>,
    pub key_path: Option<String>,
    pub client_ca_path: Option<String>,
}

fn default_ztna_enabled() -> bool {
    false
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SecurityConfig {
    pub mode: String,
    pub auth_key: Option<AuthKeyConfig>,
    pub master_key: Option<String>,
    pub ztna: Option<ZtnaConfig>,
}

/// Default security configuration for embedded mode.
/// This is used by AppConfig::from_embedded() where authentication
/// is not applicable (e.g., embedded library usage).
/// Production server mode should use AppConfig::load() which defaults
/// to auth_key mode.
fn embedded_security_default() -> SecurityConfig {
    SecurityConfig {
        mode: "none".to_string(),
        auth_key: None,
        master_key: None,
        ztna: None,
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RetentionConfig {
    #[serde(default)]
    pub default_max_age_secs: u64,
    #[serde(default = "default_retention_check_secs")]
    pub check_interval_secs: u64,
    /// Per-stream overrides, in seconds.
    #[serde(default)]
    pub streams: HashMap<String, u64>,
}

fn default_retention_check_secs() -> u64 {
    60
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            default_max_age_secs: 0,
            check_interval_secs: 60,
            streams: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AppConfig {
    #[serde(default = "default_version")]
    pub version: String,
    pub server: ServerConfig,
    pub storage: StorageConfig,
    #[serde(default = "default_limits")]
    pub limits: LimitsConfig,
    #[serde(default = "embedded_security_default")]
    pub security: SecurityConfig,
    #[serde(default)]
    pub retention: RetentionConfig,
}

impl AppConfig {
    /// Loads config from the default liven.toml file path.
    /// Used by the server binary — reads liven.toml, liven.conf,
    /// and environment variables.
    pub fn load() -> Result<Self, ConfigError> {
        Self::load_from(None)
    }

    /// Loads configuration from an explicit file (highest file priority),
    /// falling back to `./liven.toml` and `./liven.conf`, then environment
    /// variables. `LIVEN_CONFIG` supplies the explicit path when `path` is
    /// `None`. Used by `liven start --config <path>`.
    pub fn load_from(path: Option<&std::path::Path>) -> Result<Self, ConfigError> {
        let env_path = std::env::var("LIVEN_CONFIG")
            .ok()
            .filter(|p| !p.trim().is_empty())
            .map(std::path::PathBuf::from);
        let explicit_path = path.map(|p| p.to_path_buf()).or(env_path);
        let path = explicit_path.as_deref();

        let mut builder = Config::builder()
            // Layer 1: Hardcoded internal defaults
            .set_default("server.environment", "development")?
            .set_default("server.host", "127.0.0.1")?
            .set_default("server.db_port", 43121)?
            .set_default("server.webui_port", 43120)?
            .set_default("server.max_connections", 10000)?
            .set_default("server.broadcast_capacity", 4096)?
            .set_default("storage.data_directory", "./data")?
            .set_default("storage.max_segment_size_mb", 10)?
            .set_default("storage.sync_mode", "always")?
            .set_default("limits.max_concurrent_streams", 32)?
            .set_default("limits.max_open_file_descriptors", 64)?
            .set_default("limits.max_index_ram_mb", 16)?
            .set_default("limits.max_segment_size_mb", 16)?
            .set_default("limits.max_scan_results", 100_000)?
            .set_default("limits.index_high_water_percent", 90)?
            .set_default("security.mode", "auth_key")? // SECURE BY DEFAULT!
            .set_default("security.ztna.enabled", false)?
            .set_default("security.ztna.cert_path", "./certs/server.crt")?
            .set_default("security.ztna.key_path", "./certs/server.key")?
            .set_default("security.ztna.client_ca_path", "./certs/ca.crt")?
            .set_default("retention.default_max_age_secs", 0)?
            .set_default("retention.check_interval_secs", 60)?
            .set_default("retention.streams", HashMap::<String, u64>::new())?
            // Layer 2: Read and parse local configurations
            .add_source(
                File::from(std::path::Path::new("liven.toml"))
                    .required(false)
                    .format(config::FileFormat::Toml),
            )
            .add_source(
                File::from(std::path::Path::new("liven.conf"))
                    .required(false)
                    .format(config::FileFormat::Toml),
            );

        if let Some(explicit) = path {
            if !explicit.exists() {
                return Err(ConfigError::Message(format!(
                    "configuration file does not exist: {}",
                    explicit.display()
                )));
            }
            builder = builder.add_source(
                File::from(explicit)
                    .required(true)
                    .format(config::FileFormat::Toml),
            );
        }

        // Layer 3: Environment variable overrides
        builder = builder
            .add_source(Environment::with_prefix("LIVEN").separator("__"))
            .add_source(Environment::with_prefix("LIVENDB").separator("__"));

        let mut config: Self = builder.build()?.try_deserialize()?;
        config.version = env!("CARGO_PKG_VERSION").to_string();

        // P12: Auto-switch security mode based on environment
        // If user explicitly set security.mode, respect it
        let was_explicitly_set = std::env::var("LIVEN_SECURITY__MODE").is_ok()
            || std::env::var("LIVENDB_SECURITY__MODE").is_ok()
            || path.is_some_and(|p| file_mentions(p, "security") && file_mentions(p, "mode"))
            || file_mentions(std::path::Path::new("liven.toml"), "security")
                && file_mentions(std::path::Path::new("liven.toml"), "mode")
            || file_mentions(std::path::Path::new("liven.conf"), "security")
                && file_mentions(std::path::Path::new("liven.conf"), "mode");

        if !was_explicitly_set {
            // Auto-switch based on environment
            match config.server.environment.as_str() {
                "development" | "test" => {
                    config.security.mode = "none".to_string();
                    tracing::info!(
                        "🔓 Security auto-configured for {} environment: authentication disabled",
                        config.server.environment
                    );
                }
                "production" => {
                    config.security.mode = "auth_key".to_string();
                    tracing::info!(
                        "🔒 Security auto-configured for production environment: auth_key mode enabled"
                    );
                }
                _ => {
                    // Unknown environment - default to secure mode
                    config.security.mode = "auth_key".to_string();
                    tracing::warn!(
                        "Unknown environment '{}', defaulting to secure auth_key mode",
                        config.server.environment
                    );
                }
            }
        } else {
            tracing::info!(
                "Security mode explicitly configured: {}",
                config.security.mode
            );
        }

        // Auto-detect index RAM budget if not explicitly configured
        // Check if max_index_ram_mb was explicitly set by user
        let was_explicitly_set = std::env::var("LIVEN_LIMITS__MAX_INDEX_RAM_MB").is_ok()
            || std::env::var("LIVENDB_LIMITS__MAX_INDEX_RAM_MB").is_ok()
            || path.is_some_and(|p| file_mentions(p, "max_index_ram_mb"))
            || file_mentions(std::path::Path::new("liven.toml"), "max_index_ram_mb")
            || file_mentions(std::path::Path::new("liven.conf"), "max_index_ram_mb");

        if !was_explicitly_set || config.limits.max_index_ram_mb == 16 {
            // User didn't explicitly set it, or it's the old default - use auto-detection
            let auto_budget = auto_detect_index_ram_budget();
            config.limits.max_index_ram_mb = auto_budget as usize;
        } else {
            // User explicitly set a value - use it and log it
            let capacity =
                crate::sysinfo::estimate_key_capacity(config.limits.max_index_ram_mb as u64);
            tracing::info!(
                "Index RAM budget (charged estimate): {}MB (config override) — ~{}M keys at ~{} B/key (compound key + 128)",
                config.limits.max_index_ram_mb,
                capacity / 1_000_000,
                crate::sysinfo::ESTIMATE_BYTES_PER_KEY
            );
        }

        // Key resolution order:
        // 1. Environment variable LIVEN_SECURITY_MASTER_KEY (already populated by config crate)
        // 2. liven.toml / liven.conf config file
        // 3. ./liven.key file
        // 4. Generate new key and write to ./liven.key
        let env_key = std::env::var("LIVEN_SECURITY_MASTER_KEY").ok();
        let key_file_key = std::fs::read_to_string("./liven.key")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());

        let key_from_env = env_key.is_some();
        let key_from_file = key_file_key
            .as_ref()
            .and_then(|k| if !k.is_empty() { Some(k.clone()) } else { None });

        if let Some(ref env_val) = env_key {
            // Env var takes highest priority — use it, do not modify any file.
            config.security.master_key = Some(env_val.clone());
        } else if let Some(ref existing) = config.security.master_key {
            if !existing.trim().is_empty() {
                // Key from liven.toml — use as-is, no file modification.
            } else if let Some(kf) = key_from_file {
                config.security.master_key = Some(kf);
            } else {
                // Empty key in config, no env var, no key file — generate new one.
                let mut key_bytes = [0u8; 32];
                rand::RngCore::fill_bytes(&mut rand::rngs::OsRng, &mut key_bytes);
                let generated_mkey = key_bytes
                    .iter()
                    .map(|b| format!("{:02x}", b))
                    .collect::<String>();
                let _ = write_master_key_to_keyfile("./liven.key", &generated_mkey);
                config.security.master_key = Some(generated_mkey);
            }
        } else if let Some(kf) = key_from_file {
            config.security.master_key = Some(kf);
        } else {
            // No key found anywhere — generate a new one and persist to key file.
            let mut key_bytes = [0u8; 32];
            rand::RngCore::fill_bytes(&mut rand::rngs::OsRng, &mut key_bytes);
            let generated_mkey = key_bytes
                .iter()
                .map(|b| format!("{:02x}", b))
                .collect::<String>();
            let _ = write_master_key_to_keyfile("./liven.key", &generated_mkey);
            config.security.master_key = Some(generated_mkey);
        }

        if !key_from_env {
            // Log a warning if the key is coming from a file rather than env var (production best practice)
            if let Some(path) = std::env::current_dir().ok().map(|p| p.join("./liven.key"))
                && path.exists()
            {
                tracing::warn!(
                    "master key loaded from {:?} — set LIVEN_SECURITY_MASTER_KEY env var for production deployments",
                    path
                );
            }
        }

        Ok(config)
    }

    /// Validates configuration that the server cannot safely guess at.
    pub fn validate(&self) -> Result<(), String> {
        if self.storage.sync_mode != "always" {
            return Err(format!(
                "storage.sync_mode = '{}': only 'always' is implemented",
                self.storage.sync_mode
            ));
        }
        let allow_insecure = matches!(
            std::env::var("LIVEN_ALLOW_INSECURE").as_deref(),
            Ok("1") | Ok("true")
        ) || matches!(
            std::env::var("LIVENDB_ALLOW_INSECURE").as_deref(),
            Ok("1") | Ok("true")
        );
        self.validate_security_with(allow_insecure)
    }

    /// Testable security validation. `allow_insecure` is the explicit override
    /// (normally `LIVEN_ALLOW_INSECURE=1`).
    pub fn validate_security_with(&self, allow_insecure: bool) -> Result<(), String> {
        if self.security.mode != "none" {
            return Ok(());
        }
        let production = self.server.environment.eq_ignore_ascii_case("production");
        let non_loopback = !is_loopback_host(&self.server.host);
        // Production + ZTNA enforces a client certificate on every native
        // connection, so mode=none is authenticated by mTLS rather than by an
        // auth key. In development TLS is not engaged, so this exception does
        // not apply there.
        let mtls_enforced =
            production && self.security.ztna.as_ref().is_some_and(|ztna| ztna.enabled);
        if mtls_enforced {
            return Ok(());
        }
        if production || non_loopback {
            if allow_insecure {
                tracing::warn!(
                    "security.mode=none with environment='{}' host='{}' allowed by explicit override",
                    self.server.environment,
                    self.server.host
                );
                return Ok(());
            }
            return Err(format!(
                "refusing to start with security.mode='none' when environment='{}' and host='{}'. \
                 Set security.mode='auth_key', bind to loopback, or set LIVEN_ALLOW_INSECURE=1 to override.",
                self.server.environment, self.server.host
            ));
        }
        Ok(())
    }

    /// Creates an AppConfig from programmatic values.
    /// Used by the embedded API — no file system access.
    pub fn from_embedded(
        data_directory: &str,
        max_streams: usize,
        max_index_ram_mb: usize,
        max_segment_mb: usize,
        max_open_fds: usize,
        broadcast_capacity: usize,
    ) -> Self {
        AppConfig {
            version: env!("CARGO_PKG_VERSION").to_string(),
            server: ServerConfig {
                environment: "embedded".to_string(),
                host: "127.0.0.1".to_string(),
                db_port: 43121,
                webui_port: 43120,
                max_connections: 1,
                broadcast_capacity,
            },
            storage: StorageConfig {
                data_directory: data_directory.to_string(),
                max_segment_size_mb: max_segment_mb,
                sync_mode: "always".to_string(),
            },
            limits: LimitsConfig {
                max_concurrent_streams: max_streams,
                max_open_file_descriptors: max_open_fds,
                max_index_ram_mb,
                max_segment_size_mb: max_segment_mb,
                max_scan_results: 100_000,
                index_high_water_percent: 90,
            },
            security: SecurityConfig {
                mode: "none".to_string(),
                auth_key: None,
                master_key: None,
                ztna: None,
            },
            retention: RetentionConfig::default(),
        }
    }
}

fn file_mentions(path: &std::path::Path, needle: &str) -> bool {
    std::fs::read_to_string(path)
        .map(|content| content.contains(needle))
        .unwrap_or(false)
}

fn is_loopback_host(host: &str) -> bool {
    let host = host.trim().trim_start_matches('[').trim_end_matches(']');
    matches!(host, "127.0.0.1" | "::1" | "localhost")
}

fn write_master_key_to_keyfile(path: &str, mkey: &str) -> std::io::Result<()> {
    use std::io::Write;
    let mut file = std::fs::File::create(path)?;

    // Set permissions to 0600 on Unix — only the owner can read the key file.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    }

    file.write_all(mkey.as_bytes())?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    Ok(())
}
