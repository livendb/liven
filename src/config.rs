use config::{Config, ConfigError, Environment, File};
use serde::{Deserialize, Serialize};

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
    #[serde(default = "default_sync_interval_ms")]
    pub sync_interval_ms: u64,
}

fn default_sync_mode() -> String {
    "always".to_string()
}

fn default_sync_interval_ms() -> u64 {
    100
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LimitsConfig {
    pub max_concurrent_streams: usize,
    pub max_open_file_descriptors: usize,
    pub max_index_ram_mb: usize,
    pub max_segment_size_mb: usize,
}

fn default_limits() -> LimitsConfig {
    LimitsConfig {
        max_concurrent_streams: 32,
        max_open_file_descriptors: 64,
        max_index_ram_mb: 16,
        max_segment_size_mb: 16,
    }
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
    pub mode: String, // "auth_key", "none"
    pub auth_key: Option<AuthKeyConfig>,
    pub master_key: Option<String>,
    pub ztna: Option<ZtnaConfig>,
}

fn default_security() -> SecurityConfig {
    SecurityConfig {
        mode: "none".to_string(),
        auth_key: None,
        master_key: None,
        ztna: None,
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AppConfig {
    pub server: ServerConfig,
    pub storage: StorageConfig,
    #[serde(default = "default_limits")]
    pub limits: LimitsConfig,
    #[serde(default = "default_security")]
    pub security: SecurityConfig,
}

impl AppConfig {
    pub fn load() -> Result<Self, ConfigError> {
        let builder = Config::builder()
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
            .set_default("storage.sync_interval_ms", 100)?
            .set_default("limits.max_concurrent_streams", 32)?
            .set_default("limits.max_open_file_descriptors", 64)?
            .set_default("limits.max_index_ram_mb", 16)?
            .set_default("limits.max_segment_size_mb", 16)?
            .set_default("security.mode", "auth_key")? // SECURE BY DEFAULT!
            .set_default("security.ztna.enabled", false)?
            .set_default("security.ztna.cert_path", "./certs/server.crt")?
            .set_default("security.ztna.key_path", "./certs/server.key")?
            .set_default("security.ztna.client_ca_path", "./certs/ca.crt")?
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
            )
            // Layer 3: Environment variable overrides
            .add_source(Environment::with_prefix("LIVEN").separator("__"))
            .add_source(Environment::with_prefix("LIVENDB").separator("__"));

        let mut config: Self = builder.build()?.try_deserialize()?;

        // Generate 256-bit (32-byte) hexadecimal master key if missing/empty
        let needs_generation = match &config.security.master_key {
            None => true,
            Some(k) => k.trim().is_empty(),
        };

        if needs_generation {
            let mut key_bytes = [0u8; 32];
            rand::RngCore::fill_bytes(&mut rand::rngs::OsRng, &mut key_bytes);
            let generated_mkey = key_bytes
                .iter()
                .map(|b| format!("{:02x}", b))
                .collect::<String>();

            let _ = update_master_key_in_toml(std::path::Path::new("liven.toml"), &generated_mkey);
            config.security.master_key = Some(generated_mkey);
        }

        Ok(config)
    }
}

fn update_master_key_in_toml(path: &std::path::Path, mkey: &str) -> std::io::Result<()> {
    use std::io::Write;
    let content = if path.exists() {
        std::fs::read_to_string(path)?
    } else {
        r#"[server]
environment = "development"
host = "127.0.0.1"
db_port = 43121
webui_port = 43120

[security]
mode = "auth_key"

[storage]
data_directory = "./data"

[limits]
max_concurrent_streams = 32
max_open_file_descriptors = 64
max_index_ram_mb = 16
max_segment_size_mb = 16
"#
        .to_string()
    };

    let mut lines: Vec<String> = content.lines().map(|s| s.to_string()).collect();
    let mut security_sec_idx = None;
    let mut master_key_idx = None;
    let mut next_section_idx = None;

    for (i, line) in lines.iter().enumerate() {
        let trimmed = line.trim();
        if trimmed == "[security]" {
            security_sec_idx = Some(i);
        } else if trimmed.starts_with('[') && trimmed.ends_with(']') {
            if security_sec_idx.is_some() && next_section_idx.is_none() {
                next_section_idx = Some(i);
            }
        } else if trimmed.starts_with("master_key")
            && security_sec_idx.is_some()
            && next_section_idx.is_none()
        {
            master_key_idx = Some(i);
        }
    }

    if let Some(mk_idx) = master_key_idx {
        lines[mk_idx] = format!("master_key = \"{}\"", mkey);
    } else if let Some(sec_idx) = security_sec_idx {
        lines.insert(sec_idx + 1, format!("master_key = \"{}\"", mkey));
    } else {
        lines.push("".to_string());
        lines.push("[security]".to_string());
        lines.push(format!("master_key = \"{}\"", mkey));
    }

    let mut file = std::fs::File::create(path)?;
    for line in lines {
        writeln!(file, "{}", line)?;
    }

    Ok(())
}
