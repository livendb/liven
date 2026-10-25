// Security and Configuration Tests
// Tests for security defaults, configuration profiles, and error handling

use liven::config::AppConfig;
use liven::storage::StorageEngine;
use tempfile::tempdir;

#[test]
fn test_explicit_config_path_is_loaded() {
    let temp_dir = tempdir().unwrap();
    let path = temp_dir.path().join("custom.toml");
    std::fs::write(
        &path,
        r#"
[server]
environment = "production"
host = "127.0.0.1"
[storage]
data_directory = "/tmp/liven-explicit"
[security]
mode = "none"
[retention]
default_max_age_secs = 60
[retention.streams]
events = 5
"#,
    )
    .unwrap();

    let config = AppConfig::load_from(Some(&path)).unwrap();
    assert_eq!(config.server.environment, "production");
    assert_eq!(config.storage.data_directory, "/tmp/liven-explicit");
    // Explicit security.mode must not be auto-switched.
    assert_eq!(config.security.mode, "none");
    assert_eq!(config.retention.default_max_age_secs, 60);
    assert_eq!(config.retention.streams.get("events"), Some(&5));
    assert_eq!(config.limits.index_high_water_percent, 90);
}

#[test]
fn test_insecure_security_mode_is_refused() {
    let mut config = AppConfig::from_embedded("./data", 32, 16, 16, 64, 4096);
    config.security.mode = "none".to_string();

    config.server.environment = "production".to_string();
    config.server.host = "127.0.0.1".to_string();
    assert!(
        config.validate_security_with(false).is_err(),
        "production + mode none must be refused"
    );

    // Production + ZTNA is authenticated by a client certificate, so the
    // auth-key mode may be disabled.
    config.security.ztna = Some(liven::config::ZtnaConfig {
        enabled: true,
        cert_path: None,
        key_path: None,
        client_ca_path: None,
    });
    assert!(config.validate_security_with(false).is_ok());
    config.security.ztna = None;

    config.server.environment = "development".to_string();
    config.server.host = "0.0.0.0".to_string();
    assert!(
        config.validate_security_with(false).is_err(),
        "non-loopback bind + mode none must be refused"
    );

    config.server.host = "127.0.0.1".to_string();
    assert!(config.validate_security_with(false).is_ok());
    assert!(
        config.validate_security_with(true).is_ok(),
        "explicit override must allow the same configuration"
    );
}

#[test]
fn test_unsupported_sync_mode_is_rejected() {
    let mut config = AppConfig::from_embedded("./data", 32, 16, 16, 64, 4096);
    config.storage.sync_mode = "never".to_string();
    assert!(config.validate().is_err());
}

#[test]
fn test_relative_paths_resolve_against_config_directory() {
    let temp_dir = tempdir().unwrap();
    let config_dir = temp_dir.path();
    let path = config_dir.join("liven.toml");
    std::fs::write(
        &path,
        r#"
[server]
environment = "development"
[storage]
data_directory = "./var/liven-data"
"#,
    )
    .unwrap();

    let config = AppConfig::load_from(Some(&path)).unwrap();

    // Relative to the config file, not the process CWD.
    assert_eq!(
        config.storage.data_directory,
        config_dir.join("var/liven-data").to_string_lossy()
    );
    assert!(
        std::path::Path::new(&config.storage.data_directory).is_absolute(),
        "resolved data directory must be absolute: {}",
        config.storage.data_directory
    );
}

#[test]
fn test_ztna_paths_resolve_against_config_directory() {
    let temp_dir = tempdir().unwrap();
    let config_dir = temp_dir.path();
    let path = config_dir.join("liven.toml");
    std::fs::write(
        &path,
        r#"
[server]
environment = "production"
[storage]
data_directory = "./data"
[security.ztna]
enabled = true
cert_path = "./certs/server.crt"
key_path = "./certs/server.key"
client_ca_path = "/etc/liven/ca.crt"
"#,
    )
    .unwrap();

    let config = AppConfig::load_from(Some(&path)).unwrap();
    let ztna = config.security.ztna.expect("ztna config present");

    assert_eq!(
        ztna.cert_path.as_deref(),
        Some(
            config_dir
                .join("certs/server.crt")
                .to_string_lossy()
                .as_ref()
        )
    );
    assert_eq!(
        ztna.key_path.as_deref(),
        Some(
            config_dir
                .join("certs/server.key")
                .to_string_lossy()
                .as_ref()
        )
    );
    // Absolute paths are left untouched.
    assert_eq!(ztna.client_ca_path.as_deref(), Some("/etc/liven/ca.crt"));
}

#[cfg(feature = "server")]
#[test]
fn test_systemd_013_layout_keeps_admin_auth_key() {
    // 0.1.0-0.1.3 under systemd ran with CWD = /, so the master key landed at
    // /liven.key while the data directory stayed at /var/lib/liven.
    let tmp = tempdir().unwrap();
    let data_dir = tmp.path().join("var/lib/liven");
    let fake_root = tmp.path().join("root");
    let fake_cwd = tmp.path().join("cwd");
    std::fs::create_dir_all(&fake_root).unwrap();
    std::fs::create_dir_all(&fake_cwd).unwrap();
    std::fs::write(fake_root.join("liven.key"), "013-master-key\n").unwrap();

    // State left behind by 0.1.3: a default-admin whose auth hash came from the
    // key at the legacy root.
    {
        let engine = StorageEngine::new(&data_dir, 1024 * 1024).unwrap();
        liven::server::bootstrap_auth_keys_table(&engine, Some("013-master-key")).unwrap();
        assert_eq!(engine.list_keys("auth_keys").len(), 1);
    }

    // 0.1.4 upgrade: the legacy root is a lookup candidate, so the same key is
    // reused and no replacement key is written next to the data.
    let (key, source) = liven::config::resolve_master_key(
        &data_dir,
        &[fake_cwd.join("liven.key"), fake_root.join("liven.key")],
        None,
        None,
    );
    assert_eq!(key, "013-master-key");
    assert_eq!(
        source,
        liven::config::MasterKeySource::Legacy(fake_root.join("liven.key"))
    );
    assert!(!data_dir.join("liven.key").exists());

    // The existing auth_keys stream is untouched: bootstrap finds the admin and
    // does not mint a replacement.
    let engine = StorageEngine::new(&data_dir, 1024 * 1024).unwrap();
    let before = engine.get("auth_keys", "default-admin").unwrap().unwrap();
    liven::server::bootstrap_auth_keys_table(&engine, Some(&key)).unwrap();
    let after = engine.get("auth_keys", "default-admin").unwrap().unwrap();
    assert_eq!(
        engine.list_keys("auth_keys"),
        vec!["default-admin".to_string()]
    );
    assert_eq!(before.value, after.value);
    assert!(matches!(after.value, liven::types::DataValue::String(_)));
}

#[test]
fn test_index_ram_limit_error_message() {
    let temp_dir = tempdir().unwrap();
    let data_dir = temp_dir.path();

    // Create engine with very small RAM limit
    let mut engine = StorageEngine::new(data_dir, 16 * 1024 * 1024).unwrap();
    engine.set_max_index_ram_bytes(1); // 1 byte limit - will be hit immediately

    // Try to insert data - should hit RAM limit
    let result = engine.append(
        "test_stream",
        "test_key",
        liven::types::DataValue::String("test_value".to_string()),
        false,
    );

    // Should fail with IndexRamLimitExceeded error
    assert!(result.is_err());

    if let Err(e) = result {
        let error_msg = e.to_string();

        // Error should mention the limit was reached
        assert!(error_msg.contains("Index RAM limit reached"));

        // Should show current and max bytes
        assert!(error_msg.contains("bytes used of"));
        assert!(error_msg.contains("bytes"));

        // Should provide guidance
        assert!(error_msg.contains("set max_index_ram_mb"));
        assert!(error_msg.contains("liven.toml"));
    }
}

#[test]
fn test_config_profiles_documentation_exists() {
    // Verify that liven.toml contains the recommended profiles section
    let config_content = std::fs::read_to_string("liven.toml").unwrap();

    // Should contain the profiles section
    assert!(config_content.contains("RECOMMENDED CONFIGURATION PROFILES"));
    assert!(config_content.contains("Edge/Embedded Profile"));
    assert!(config_content.contains("Development Profile"));
    assert!(config_content.contains("Production Profile"));
    assert!(config_content.contains("High-Volume Profile"));

    // Should explain these are examples
    assert!(config_content.contains("NOT active TOML sections"));
    assert!(config_content.contains("examples only"));
}

#[test]
fn test_auto_ram_detection_formula() {
    // Test the auto-budget calculation formula
    assert_eq!(liven::sysinfo::calculate_auto_budget(128), 32); // Min clamp
    assert_eq!(liven::sysinfo::calculate_auto_budget(256), 64); // 25%
    assert_eq!(liven::sysinfo::calculate_auto_budget(1024), 256); // 25%
    assert_eq!(liven::sysinfo::calculate_auto_budget(4096), 1024); // 25%
    assert_eq!(liven::sysinfo::calculate_auto_budget(16384), 4096); // Max clamp
}
