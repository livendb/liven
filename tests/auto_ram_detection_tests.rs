use liven::sysinfo;

#[test]
fn test_auto_budget_calculation() {
    // Test the budget calculation formula: (system_ram / 4).clamp(32, 4096)
    assert_eq!(sysinfo::calculate_auto_budget(128), 32); // Min clamp
    assert_eq!(sysinfo::calculate_auto_budget(256), 64); // 25%
    assert_eq!(sysinfo::calculate_auto_budget(1024), 256); // 25%
    assert_eq!(sysinfo::calculate_auto_budget(4096), 1024); // 25%
    assert_eq!(sysinfo::calculate_auto_budget(16384), 4096); // Max clamp
    assert_eq!(sysinfo::calculate_auto_budget(32768), 4096); // Max clamp
}

#[test]
fn test_key_capacity_estimation() {
    // Real charge: 20-byte representative compound key + 128 bytes/node.
    assert_eq!(sysinfo::ESTIMATE_BYTES_PER_KEY, 148);
    assert_eq!(sysinfo::estimate_key_capacity(16), 113_360);
    assert_eq!(sysinfo::estimate_key_capacity(64), 453_439);
    assert_eq!(sysinfo::estimate_key_capacity(256), 1_813_754);
}

#[test]
fn test_system_ram_detection_fallback() {
    // This test just verifies the function exists and doesn't panic
    // Actual detection may return None in test environment
    let system_ram = sysinfo::detect_system_ram_mb();

    if let Some(ram) = system_ram {
        println!("Detected system RAM: {}MB", ram);
        assert!(ram > 0, "Detected RAM should be positive");
    } else {
        println!("System RAM detection not available in test environment");
    }
}
