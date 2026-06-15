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
    // Test the key capacity formula: budget_mb * 1024 * 1024 / 84
    let capacity_16 = sysinfo::estimate_key_capacity(16);
    let capacity_64 = sysinfo::estimate_key_capacity(64);
    let capacity_256 = sysinfo::estimate_key_capacity(256);

    // Approximate checks due to integer division
    assert!(
        capacity_16 > 199_000 && capacity_16 < 200_000,
        "16MB: got {}",
        capacity_16
    );
    assert!(
        capacity_64 > 798_000 && capacity_64 < 799_000,
        "64MB: got {}",
        capacity_64
    );
    assert!(
        capacity_256 > 3_195_000 && capacity_256 < 3_196_000,
        "256MB: got {}",
        capacity_256
    );
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
