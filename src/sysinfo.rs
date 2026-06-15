/// System information detection for auto-configuration

/// Detects total system RAM in megabytes
/// Returns None if detection fails, in which case fallback values are used
pub fn detect_system_ram_mb() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        detect_linux_ram()
    }

    #[cfg(target_os = "macos")]
    {
        detect_macos_ram()
    }

    #[cfg(target_os = "windows")]
    {
        detect_windows_ram()
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    {
        // Unsupported platform - return None to use fallback
        None
    }
}

#[cfg(target_os = "linux")]
fn detect_linux_ram() -> Option<u64> {
    use std::fs::read_to_string;

    let meminfo = read_to_string("/proc/meminfo").ok()?;

    for line in meminfo.lines() {
        if line.starts_with("MemTotal:") {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 2 {
                if let Ok(kb) = parts[1].parse::<u64>() {
                    return Some(kb / 1024); // Convert KB to MB
                }
            }
            break;
        }
    }
    None
}

#[cfg(target_os = "macos")]
fn detect_macos_ram() -> Option<u64> {
    use std::process::Command;

    let output = Command::new("sysctl")
        .arg("-n")
        .arg("hw.memsize")
        .output()
        .ok()?;

    if output.status.success() {
        let bytes_str = String::from_utf8(output.stdout).ok()?;
        if let Ok(bytes) = bytes_str.trim().parse::<u64>() {
            return Some(bytes / 1024 / 1024); // Convert bytes to MB
        }
    }
    None
}

#[cfg(target_os = "windows")]
fn detect_windows_ram() -> Option<u64> {
    // Windows implementation would use GlobalMemoryStatusEx
    // This is a simplified fallback for now
    use std::process::Command;

    let output = Command::new("wmic")
        .args(["OS", "get", "TotalVisibleMemorySize"])
        .output()
        .ok()?;

    if output.status.success() {
        let kb_str = String::from_utf8(output.stdout).ok()?;
        if let Ok(kb) = kb_str.trim().parse::<u64>() {
            return Some(kb / 1024); // Convert KB to MB
        }
    }
    None
}

/// Calculates the auto-budget for index RAM based on detected system RAM
/// Formula: (system_ram_mb / 4).clamp(32, 4096)
pub fn calculate_auto_budget(system_ram_mb: u64) -> u64 {
    let budget = system_ram_mb / 4; // 25% of system RAM
    budget.clamp(32, 4096) // Min 32MB, Max 4GB
}

/// Estimates key capacity based on RAM budget
/// Formula: budget_mb * 1024 * 1024 / 84 bytes per key
pub fn estimate_key_capacity(budget_mb: u64) -> u64 {
    (budget_mb * 1024 * 1024 + 83) / 84 // Add 83 for rounding
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_auto_budget() {
        // Test various system RAM sizes
        assert_eq!(calculate_auto_budget(128), 32); // Min clamp
        assert_eq!(calculate_auto_budget(256), 64); // 25%
        assert_eq!(calculate_auto_budget(1024), 256); // 25%
        assert_eq!(calculate_auto_budget(4096), 1024); // 25%
        assert_eq!(calculate_auto_budget(16384), 4096); // Max clamp
        assert_eq!(calculate_auto_budget(32768), 4096); // Max clamp
    }

    #[test]
    fn test_estimate_key_capacity() {
        // Test key capacity estimation (approximate due to integer division)
        let capacity_16 = estimate_key_capacity(16);
        assert!(
            capacity_16 > 190_000 && capacity_16 < 200_000,
            "16MB should be ~190K keys, got {}",
            capacity_16
        );

        let capacity_64 = estimate_key_capacity(64);
        assert!(
            capacity_64 > 790_000 && capacity_64 < 800_000,
            "64MB should be ~798K keys, got {}",
            capacity_64
        );

        let capacity_256 = estimate_key_capacity(256);
        assert!(
            capacity_256 > 3_100_000 && capacity_256 < 3_200_000,
            "256MB should be ~3.19M keys, got {}",
            capacity_256
        );
    }
}
