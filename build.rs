/// LIVEN build script
///
/// When the `server` feature is enabled, this script builds the
/// Web UI (via npm) so it can be embedded into the binary by
/// `rust-embed`. If npm is not available the build is skipped
/// with a warning — the server binary will still compile but
/// will lack the embedded dashboard.

fn main() {
    // Only required when the server feature (which enables rust-embed) is active
    #[cfg(feature = "server")]
    {
        use std::path::Path;
        use std::process::Command;
        let ui_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("ui");
        let dist_dir = ui_dir.join("dist");

        // If dist/ already exists and is non-empty, skip the build
        if dist_dir.exists()
            && dist_dir
                .read_dir()
                .map(|mut d| d.next().is_some())
                .unwrap_or(false)
        {
            println!("cargo:warning=LIVEN Web UI dist/ already exists — skipping UI build");
            return;
        }

        // Check for npm
        let npm = if cfg!(target_os = "windows") {
            "npm.cmd"
        } else {
            "npm"
        };

        if Command::new(npm).arg("--version").output().is_err() {
            println!(
                "cargo:warning=npm not found — skipping Web UI build. \
                 The server binary will lack the embedded dashboard. \
                 Install Node.js and run `cd ui && npm install && npm run build` first."
            );
            // Create empty dist directory so rust-embed doesn't error
            let _ = std::fs::create_dir_all(&dist_dir);
            return;
        }

        // Install dependencies and build
        let status = Command::new(npm)
            .args(["ci", "--legacy-peer-deps"])
            .current_dir(&ui_dir)
            .status()
            .unwrap_or_else(|e| {
                panic!("Failed to run npm ci in {:?}: {}", ui_dir, e);
            });

        if !status.success() {
            panic!("npm ci failed — cannot build embedded Web UI");
        }

        let status = Command::new(npm)
            .args(["run", "build"])
            .current_dir(&ui_dir)
            .status()
            .unwrap_or_else(|e| {
                panic!("Failed to run npm run build in {:?}: {}", ui_dir, e);
            });

        if !status.success() {
            panic!("npm run build failed — cannot build embedded Web UI");
        }

        // Trigger rebuild if any UI source changes
        println!("cargo:rerun-if-changed=ui/src");
        println!("cargo:rerun-if-changed=ui/public");
        println!("cargo:rerun-if-changed=ui/index.html");
        println!("cargo:rerun-if-changed=ui/package.json");
        println!("cargo:rerun-if-changed=ui/package-lock.json");
        println!("cargo:rerun-if-changed=ui/vite.config.ts");
        println!("cargo:rerun-if-changed=ui/tailwind.config.js");
        println!("cargo:rerun-if-changed=ui/tsconfig.json");
    }

    // Always rebuild when Cargo.toml changes (feature flags affect this script)
    println!("cargo:rerun-if-changed=Cargo.toml");
}
