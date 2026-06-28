# LIVEN Release Process

## Prerequisites

- Rust toolchain (stable) — `rustup install stable`
- Node.js 20+ and npm — for building the embedded web UI
- `cargo-deb` — `cargo install cargo-deb`
- `cargo-generate-rpm` — `cargo install cargo-generate-rpm`
- `cargo-wix` — `cargo install cargo-wix` (Windows only)
- `appdmg` — `npm install -g appdmg` (macOS only)

## Step 1: Update version

```bash
# Update version in Cargo.toml
# Then commit and tag
git tag v0.0.2
```

## Step 2: Build and test

```bash
# Full build with embedded UI
cargo build --release

# Run all tests
cargo test --tests

# Lint
cargo clippy --all-targets

# Format check
cargo fmt --all -- --check
```

## Step 3: Publish to crates.io

```bash
# Dry run first
cargo publish --dry-run

# If dry run succeeds, publish
cargo publish
```

> **Note:** The `server` feature embeds a Web UI via `rust-embed`.
> Before publishing, ensure `ui/dist/` is built:
> ```bash
> cd ui && npm ci --legacy-peer-deps && npm run build && cd ..
> ```
> Or publish with `--no-default-features` if the dashboard is not needed.

## Step 4: Build distribution packages

### Linux (.deb)

```bash
cargo deb --no-build -p liven
# Output: target/debian/liven_0.0.1_amd64.deb
```

### Linux (.rpm)

```bash
cargo generate-rpm --no-build -p liven
# Output: target/generate-rpm/liven-0.0.1-1.x86_64.rpm
```

### Linux (tarball)

```bash
tar -czvf liven-linux-amd64.tar.gz \
    -C target/release liven \
    -C ../.. liven.toml
```

### macOS (.tar.gz)

```bash
tar -czvf liven-macos-x64.tar.gz \
    -C target/release liven \
    -C ../.. liven.toml
```

### macOS (.dmg)

```bash
mkdir -p assets
# Place liven.icns and dmg_background.png in assets/
appdmg appdmg.json dist/liven-macos.dmg
```

### Windows (.msi)

```powershell
cargo wix --no-build
# Output: target/wix/liven-0.0.1-x86_64.msi
```

### Windows (.zip)

```powershell
Compress-Archive -Path target/release/liven.exe, liven.toml `
    -DestinationPath liven-windows-amd64.zip
```

### Docker

```bash
docker build -t liven/liven:latest .
docker tag liven/liven:latest liven/liven:0.0.1
```

## Step 5: Push Docker image

```bash
docker push liven/liven:0.0.1
docker push liven/liven:latest
```

## Step 6: Create GitHub Release

1. Go to https://github.com/livendb/liven/releases
2. Click "Draft a new release"
3. Select the tag
4. Upload all artifacts from `dist/`
5. Publish

## CI/CD

The GitHub Actions workflows in `.github/workflows/` handle all of the above
automatically on push to main/master:
- `build.yml` — builds, tests, and packages for Linux/macOS/Windows
- `docker.yml` — builds and pushes Docker images to GHCR

The CI requires the following secrets:
- `GITHUB_TOKEN` (provided automatically by GitHub Actions)
