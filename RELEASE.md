# LIVEN Release Process

## Prerequisites

- Rust toolchain (stable) — `rustup install stable`
- Node.js 24+ and pnpm 11 (pnpm 11 loads `node:sqlite`) — for building the embedded web UI
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
>
> ```bash
> cd ui && pnpm install --frozen-lockfile && pnpm build && cd ..
> ```
>
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

---

## 0.1.4 manual release checklist (data-loss advisory)

Repo-side work for 0.1.4 is done on `release-0.1.4`; these are the actions that
require credentials or human review, in order.

1. **Review and merge** `release-0.1.4` into `main` (draft PR #1). The
   `Build & Verify` matrix only runs cross-builds on push/`workflow_dispatch`;
   a `workflow_dispatch` run on the branch is the pre-merge evidence.
2. **Bump the version** to `0.1.4` in `Cargo.toml`, update `Cargo.lock`
   (`cargo update -p liven --precise 0.1.4` or `cargo check`), commit, tag
   `v0.1.4`.
3. **Publish the draft advisory** (`docs/SECURITY-ADVISORY-0.1.4.md`) on the
   site and as a GitHub Security Advisory (`Repository → Security → Advisories`).
   File a RUSTSEC advisory for the crates.io releases (the data-loss defects are
   not a memory-safety issue, so this may be published as an informational
   advisory pointing at the GitHub one).
4. **Yank affected crates.io versions**: `0.1.1`, `0.1.2`, `0.1.3` for the
   torn-tail defect; `0.0.2`–`0.0.9` as well for the compaction ID collision.
   (`0.1.0` did not truncate, but it also cannot recover stranded frames — yank
   it for consistency if the advisory names the 0.1.x line.) `cargo yank --vers
   <v> liven` for each.
5. **Retag the Docker image**: `docker.yml` runs on push to `main`; confirm
   `ghcr.io/livendb/liven:latest` now points at the 0.1.4 build and that the
   image starts with `-v ./data:/var/lib/liven` (the 0.1.4 `WORKDIR` path).
6. **Update the install script / one-liner** to fetch 0.1.4
   (`install.sh` resolves the latest release; verify the pinned fallback, if
   any).
7. **Add the CHANGELOG entry** from this repository to the release notes and the
   site changelog page.
8. **Notify 0.1.x operators** that `--repair` must be run once against
   directories written by the affected versions and that `*.torn-*.bak`
   sidecars must be kept until the data is verified.

Do not bump the version, tag, publish, or retag anything from an automated
change: steps 2–7 are deliberately manual.
