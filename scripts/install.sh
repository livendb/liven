#!/bin/sh
# LIVEN — curl https://livendb.com/install | sh
#
# One-liner installer. Downloads the latest LIVEN release binary
# for your platform, verifies it with SHA256 (and GPG if available),
# and installs it to /usr/local/bin.
#
# Usage:
#   curl --proto '=https' --tlsv1.2 -sSfL https://livendb.com/install | sh
#   curl ... | sh -s -- --dir ~/.local/bin          # custom install dir
#   curl ... | sh -s -- --no-service                 # skip systemd/launchd
#   curl ... | sh -s -- --version 0.0.1             # specific version
#
# Environment variables:
#   LIVEN_VERSION    — release tag (default: latest)
#   LIVEN_DIR        — install directory (default: /usr/local/bin)
#   LIVEN_SKIP_SVC   — set to 1 to skip service setup
#
# Repository: https://github.com/livendb/liven

set -e

# ── Config ────────────────────────────────────────────────────────────

REPO="livendb/liven"
BASE_URL="https://github.com/${REPO}/releases"
ARTIFACT_NAME="liven"
GPG_KEY_ID=""                                   # optional: packager GPG key ID

# ── Parse args ────────────────────────────────────────────────────────

INSTALL_DIR="${LIVEN_DIR:-/usr/local/bin}"
SKIP_SVC="${LIVEN_SKIP_SVC:-0}"
VERSION="${LIVEN_VERSION:-}"
while [ "$#" -gt 0 ]; do
  case "$1" in
    --dir)          INSTALL_DIR="$2";  shift 2 ;;
    --dir=*)        INSTALL_DIR="${1#*=}";  shift 1 ;;
    --version)      VERSION="$2";     shift 2 ;;
    --version=*)    VERSION="${1#*=}"; shift 1 ;;
    --no-service)   SKIP_SVC=1;       shift 1 ;;
    --help|-h)      cat <<'HELP'; exit 0 ;;
LIVEN installer — curl https://livendb.com/install | sh

Options:
  --dir <path>       Install directory (default: /usr/local/bin)
  --version <tag>    Release version (default: latest)
  --no-service       Skip systemd/launchd service setup
  --help             Show this help
HELP
    *) echo "Unknown argument: $1"; exit 1 ;;
  esac
done

# ── Detect platform ───────────────────────────────────────────────────

OS="$(uname -s)"
ARCH="$(uname -m)"

case "${OS}" in
  Linux)  OS_TARGET="unknown-linux-gnu" ;;
  Darwin) OS_TARGET="apple-darwin" ;;
  *)
    echo "Unsupported OS: ${OS}"
    echo "LIVEN currently supports Linux and macOS."
    echo "To build from source: cargo build --release"
    exit 1
    ;;
esac

case "${ARCH}" in
  x86_64|amd64) ARCH_TARGET="x86_64" ;;
  aarch64|arm64) ARCH_TARGET="aarch64" ;;
  *)
    echo "Unsupported architecture: ${ARCH}"
    exit 1
    ;;
esac

PLATFORM="${ARCH_TARGET}-${OS_TARGET}"
echo "Detected: ${PLATFORM}"

# ── Resolve version ───────────────────────────────────────────────────

if [ -z "${VERSION}" ]; then
  echo "Fetching latest release version..."
  VERSION="$(curl -sSfL "https://api.github.com/repos/${REPO}/releases/latest" \
    | grep '"tag_name":' \
    | sed 's/.*"tag_name": "\(.*\)",/\1/')"
  if [ -z "${VERSION}" ]; then
    echo "Failed to determine latest version. Try: --version <tag>"
    exit 1
  fi
fi

echo "Release: ${VERSION}"

# ── Download ──────────────────────────────────────────────────────────

ARCHIVE="${ARTIFACT_NAME}-${PLATFORM}.tar.gz"
ARCHIVE_URL="${BASE_URL}/download/${VERSION}/${ARCHIVE}"
CHECKSUM_URL="${ARCHIVE_URL}.sha256"

TMP_DIR="$(mktemp -d 2>/dev/null || mktemp -d -t liven-install)"
cd "${TMP_DIR}"

cleanup() { rm -rf "${TMP_DIR}"; }
trap cleanup EXIT

echo "Downloading ${ARCHIVE_URL} ..."
curl -sSfL -o "${ARCHIVE}" "${ARCHIVE_URL}"
curl -sSfL -o "${ARCHIVE}.sha256" "${CHECKSUM_URL}" || true

# ── Verify SHA256 ─────────────────────────────────────────────────────

if [ -f "${ARCHIVE}.sha256" ]; then
  echo "Verifying SHA256 checksum..."
  if command -v sha256sum >/dev/null; then
    sha256sum -c "${ARCHIVE}.sha256"
  elif command -v shasum >/dev/null; then
    shasum -a 256 -c "${ARCHIVE}.sha256"
  else
    # Manual verification
    EXPECTED="$(cat "${ARCHIVE}.sha256" | awk '{print $1}')"
    if command -v openssl >/dev/null; then
      GOT="$(openssl dgst -sha256 "${ARCHIVE}" | awk '{print $NF}')"
    elif command -v python3 >/dev/null; then
      GOT="$(python3 -c "import hashlib; print(hashlib.sha256(open('${ARCHIVE}','rb').read()).hexdigest())")"
    else
      echo "Warning: no checksum tool found. Skipping verification."
      GOT="${EXPECTED}"
    fi
    if [ "${GOT}" != "${EXPECTED}" ]; then
      echo "Checksum mismatch! Expected ${EXPECTED}, got ${GOT}"
      exit 1
    fi
  fi
  echo "Checksum verified."
fi

# ── Optional GPG verification ─────────────────────────────────────────

if [ -n "${GPG_KEY_ID}" ] && [ -f "${ARCHIVE}.sig" ] && command -v gpg >/dev/null; then
  echo "Verifying GPG signature..."
  gpg --keyserver keyserver.ubuntu.com --recv-keys "${GPG_KEY_ID}" 2>/dev/null || true
  if gpg --verify "${ARCHIVE}.sig" "${ARCHIVE}" 2>/dev/null; then
    echo "GPG signature verified (key: ${GPG_KEY_ID})."
  else
    echo "Warning: GPG signature could not be verified."
  fi
fi

# ── Extract ───────────────────────────────────────────────────────────

echo "Extracting..."
tar -xzf "${ARCHIVE}"
BINARY="./${ARTIFACT_NAME}"

if [ ! -f "${BINARY}" ]; then
  echo "Binary not found in archive. Contents:"
  tar -tzf "${ARCHIVE}"
  exit 1
fi

# ── Install ───────────────────────────────────────────────────────────

echo "Installing to ${INSTALL_DIR}..."
mkdir -p "${INSTALL_DIR}"
cp "${BINARY}" "${INSTALL_DIR}/liven"
chmod 755 "${INSTALL_DIR}/liven"

echo "Installed: ${INSTALL_DIR}/liven"

# ── Post-install sanity check ─────────────────────────────────────────

if "${INSTALL_DIR}/liven" status >/dev/null 2>&1; then
  echo "Binary runs correctly."
else
  # status returns non-zero when server is stopped — that's fine
  # just verify the binary exists and is executable
  if [ -x "${INSTALL_DIR}/liven" ]; then
    echo "Binary installed and executable."
  fi
fi

# ── Service setup ─────────────────────────────────────────────────────

if [ "${SKIP_SVC}" = "1" ]; then
  echo "Service setup skipped (--no-service)."
else
  case "${OS}" in
    Linux)
      if [ -d /run/systemd/system ]; then
        echo "Installing systemd service..."
        cat <<UNIT > /tmp/liven.service
[Unit]
Description=LIVEN High-Performance Storage Engine
After=network.target

[Service]
Type=simple
ExecStart=${INSTALL_DIR}/liven start
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
UNIT
      if [ "$(id -u)" -eq 0 ]; then
        cp /tmp/liven.service /etc/systemd/system/liven.service
        systemctl daemon-reload 2>/dev/null || true
        systemctl enable liven 2>/dev/null || true
        echo "systemd service installed. Start with: systemctl start liven"
        echo "Logs: journalctl -u liven -f"
      else
        echo "Skip systemd install (not root). To install manually:"
        echo "  sudo cp /tmp/liven.service /etc/systemd/system/liven.service"
        echo "  sudo systemctl daemon-reload && sudo systemctl enable liven"
      fi
    else
      echo "Systemd not detected. Manual service setup required."
    fi
      ;;
    Darwin)
      if [ "$(id -u)" -eq 0 ]; then
        echo "Installing launchd plist..."

        # Create log directory if it doesn't exist
        if [ ! -d /usr/local/var/log/liven ]; then
          mkdir -p /usr/local/var/log/liven
          chmod 755 /usr/local/var/log/liven
        fi

        cat <<PLIST > /Library/LaunchDaemons/com.livendb.liven.plist
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.livendb.liven</string>
    <key>ProgramArguments</key>
    <array>
        <string>${INSTALL_DIR}/liven</string>
        <string>start</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>/usr/local/var/log/liven/stdout.log</string>
    <key>StandardErrorPath</key>
    <string>/usr/local/var/log/liven/stderr.log</string>
    <key>EnvironmentVariables</key>
    <dict>
        <key>RUST_LOG</key>
        <string>info</string>
    </dict>
</dict>
</plist>
PLIST
        launchctl load /Library/LaunchDaemons/com.livendb.liven.plist 2>/dev/null || true
        echo "launchd service installed."
        echo "Logs: tail -f /usr/local/var/log/liven/stdout.log"
      else
        echo "Run as root to install launchd service, or start manually:"
        echo "  ${INSTALL_DIR}/liven start"
      fi
      ;;
  esac
fi

# ── First-run notice ──────────────────────────────────────────────────

echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║              LIVEN installed successfully!                 ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""
echo "  Binary:     ${INSTALL_DIR}/liven"
echo "  Version:    ${VERSION}"
echo "  Platform:   ${PLATFORM}"
echo ""
echo "  Start server:  liven start"
echo "  Check status:  liven status"
echo "  Open web UI:   http://localhost:43120"
echo ""
echo "  On first start, an admin auth key is printed to stdout."
echo "  Save it securely — it will never be shown again."
echo ""
echo "  Docs: https://github.com/${REPO}"
