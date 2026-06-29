#!/bin/sh
# install.sh - LIVEN Cross-Platform Download & Install Script
# Fetches the pre-built binary archive from GitHub releases and installs it.
# Supported targets:
#   Linux   x86_64  -> liven-linux-amd64.tar.gz
#   Linux   ARM64   -> liven-linux-arm64.tar.gz
#   macOS   x86_64  -> liven-macos-amd64.tar.gz
#   macOS   ARM64   -> liven-macos-arm64.tar.gz
#   Windows x86_64  -> liven-windows-amd64.zip
#   Windows ARM64   -> liven-windows-arm64.zip
#
# Archive names/formats above MUST match what the release pipeline's
# `package` job actually uploads — if you rename assets there, update
# the PLATFORM_ARCH mapping below to match.

set -e

REPO="livendb/liven"

# ---- Argument parsing ----
ENV="production"
VERSION=""
DETECT_ONLY="false"

while [ "$#" -gt 0 ]; do
  case "$1" in
    --env)
      ENV="$2"
      shift 2
      ;;
    --env=*)
      ENV="${1#*=}"
      shift 1
      ;;
    --version)
      VERSION="$2"
      shift 2
      ;;
    --version=*)
      VERSION="${1#*=}"
      shift 1
      ;;
    --detect-only)
      DETECT_ONLY="true"
      shift 1
      ;;
    *)
      shift 1
      ;;
  esac
done

# ---- OS & Architecture detection ----
OS="$(uname -s 2>/dev/null || echo "Windows")"
ARCH="$(uname -m 2>/dev/null || echo "x86_64")"

# Normalize OS name
case "$OS" in
  Linux)  OS_NORMALIZED="Linux" ;;
  Darwin) OS_NORMALIZED="Darwin" ;;
  *)      OS_NORMALIZED="Windows" ;;
esac

# Normalize architecture
case "$ARCH" in
  x86_64|amd64)  ARCH_NORMALIZED="x86_64"  ;;
  aarch64|arm64) ARCH_NORMALIZED="aarch64" ;;
  *)             ARCH_NORMALIZED="$ARCH"    ;;
esac

# ---- Resolve version: explicit --version, else latest GitHub release ----
# On API rate-limit or network failure, fall back to GitHub's
# /releases/latest/download/ redirect URLs (no version needed).
LATEST_URL="https://github.com/${REPO}/releases/latest/download"

if [ -z "$VERSION" ]; then
  if command -v curl >/dev/null 2>&1; then
    VERSION="$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" 2>/dev/null | grep -m1 '"tag_name"' | sed -E 's/.*"tag_name": *"([^"]+)".*/\1/')"
  elif command -v wget >/dev/null 2>&1; then
    VERSION="$(wget -qO- "https://api.github.com/repos/${REPO}/releases/latest" 2>/dev/null | grep -m1 '"tag_name"' | sed -E 's/.*"tag_name": *"([^"]+)".*/\1/')"
  fi
fi

if [ -n "$VERSION" ]; then
  BASE_URL="https://github.com/${REPO}/releases/download/${VERSION}"
else
  VERSION="latest"
  BASE_URL="${LATEST_URL}"
  echo "Note: could not determine latest version from API, using latest release."
fi

echo "Detected OS:   $OS_NORMALIZED"
echo "Detected Arch: $ARCH_NORMALIZED"
echo "Version:       $VERSION"
echo "Environment:   $ENV"

# ---- Map OS/Arch to release asset platform-arch naming ----
case "${OS_NORMALIZED}-${ARCH_NORMALIZED}" in
  Linux-x86_64)    TARGET="x86_64-unknown-linux-gnu"   ;;
  Linux-aarch64)   TARGET="aarch64-unknown-linux-gnu"   ;;
  Darwin-x86_64)   TARGET="x86_64-apple-darwin"         ;;
  Darwin-aarch64)  TARGET="aarch64-apple-darwin"        ;;
  Windows-x86_64)  TARGET="x86_64-pc-windows-msvc"      ;;
  Windows-aarch64) TARGET="aarch64-pc-windows-msvc"     ;;
  *)
    echo "Unsupported platform: ${OS_NORMALIZED}/${ARCH_NORMALIZED}"
    echo "Please build from source: https://github.com/${REPO}"
    exit 1
    ;;
esac

ARCHIVE_NAME="liven-${TARGET}.zip"
ARCHIVE_URL="${BASE_URL}/${ARCHIVE_NAME}"

BINARY_NAME="liven"
if [ "$OS_NORMALIZED" = "Windows" ]; then
  BINARY_NAME="liven.exe"
fi

# ---- Detection-only mode: stop here, no download/install ----
if [ "$DETECT_ONLY" = "true" ]; then
  echo "Resolved archive: $ARCHIVE_NAME"
  echo "Resolved URL:     $ARCHIVE_URL"
  echo "PASS: detection-only mode, exiting before download/install."
  exit 0
fi

# ---- Determine download tool ----
if command -v curl >/dev/null 2>&1; then
  DOWNLOAD_CMD="curl -fsSL -o"
elif command -v wget >/dev/null 2>&1; then
  DOWNLOAD_CMD="wget -q -O"
else
  echo "Error: neither curl nor wget is available. Please install one of them."
  exit 1
fi

# ---- Windows: download, extract (.zip), print instructions ----
if [ "$OS_NORMALIZED" = "Windows" ]; then
  echo "Downloading ${ARCHIVE_NAME}..."
  $DOWNLOAD_CMD "./${ARCHIVE_NAME}" "$ARCHIVE_URL"

  echo "Extracting ${ARCHIVE_NAME}..."
  if command -v unzip >/dev/null 2>&1; then
    unzip -oq "./${ARCHIVE_NAME}"
  elif command -v powershell >/dev/null 2>&1; then
    powershell -NoProfile -Command "Expand-Archive -Path '.\\${ARCHIVE_NAME}' -DestinationPath '.'"
  else
    echo "Error: neither unzip nor powershell is available for extraction."
    exit 1
  fi

  echo "============================================================"
  echo "         LIVEN Windows Installation                        "
  echo "============================================================"
  echo "Binary extracted to: ./${BINARY_NAME}"
  echo ""
  echo "Next steps:"
  echo "  1. Move the binary to your PATH:"
  echo "     move .\\${BINARY_NAME} liven.exe"
  echo "  2. Run: liven.exe start"
  echo "  3. Create a liven.toml configuration file as needed."
  echo "============================================================"
  exit 0
fi

# ---- Unix / macOS: download, extract (.zip), install, configure ----
# Use system paths only if /usr/local/bin is user-writable (brew on macOS).
# Otherwise install to ~/.liven/bin — no sudo needed, no password prompts.

if [ -w /usr/local/bin ] 2>/dev/null; then
  INSTALL_DIR="/usr/local/bin"
  CONFIG_DIR="/etc/liven"
  DATA_DIR="/var/lib/liven"
  LOG_DIR="/var/log/liven"
  USE_SUDO=""
  SYSTEM_WIDE=true
else
  # User-local install — no sudo needed
  INSTALL_DIR="${HOME}/.liven/bin"
  CONFIG_DIR="${HOME}/.liven"
  DATA_DIR="${HOME}/.liven/data"
  LOG_DIR="${HOME}/.liven/logs"
  USE_SUDO=""
  SYSTEM_WIDE=false
fi

# Check if already installed
CURRENT_VERSION=""
if command -v "$INSTALL_DIR/liven" >/dev/null 2>&1; then
  CURRENT_VERSION="$("$INSTALL_DIR/liven" --version 2>/dev/null || true)"
  if [ -n "$CURRENT_VERSION" ]; then
    echo "Liven already installed: $CURRENT_VERSION"
    echo "Upgrading to:           v$VERSION"
  else
    echo "Liven already installed. Upgrading to v$VERSION..."
  fi
  echo ""
fi

echo "Downloading ${ARCHIVE_NAME} from ${ARCHIVE_URL} ..."
$DOWNLOAD_CMD "/tmp/${ARCHIVE_NAME}" "$ARCHIVE_URL"

echo "Extracting ${ARCHIVE_NAME}..."
# Files are at the archive root (liven + liven.toml), no subfolder
unzip -oq "/tmp/${ARCHIVE_NAME}" -d "/tmp/"
BINARY_PATH="/tmp/${BINARY_NAME}"
chmod +x "$BINARY_PATH"

# Ensure directories exist
$USE_SUDO mkdir -p "$INSTALL_DIR"
$USE_SUDO mkdir -p "$CONFIG_DIR"
$USE_SUDO mkdir -p "$LOG_DIR"
$USE_SUDO chmod 700 "$LOG_DIR"
$USE_SUDO mkdir -p "$DATA_DIR"
$USE_SUDO chmod 700 "$DATA_DIR"

# Install binary
$USE_SUDO cp "$BINARY_PATH" "$INSTALL_DIR/liven"
$USE_SUDO chmod +x "$INSTALL_DIR/liven"

# Clean up downloaded artifacts
rm -f "/tmp/${ARCHIVE_NAME}"
rm -f "$BINARY_PATH"

# ---- Setup Configuration ----
if [ "$ENV" = "production" ]; then
  $USE_SUDO tee "$CONFIG_DIR/liven.toml" > /dev/null << TOML
[server]
environment = "production"
host = "0.0.0.0"
db_port = 43121
webui_port = 43120

[storage]
data_directory = "$DATA_DIR"

[security]
mode = "auth_key"

[security.ztna]
enabled = true
cert_path = "$CONFIG_DIR/certs/server.crt"
key_path = "$CONFIG_DIR/certs/server.key"
client_ca_path = "$CONFIG_DIR/certs/ca.crt"
TOML

  # Provision TLS Certificates
  $USE_SUDO mkdir -p "$CONFIG_DIR/certs"
  if command -v openssl >/dev/null; then
    echo "Provisioning self-signed production CA and Server certificates..."
    $USE_SUDO openssl genrsa -out "$CONFIG_DIR/certs/ca.key" 2048 2>/dev/null
    $USE_SUDO openssl req -x509 -new -nodes -key "$CONFIG_DIR/certs/ca.key" -sha256 -days 365 \
      -out "$CONFIG_DIR/certs/ca.crt" -subj "/CN=MyLIVENCA" 2>/dev/null

    $USE_SUDO openssl genrsa -out "$CONFIG_DIR/certs/server.key" 2048 2>/dev/null
    $USE_SUDO openssl req -new -key "$CONFIG_DIR/certs/server.key" -out "$CONFIG_DIR/certs/server.csr" \
      -subj "/CN=127.0.0.1" 2>/dev/null
    $USE_SUDO openssl x509 -req -in "$CONFIG_DIR/certs/server.csr" \
      -CA "$CONFIG_DIR/certs/ca.crt" -CAkey "$CONFIG_DIR/certs/ca.key" -CAcreateserial \
      -out "$CONFIG_DIR/certs/server.crt" -days 365 -sha256 2>/dev/null

    $USE_SUDO chmod 600 "$CONFIG_DIR/certs/ca.key"
    $USE_SUDO chmod 600 "$CONFIG_DIR/certs/server.key"
    $USE_SUDO chmod 644 "$CONFIG_DIR/certs/ca.crt"
    $USE_SUDO chmod 644 "$CONFIG_DIR/certs/server.crt"
  else
    echo "Warning: openssl not found, writing placeholder certs"
    $USE_SUDO touch "$CONFIG_DIR/certs/ca.crt"
    $USE_SUDO touch "$CONFIG_DIR/certs/ca.key"
    $USE_SUDO touch "$CONFIG_DIR/certs/server.crt"
    $USE_SUDO touch "$CONFIG_DIR/certs/server.key"
    $USE_SUDO chmod 600 "$CONFIG_DIR/certs/ca.key"
    $USE_SUDO chmod 600 "$CONFIG_DIR/certs/server.key"
  fi
else
  $USE_SUDO tee "$CONFIG_DIR/liven.toml" > /dev/null << TOML
[server]
environment = "development"
host = "127.0.0.1"
db_port = 43121
webui_port = 43120

[storage]
data_directory = "$DATA_DIR"

[security]
mode = "none"
TOML
fi

# ---- Setup OS-specific launch service (system-wide only) ----
if [ "$SYSTEM_WIDE" = true ]; then
  if [ "$OS_NORMALIZED" = "Linux" ]; then
    # Check if systemd is running
    if [ -d /run/systemd/system ] || ( command -v systemctl >/dev/null 2>&1 && systemctl is-system-running >/dev/null 2>&1 ); then
      echo "Systemd detected. Configuring systemd service unit..."
      $USE_SUDO tee /etc/systemd/system/liven.service > /dev/null << 'SERVICE'
[Unit]
Description=LIVEN High-Performance Storage Engine
After=network.target

[Service]
Type=simple
User=root
ExecStart=/usr/local/bin/liven start --config /etc/liven/liven.toml
Restart=on-failure
StandardOutput=append:/var/log/liven/access.log
StandardError=append:/var/log/liven/error.log

[Install]
WantedBy=multi-user.target
SERVICE
      if command -v systemctl >/dev/null; then
        $USE_SUDO systemctl daemon-reload || true
      fi
    else
      echo "Non-Systemd/Container environment detected. Installing POSIX entrypoint shell script..."
      $USE_SUDO tee /usr/local/bin/liven-entrypoint > /dev/null << 'SCRIPT'
#!/bin/sh
exec /usr/local/bin/liven start --config /etc/liven/liven.toml
SCRIPT
      $USE_SUDO chmod +x /usr/local/bin/liven-entrypoint
    fi
  elif [ "$OS_NORMALIZED" = "Darwin" ]; then
    echo "macOS detected. Configuring launchd plist..."
    $USE_SUDO mkdir -p /Library/LaunchDaemons
    $USE_SUDO tee /Library/LaunchDaemons/com.liven.liven.plist > /dev/null << 'PLIST'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.liven.liven</string>
    <key>ProgramArguments</key>
    <array>
        <string>/usr/local/bin/liven</string>
        <string>start</string>
        <string>--config</string>
        <string>/etc/liven/liven.toml</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
</dict>
</plist>
PLIST
  fi
fi

# ---- Ensure install directory is in PATH (user-local only) ----
if [ "$SYSTEM_WIDE" = false ]; then
  case ":$PATH:" in
    *":$INSTALL_DIR:"*) : ;; # already in PATH
    *)
      echo "Adding $INSTALL_DIR to PATH in shell configuration..."
      for rc in "${HOME}/.bashrc" "${HOME}/.zshrc" "${HOME}/.profile"; do
        if [ -f "$rc" ]; then
          if ! grep -q "export PATH=.*\$HOME/\.liven/bin" "$rc"; then
            echo "" >> "$rc"
            echo "# Added by Liven installer" >> "$rc"
            echo "export PATH=\"\$HOME/.liven/bin:\$PATH\"" >> "$rc"
          fi
        fi
      done
      # Also try a more generic approach for other shells (fish, etc.)
      if command -v fish >/dev/null 2>&1; then
        fish_conf="${HOME}/.config/fish/config.fish"
        mkdir -p "$(dirname "$fish_conf")"
        if ! grep -q "fish_add_path.*local/bin" "$fish_conf" 2>/dev/null; then
          echo "" >> "$fish_conf"
          echo "# Added by Liven installer" >> "$fish_conf"
          echo "fish_add_path \$HOME/.liven/bin" >> "$fish_conf"
        fi
      fi
      export PATH="$INSTALL_DIR:$PATH"
      echo "  ✓ Added to PATH for current and future shell sessions."
      ;;
  esac
fi

echo "✨ LIVEN ${VERSION} installation completed successfully!"
echo "   Binary: $INSTALL_DIR/liven"
echo "   Config: $CONFIG_DIR/liven.toml"

if [ "$SYSTEM_WIDE" = false ]; then
  echo ""
  echo "   Open a new terminal or run: source ~/.bashrc (or ~/.zshrc)"
  echo "   Then: liven start"
fi
