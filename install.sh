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
if [ -z "$VERSION" ]; then
  if command -v curl >/dev/null 2>&1; then
    VERSION="$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" 2>/dev/null | grep -m1 '"tag_name"' | sed -E 's/.*"tag_name": *"([^"]+)".*/\1/')"
  elif command -v wget >/dev/null 2>&1; then
    VERSION="$(wget -qO- "https://api.github.com/repos/${REPO}/releases/latest" 2>/dev/null | grep -m1 '"tag_name"' | sed -E 's/.*"tag_name": *"([^"]+)".*/\1/')"
  fi
  if [ -z "$VERSION" ]; then
    echo "Error: could not determine latest release version automatically."
    echo "Pass an explicit version with --version vX.Y.Z"
    exit 1
  fi
fi

BASE_URL="https://github.com/${REPO}/releases/download/${VERSION}"

echo "Detected OS:   $OS_NORMALIZED"
echo "Detected Arch: $ARCH_NORMALIZED"
echo "Version:       $VERSION"
echo "Environment:   $ENV"

# ---- Map OS/Arch to release asset platform-arch naming ----
case "${OS_NORMALIZED}-${ARCH_NORMALIZED}" in
  Linux-x86_64)    PLATFORM_ARCH="linux-amd64"   ; ARCHIVE_EXT="tar.gz" ;;
  Linux-aarch64)   PLATFORM_ARCH="linux-arm64"   ; ARCHIVE_EXT="tar.gz" ;;
  Darwin-x86_64)   PLATFORM_ARCH="macos-amd64"   ; ARCHIVE_EXT="tar.gz" ;;
  Darwin-aarch64)  PLATFORM_ARCH="macos-arm64"   ; ARCHIVE_EXT="tar.gz" ;;
  Windows-x86_64)  PLATFORM_ARCH="windows-amd64" ; ARCHIVE_EXT="zip"    ;;
  Windows-aarch64) PLATFORM_ARCH="windows-arm64" ; ARCHIVE_EXT="zip"    ;;
  *)
    echo "Unsupported platform: ${OS_NORMALIZED}/${ARCH_NORMALIZED}"
    echo "Please build from source: https://github.com/${REPO}"
    exit 1
    ;;
esac

ARCHIVE_NAME="liven-${PLATFORM_ARCH}.${ARCHIVE_EXT}"
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

# ---- Unix / macOS: download, extract (.tar.gz), install, configure ----
echo "Downloading ${ARCHIVE_NAME} from ${ARCHIVE_URL} ..."
$DOWNLOAD_CMD "/tmp/${ARCHIVE_NAME}" "$ARCHIVE_URL"

echo "Extracting ${ARCHIVE_NAME}..."
# Release archives are .tar.gz on Unix — tar is always present on Linux/macOS,
# unlike unzip, which isn't guaranteed on minimal Linux images.
tar -xzf "/tmp/${ARCHIVE_NAME}" -C "/tmp/"
# Files are at the archive root (liven + liven.toml), no subfolder
BINARY_PATH="/tmp/${BINARY_NAME}"
chmod +x "$BINARY_PATH"

# Ensure directories exist with secure permissions
sudo mkdir -p /usr/local/bin
sudo mkdir -p /etc/liven
sudo mkdir -p /var/log/liven
sudo chmod 700 /var/log/liven
sudo mkdir -p /var/lib/liven
sudo chmod 700 /var/lib/liven

# Install binary
sudo cp "$BINARY_PATH" /usr/local/bin/liven
sudo chmod +x /usr/local/bin/liven

# Clean up downloaded artifacts
rm -f "/tmp/${ARCHIVE_NAME}"
rm -f "$BINARY_PATH"

# ---- Setup Configuration ----
if [ "$ENV" = "production" ]; then
  # Production enforces ZTNA and mTLS
  sudo tee /etc/liven/liven.toml > /dev/null << 'TOML'
[server]
environment = "production"
host = "0.0.0.0"
db_port = 43121
webui_port = 43120

[storage]
data_directory = "/var/lib/liven"

[security]
mode = "auth_key"

[security.ztna]
enabled = true
cert_path = "/etc/liven/certs/server.crt"
key_path = "/etc/liven/certs/server.key"
client_ca_path = "/etc/liven/certs/ca.crt"
TOML

  # Provision TLS Certificates
  sudo mkdir -p /etc/liven/certs
  if command -v openssl >/dev/null; then
    echo "Provisioning self-signed production CA and Server certificates..."
    sudo openssl genrsa -out /etc/liven/certs/ca.key 2048 2>/dev/null
    sudo openssl req -x509 -new -nodes -key /etc/liven/certs/ca.key -sha256 -days 365 \
      -out /etc/liven/certs/ca.crt -subj "/CN=MyLIVENCA" 2>/dev/null

    sudo openssl genrsa -out /etc/liven/certs/server.key 2048 2>/dev/null
    sudo openssl req -new -key /etc/liven/certs/server.key -out /etc/liven/certs/server.csr \
      -subj "/CN=127.0.0.1" 2>/dev/null
    sudo openssl x509 -req -in /etc/liven/certs/server.csr \
      -CA /etc/liven/certs/ca.crt -CAkey /etc/liven/certs/ca.key -CAcreateserial \
      -out /etc/liven/certs/server.crt -days 365 -sha256 2>/dev/null

    sudo chmod 600 /etc/liven/certs/ca.key
    sudo chmod 600 /etc/liven/certs/server.key
    sudo chmod 644 /etc/liven/certs/ca.crt
    sudo chmod 644 /etc/liven/certs/server.crt
  else
    echo "Warning: openssl not found, writing placeholder certs"
    sudo touch /etc/liven/certs/ca.crt
    sudo touch /etc/liven/certs/ca.key
    sudo touch /etc/liven/certs/server.crt
    sudo touch /etc/liven/certs/server.key
    sudo chmod 600 /etc/liven/certs/ca.key
    sudo chmod 600 /etc/liven/certs/server.key
  fi
else
  # Development mode configuration (bypasses mTLS)
  sudo tee /etc/liven/liven.toml > /dev/null << 'TOML'
[server]
environment = "development"
host = "127.0.0.1"
db_port = 43121
webui_port = 43120

[storage]
data_directory = "/var/lib/liven"

[security]
mode = "none"
TOML
fi

# ---- Setup OS-specific launch service ----
if [ "$OS_NORMALIZED" = "Linux" ]; then
  # Check if systemd is running
  if [ -d /run/systemd/system ] || ( command -v systemctl >/dev/null 2>&1 && systemctl is-system-running >/dev/null 2>&1 ); then
    echo "Systemd detected. Configuring systemd service unit..."
    sudo tee /etc/systemd/system/liven.service > /dev/null << 'SERVICE'
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
      sudo systemctl daemon-reload || true
    fi
  else
    echo "Non-Systemd/Container environment detected. Installing POSIX entrypoint shell script..."
    sudo tee /usr/local/bin/liven-entrypoint > /dev/null << 'SCRIPT'
#!/bin/sh
exec /usr/local/bin/liven start --config /etc/liven/liven.toml
SCRIPT
    sudo chmod +x /usr/local/bin/liven-entrypoint
  fi
elif [ "$OS_NORMALIZED" = "Darwin" ]; then
  echo "macOS detected. Configuring launchd plist..."
  sudo mkdir -p /Library/LaunchDaemons
  sudo tee /Library/LaunchDaemons/com.liven.liven.plist > /dev/null << 'PLIST'
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

echo "✨ LIVEN ${VERSION} installation completed successfully!"
echo "   Binary: /usr/local/bin/liven"
echo "   Config: /etc/liven/liven.toml"
