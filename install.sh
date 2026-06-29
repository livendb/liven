#!/bin/sh
# install.sh - LIVEN Cross-Platform Download & Install Script
# Fetches the pre-built binary archive from GitHub releases and installs it.
# Supported targets:
#   Linux   x86_64  -> liven-x86_64-unknown-linux-gnu
#   Linux   ARM64   -> liven-aarch64-unknown-linux-gnu
#   macOS   x86_64  -> liven-x86_64-apple-darwin
#   macOS   ARM64   -> liven-aarch64-apple-darwin
#   Windows x86_64  -> liven-x86_64-pc-windows-msvc
#   Windows ARM64   -> liven-aarch64-pc-windows-msvc

set -e

VERSION="v0.0.2"
REPO="livendb/liven"
BASE_URL="https://github.com/${REPO}/releases/download/${VERSION}"

# ---- Argument parsing ----
ENV="production"

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

echo "Detected OS:   $OS_NORMALIZED"
echo "Detected Arch: $ARCH_NORMALIZED"
echo "Version:       $VERSION"
echo "Environment:   $ENV"

# ---- Build target triple and archive/file names ----
case "${OS_NORMALIZED}-${ARCH_NORMALIZED}" in
  Linux-x86_64)   TARGET="x86_64-unknown-linux-gnu"   ;;
  Linux-aarch64)  TARGET="aarch64-unknown-linux-gnu"   ;;
  Darwin-x86_64)  TARGET="x86_64-apple-darwin"         ;;
  Darwin-aarch64) TARGET="aarch64-apple-darwin"        ;;
  Windows-x86_64) TARGET="x86_64-pc-windows-msvc"      ;;
  Windows-aarch64)TARGET="aarch64-pc-windows-msvc"     ;;
  *)
    echo "Unsupported platform: ${OS_NORMALIZED}/${ARCH_NORMALIZED}"
    echo "Please build from source: https://github.com/${REPO}"
    exit 1
    ;;
esac

FOLDER_NAME="liven-${TARGET}"
ARCHIVE_NAME="${FOLDER_NAME}.zip"
ARCHIVE_URL="${BASE_URL}/${ARCHIVE_NAME}"

BINARY_NAME="liven"
if [ "$OS_NORMALIZED" = "Windows" ]; then
  BINARY_NAME="liven.exe"
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

# ---- Determine extraction tool ----
EXTRACT_CMD=""
if command -v unzip >/dev/null 2>&1; then
  EXTRACT_CMD="unzip -q"
elif command -v powershell >/dev/null 2>&1; then
  # Fallback for minimal Windows environments without unzip
  EXTRACT_CMD="powershell -NoProfile -Command Expand-Archive -Path"
else
  echo "Error: neither unzip nor powershell is available for extraction."
  exit 1
fi

# ---- Windows: download, extract, print instructions ----
if [ "$OS_NORMALIZED" = "Windows" ]; then
  echo "Downloading ${ARCHIVE_NAME}..."
  $DOWNLOAD_CMD "./${ARCHIVE_NAME}" "$ARCHIVE_URL"

  echo "Extracting ${ARCHIVE_NAME}..."
  if echo "$EXTRACT_CMD" | grep -q "^powershell"; then
    powershell -NoProfile -Command "Expand-Archive -Path '.\${ARCHIVE_NAME}' -DestinationPath '.'"
  else
    $EXTRACT_CMD "./${ARCHIVE_NAME}"
  fi

  echo "============================================================"
  echo "         LIVEN Windows Installation                        "
  echo "============================================================"
  echo "Binary extracted to: ./${FOLDER_NAME}/${BINARY_NAME}"
  echo ""
  echo "Next steps:"
  echo "  1. Add to PATH or copy:"
  echo "     copy .\\${FOLDER_NAME}\\${BINARY_NAME} liven.exe"
  echo "  2. Run: liven.exe start"
  echo "  3. Create a liven.toml configuration file as needed."
  echo "============================================================"
  exit 0
fi

# ---- Unix / macOS: download, extract, install, configure ----
echo "Downloading ${ARCHIVE_NAME} from ${ARCHIVE_URL} ..."
$DOWNLOAD_CMD "/tmp/${ARCHIVE_NAME}" "$ARCHIVE_URL"

echo "Extracting ${ARCHIVE_NAME}..."
$EXTRACT_CMD "/tmp/${ARCHIVE_NAME}" -d "/tmp/"
BINARY_PATH="/tmp/${FOLDER_NAME}/${BINARY_NAME}"
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
rm -rf "/tmp/${FOLDER_NAME}"

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
