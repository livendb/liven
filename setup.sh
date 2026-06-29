#!/bin/sh
# setup.sh - LIVEN Cross-Platform Deployment Script

set -e

# Setup default values
ENV="production"

# Parse arguments
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

OS="$(uname -s 2>/dev/null || echo "Windows")"
echo "Detected OS: $OS"
echo "Deployment Environment: $ENV"

# Graceful termination for Windows or unsupported environments
if [ "$OS" != "Linux" ] && [ "$OS" != "Darwin" ]; then
    echo "============================================================"
    echo "         LIVEN Windows Deployment Instructions            "
    echo "============================================================"
    echo "For Windows execution: "
    echo "1. Build LIVEN using: cargo build --release"
    echo "2. Run binary via PowerShell: .\target\release\liven.exe start"
    echo "3. Default configurations will be loaded from liven.toml."
    echo "============================================================"
    exit 0
fi

# Ensure directories exist with secure permissions
mkdir -p /usr/local/bin
mkdir -p /etc/liven
mkdir -p /var/log/liven
chmod 700 /var/log/liven
mkdir -p /var/lib/liven
chmod 700 /var/lib/liven

# Copy binary to system bin path
if [ -f target/release/liven ]; then
    cp target/release/liven /usr/local/bin/liven
    chmod +x /usr/local/bin/liven
elif [ -f ./liven ]; then
    cp ./liven /usr/local/bin/liven
    chmod +x /usr/local/bin/liven
else
    echo "Warning: liven binary not found in target/release/ or current directory"
fi

# Setup Configuration
if [ "$ENV" = "production" ]; then
    # Production enforces ZTNA and mTLS
    cat << 'EOF' > /etc/liven/liven.toml
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
EOF

    # Provision TLS Certificates
    mkdir -p /etc/liven/certs
    if command -v openssl >/dev/null; then
        echo "Provisioning self-signed production CA and Server certificates..."
        openssl genrsa -out /etc/liven/certs/ca.key 2048 2>/dev/null
        openssl req -x509 -new -nodes -key /etc/liven/certs/ca.key -sha256 -days 365 -out /etc/liven/certs/ca.crt -subj "/CN=MyLIVENCA" 2>/dev/null

        openssl genrsa -out /etc/liven/certs/server.key 2048 2>/dev/null
        openssl req -new -key /etc/liven/certs/server.key -out /etc/liven/certs/server.csr -subj "/CN=127.0.0.1" 2>/dev/null
        openssl x509 -req -in /etc/liven/certs/server.csr -CA /etc/liven/certs/ca.crt -CAkey /etc/liven/certs/ca.key -CAcreateserial -out /etc/liven/certs/server.crt -days 365 -sha256 2>/dev/null

        chmod 600 /etc/liven/certs/ca.key
        chmod 600 /etc/liven/certs/server.key
        chmod 644 /etc/liven/certs/ca.crt
        chmod 644 /etc/liven/certs/server.crt
    else
        echo "Warning: openssl not found, writing placeholder certs"
        touch /etc/liven/certs/ca.crt
        touch /etc/liven/certs/ca.key
        touch /etc/liven/certs/server.crt
        touch /etc/liven/certs/server.key
        chmod 600 /etc/liven/certs/ca.key
        chmod 600 /etc/liven/certs/server.key
    fi

else
    # Development mode configuration (bypasses mTLS)
    cat << 'EOF' > /etc/liven/liven.toml
[server]
environment = "development"
host = "127.0.0.1"
db_port = 43121
webui_port = 43120

[storage]
data_directory = "/var/lib/liven"

[security]
mode = "none"
EOF
fi

# Setup OS-specific launch service trackers
if [ "$OS" = "Linux" ]; then
    # Check if systemd is running (PID 1)
    if [ -d /run/systemd/system ] || ( command -v systemctl >/dev/null 2>&1 && systemctl is-system-running >/dev/null 2>&1 ); then
        echo "Systemd detected. Configuring systemd service unit..."
        cat << 'EOF' > /etc/systemd/system/liven.service
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
EOF
        if command -v systemctl >/dev/null; then
            systemctl daemon-reload || true
        fi
    else
        echo "Non-Systemd/Container environment detected. Installing POSIX entrypoint shell script layout..."
        cat << 'EOF' > /usr/local/bin/liven-entrypoint
#!/bin/sh
exec /usr/local/bin/liven start --config /etc/liven/liven.toml
EOF
        chmod +x /usr/local/bin/liven-entrypoint
    fi
elif [ "$OS" = "Darwin" ]; then
    echo "macOS detected. Configuring launchd plist template..."
    mkdir -p /Library/LaunchDaemons
    cat << 'EOF' > /Library/LaunchDaemons/com.liven.liven.plist
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
EOF
fi

echo "✨ LIVEN system installation completed successfully!"
