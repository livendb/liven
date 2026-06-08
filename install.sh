#!/bin/sh
# install.sh - KondaDB Cross-Platform Deployment Script

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
    echo "         KondaDB Windows Deployment Instructions            "
    echo "============================================================"
    echo "For Windows execution: "
    echo "1. Build KondaDB using: cargo build --release"
    echo "2. Run binary via PowerShell: .\target\release\kondadb.exe start"
    echo "3. Default configurations will be loaded from kondadb.toml."
    echo "============================================================"
    exit 0
fi

# Ensure directories exist
mkdir -p /usr/local/bin
mkdir -p /etc/kondadb
mkdir -p /var/log/kondadb
chmod 700 /var/log/kondadb

# Copy binary to system bin path
if [ -f target/release/kondadb ]; then
    cp target/release/kondadb /usr/local/bin/kondadb
    chmod +x /usr/local/bin/kondadb
elif [ -f ./kondadb ]; then
    cp ./kondadb /usr/local/bin/kondadb
    chmod +x /usr/local/bin/kondadb
else
    echo "Warning: kondadb binary not found in target/release/ or current directory"
fi

# Setup Configuration
if [ "$ENV" = "production" ]; then
    # Production enforces ZTNA and mTLS
    cat << 'EOF' > /etc/kondadb/kondadb.toml
[server]
environment = "production"
host = "0.0.0.0"
db_port = 43121
webui_port = 43120

[storage]
data_directory = "/var/lib/kondadb"

[security]
mode = "auth_key"

[security.ztna]
enabled = true
cert_path = "/etc/kondadb/certs/server.crt"
key_path = "/etc/kondadb/certs/server.key"
client_ca_path = "/etc/kondadb/certs/ca.crt"
EOF

    # Provision TLS Certificates
    mkdir -p /etc/kondadb/certs
    if command -v openssl >/dev/null; then
        echo "Provisioning self-signed production CA and Server certificates..."
        openssl genrsa -out /etc/kondadb/certs/ca.key 2048 2>/dev/null
        openssl req -x509 -new -nodes -key /etc/kondadb/certs/ca.key -sha256 -days 365 -out /etc/kondadb/certs/ca.crt -subj "/CN=MyKondaDBCA" 2>/dev/null

        openssl genrsa -out /etc/kondadb/certs/server.key 2048 2>/dev/null
        openssl req -new -key /etc/kondadb/certs/server.key -out /etc/kondadb/certs/server.csr -subj "/CN=127.0.0.1" 2>/dev/null
        openssl x509 -req -in /etc/kondadb/certs/server.csr -CA /etc/kondadb/certs/ca.crt -CAkey /etc/kondadb/certs/ca.key -CAcreateserial -out /etc/kondadb/certs/server.crt -days 365 -sha256 2>/dev/null

        chmod 600 /etc/kondadb/certs/ca.key
        chmod 600 /etc/kondadb/certs/server.key
        chmod 644 /etc/kondadb/certs/ca.crt
        chmod 644 /etc/kondadb/certs/server.crt
    else
        echo "Warning: openssl not found, writing placeholder certs"
        touch /etc/kondadb/certs/ca.crt
        touch /etc/kondadb/certs/ca.key
        touch /etc/kondadb/certs/server.crt
        touch /etc/kondadb/certs/server.key
        chmod 600 /etc/kondadb/certs/ca.key
        chmod 600 /etc/kondadb/certs/server.key
    fi

else
    # Development mode configuration (bypasses mTLS)
    cat << 'EOF' > /etc/kondadb/kondadb.toml
[server]
environment = "development"
host = "127.0.0.1"
db_port = 43121
webui_port = 43120

[storage]
data_directory = "/var/lib/kondadb"

[security]
mode = "none"
EOF
fi

# Setup OS-specific launch service trackers
if [ "$OS" = "Linux" ]; then
    # Check if systemd is running (PID 1)
    if [ -d /run/systemd/system ] || [ -d /lib/systemd/system ] || command -v systemctl >/dev/null; then
        echo "Systemd detected. Configuring systemd service unit..."
        cat << 'EOF' > /etc/systemd/system/kondadb.service
[Unit]
Description=KondaDB High-Performance Storage Engine
After=network.target

[Service]
Type=simple
User=root
ExecStart=/usr/local/bin/kondadb start --config /etc/kondadb/kondadb.toml
Restart=on-failure
StandardOutput=append:/var/log/kondadb/access.log
StandardError=append:/var/log/kondadb/error.log

[Install]
WantedBy=multi-user.target
EOF
        if command -v systemctl >/dev/null; then
            systemctl daemon-reload || true
        fi
    else
        echo "Non-Systemd/Container environment detected. Installing POSIX entrypoint shell script layout..."
        cat << 'EOF' > /usr/local/bin/kondadb-entrypoint
#!/bin/sh
exec /usr/local/bin/kondadb start --config /etc/kondadb/kondadb.toml
EOF
        chmod +x /usr/local/bin/kondadb-entrypoint
    fi
elif [ "$OS" = "Darwin" ]; then
    echo "macOS detected. Configuring launchd plist template..."
    mkdir -p /Library/LaunchDaemons
    cat << 'EOF' > /Library/LaunchDaemons/com.konda.kondadb.plist
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.konda.kondadb</string>
    <key>ProgramArguments</key>
    <array>
        <string>/usr/local/bin/kondadb</string>
        <string>start</string>
        <string>--config</string>
        <string>/etc/kondadb/kondadb.toml</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
</dict>
</plist>
EOF
fi

echo "✨ KondaDB system installation completed successfully!"
