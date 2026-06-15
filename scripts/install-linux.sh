#!/bin/bash
# Install Liven as a systemd service

set -e

echo "Installing Liven systemd service..."

# Create log directory if it doesn't exist
if [ ! -d /var/log/liven ]; then
    echo "Creating log directory at /var/log/liven"
    sudo mkdir -p /var/log/liven
    sudo chmod 755 /var/log/liven
fi

# Copy the service file
sudo cp liven.service /etc/systemd/system/

# Reload systemd
echo "Reloading systemd daemon..."
sudo systemctl daemon-reload

# Enable the service
echo "Enabling Liven service..."
sudo systemctl enable liven

# Start the service
echo "Starting Liven service..."
sudo systemctl start liven

echo ""
echo "Liven service installed successfully!"
echo ""
echo "Commands:"
echo "  Start:     sudo systemctl start liven"
echo "  Stop:      sudo systemctl stop liven"
echo "  Status:    sudo systemctl status liven"
echo "  Logs:      sudo journalctl -u liven -f"
echo ""
echo "Logs are captured by journald. View with: journalctl -u liven -f"
