#!/bin/bash
# Install Liven as a launchd service

set -e

echo "Installing Liven launchd service..."

# Create log directory if it doesn't exist
if [ ! -d /usr/local/var/log/liven ]; then
    echo "Creating log directory at /usr/local/var/log/liven"
    sudo mkdir -p /usr/local/var/log/liven
    sudo chmod 755 /usr/local/var/log/liven
fi

# Copy the plist file
sudo cp com.livendb.liven.plist /Library/LaunchDaemons/

# Set proper permissions
sudo chown root:wheel /Library/LaunchDaemons/com.livendb.liven.plist
sudo chmod 644 /Library/LaunchDaemons/com.livendb.liven.plist

# Load the service
echo "Loading Liven launchd service..."
sudo launchctl load /Library/LaunchDaemons/com.livendb.liven.plist

# Start the service
echo "Starting Liven service..."
sudo launchctl start com.livendb.liven

echo ""
echo "Liven service installed successfully!"
echo ""
echo "Commands:"
echo "  Start:     sudo launchctl start com.livendb.liven"
echo "  Stop:      sudo launchctl stop com.livendb.liven"
echo "  Logs:      tail -f /usr/local/var/log/liven/stdout.log"
echo "  Errors:    tail -f /usr/local/var/log/liven/stderr.log"
echo ""
echo "Logs are written to:"
echo "  stdout: /usr/local/var/log/liven/stdout.log"
echo "  stderr: /usr/local/var/log/liven/stderr.log"
