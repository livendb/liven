#!/bin/bash

# Test script for import/export functionality
set -e

echo "Testing LIVEN import/export functionality..."

# Create a temporary directory for test files
TEST_DIR=$(mktemp -d)
echo "Using test directory: $TEST_DIR"

# Create test JSONL file with all features
echo 'Creating test JSONL file...'
cat > "$TEST_DIR/test.jsonl" << 'EOF'
{"stream": "users", "key": "user1", "timestamp": 1700000000000, "type_tag": 9, "flags": 0, "value": {"name": "Alice", "age": 30}}
{"stream": "users", "key": "user2", "timestamp": 1700000001000, "type_tag": 9, "flags": 1, "value": {"name": "Bob", "age": 25}}
{"stream": "events", "key": "event1", "timestamp": 1700000002000, "type_tag": 9, "flags": 0, "value": {"type": "click", "x": 100, "y": 200}}
EOF

# Create test JSONL file with base64 binary data
echo 'Creating test JSONL file with base64...'
cat > "$TEST_DIR/test_base64.jsonl" << 'EOF'
{"stream": "data", "key": "bin1", "value": "base64:SGVsbG8gV29ybGQ="}
{"stream": "data", "key": "bin2", "value": "base64:V2VsY29tZSB0byBMSVZFTg=="}
EOF

echo "Test files created successfully."
echo "Test directory: $TEST_DIR"
echo "You can manually test with:"
echo "  liven import --path $TEST_DIR/test.jsonl --dry-run"
echo "  liven import --path $TEST_DIR/test_base64.jsonl --dry-run"

# Clean up
# rm -rf "$TEST_DIR"

echo "Test script completed."
