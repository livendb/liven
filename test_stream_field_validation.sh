#!/bin/bash

# Test script to verify stream field validation
echo "Testing stream field validation..."

# Create a temporary directory
TEST_DIR=$(mktemp -d)
echo "Test directory: $TEST_DIR"

# Test 1: Valid JSONL with stream field
echo "Test 1: Valid JSONL with stream field"
cat > "$TEST_DIR/valid.jsonl" << 'EOF'
{"stream": "users", "key": "user1", "value": {"name": "Alice"}}
{"stream": "events", "key": "event1", "value": {"type": "click"}}
EOF

echo "✅ Created valid JSONL file with stream fields"

# Test 2: Invalid JSONL missing stream field
echo "Test 2: Invalid JSONL missing stream field"
cat > "$TEST_DIR/invalid.jsonl" << 'EOF'
{"key": "user1", "value": {"name": "Alice"}}
{"stream": "events", "key": "event1", "value": {"type": "click"}}
EOF

echo "✅ Created invalid JSONL file (first record missing stream field)"

# Test 3: Valid JSONL with all optional fields
echo "Test 3: Valid JSONL with all optional fields"
cat > "$TEST_DIR/complete.jsonl" << 'EOF'
{"stream": "users", "key": "user1", "timestamp": 1700000000000, "type_tag": 9, "flags": 1, "value": {"name": "Alice", "age": 30}}
{"stream": "users", "key": "user2", "timestamp": 1700000001000, "type_tag": 9, "flags": 0, "value": {"name": "Bob", "age": 25}}
EOF

echo "✅ Created complete JSONL file with all fields"

echo ""
echo "Test files created in: $TEST_DIR"
echo ""
echo "You can test the validation with:"
echo "  # This should pass validation"
echo "  cargo run --bin liven -- import --path $TEST_DIR/valid.jsonl --dry-run"
echo ""
echo "  # This should fail validation (missing stream field)"
echo "  cargo run --bin liven -- import --path $TEST_DIR/invalid.jsonl --dry-run"
echo ""
echo "  # This should pass validation (complete records)"
echo "  cargo run --bin liven -- import --path $TEST_DIR/complete.jsonl --dry-run"

# Clean up
# rm -rf "$TEST_DIR"

echo ""
echo "Test script completed successfully!"
