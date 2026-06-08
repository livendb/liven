#!/bin/sh
# LIVEN Git Pre-Push Hook
# Automatically verifies code format, clippy lints, and test status before pushing.

echo "======================================================"
echo "⚡ Running LIVEN Pre-Push Verification Suite..."
echo "======================================================"

# 1. Verify Code Formatting
echo "→ Checking code formatting..."
cargo fmt --all -- --check
if [ $? -ne 0 ]; then
    echo "❌ Error: Code formatting check failed."
    echo "   Please run 'cargo fmt --all' before pushing."
    exit 1
fi
echo "✓ Formatting verified!"

# 2. Run Clippy Lints
echo "→ Checking Clippy lints..."
cargo clippy --all-targets -- -D warnings
if [ $? -ne 0 ]; then
    echo "❌ Error: Clippy lints failed."
    echo "   Please fix all warnings or configure #[allow(...)] tags."
    exit 1
fi
echo "✓ Lints verified!"

# 3. Run Automated Tests
echo "→ Checking automated test suite..."
cargo test --all-targets --workspace
if [ $? -ne 0 ]; then
    echo "❌ Error: Test suite execution failed."
    exit 1
fi
echo "✓ Tests verified!"

echo "======================================================"
echo "✨ Pre-push checks completed successfully! Pushing code..."
echo "======================================================"
exit 0
