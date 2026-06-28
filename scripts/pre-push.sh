#!/bin/sh
# LIVEN Git Pre-Push Hook
# Verifies the project compiles and passes lints before pushing.

set -e

echo "→ cargo check..."
cargo check

echo "→ cargo clippy..."
cargo clippy -- -D warnings

echo "✓ Pre-push checks passed."
