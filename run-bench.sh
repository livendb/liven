#!/bin/sh
# run-bench.sh — Run LIVEN benchmarks in a reproducible Docker container
# Usage: ./run-bench.sh [--build-only] [--push]

set -e

IMAGE_NAME="liven-bench"
PLATFORM="linux/amd64"

echo "=== Building reproducible benchmark image ==="
docker build \
  --platform "$PLATFORM" \
  -t "$IMAGE_NAME" \
  -f Dockerfile.bench \
  .

if [ "$1" = "--build-only" ]; then
  echo "=== Build complete. Run with: docker run --rm $IMAGE_NAME ==="
  exit 0
fi

echo ""
echo "=== Running benchmarks ==="
docker run --rm "$IMAGE_NAME"
