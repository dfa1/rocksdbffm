#!/usr/bin/env bash
# Build RocksDB shared library using zig cc/c++ and install it into
# src/main/resources/native/<classifier>/ so Maven bundles it in the JAR.
#
# Usage:
#   ./scripts/build-rocksdb.sh            # native platform
#   ./scripts/build-rocksdb.sh --jobs 8   # override parallelism
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ROCKSDB_DIR="$PROJECT_DIR/rocksdb"
JOBS="${ROCKSDB_BUILD_JOBS:-$(sysctl -n hw.logicalcpu 2>/dev/null || nproc)}"

# ---------------------------------------------------------------------------
# Detect platform classifier  (osx-aarch64 | osx-x86_64 | linux-x86_64 …)
# ---------------------------------------------------------------------------
OS=$(uname -s)
ARCH=$(uname -m)
case "$OS" in
    Darwin) OS_NAME="osx"   ; LIB_NAME="librocksdb.dylib" ;;
    Linux)  OS_NAME="linux" ; LIB_NAME="librocksdb.so"    ;;
    *)      echo "Unsupported OS: $OS"; exit 1             ;;
esac
case "$ARCH" in
    arm64|aarch64) ARCH_NAME="aarch64" ;;
    x86_64)        ARCH_NAME="x86_64"  ;;
    *)             echo "Unsupported arch: $ARCH"; exit 1  ;;
esac
CLASSIFIER="${OS_NAME}-${ARCH_NAME}"

# ---------------------------------------------------------------------------
# Destination: bundled into the JAR via Maven resources
# ---------------------------------------------------------------------------
DEST_DIR="$PROJECT_DIR/src/main/resources/native/$CLASSIFIER"
mkdir -p "$DEST_DIR"

# Skip if already built (CI cache or repeated local builds)
if [ -f "$DEST_DIR/$LIB_NAME" ]; then
    echo "[build-rocksdb] $DEST_DIR/$LIB_NAME already exists, skipping build."
    exit 0
fi

# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------
echo "[build-rocksdb] Building RocksDB $CLASSIFIER with zig cc/c++ (jobs=$JOBS)..."

export CC="zig cc"
export CXX="zig c++"
export PORTABLE=1

cd "$ROCKSDB_DIR"
make shared_lib -j"$JOBS"

# ---------------------------------------------------------------------------
# Install
# ---------------------------------------------------------------------------
cp "$ROCKSDB_DIR/$LIB_NAME" "$DEST_DIR/$LIB_NAME"
echo "[build-rocksdb] Installed: $DEST_DIR/$LIB_NAME"
