#!/usr/bin/env bash
# Build RocksDB shared library using zig cc/c++ and install it into the
# caller's resources directory so Maven bundles it in the JAR.
#
# Supports cross-compilation: runs on any host but can produce a binary
# for any supported target by passing a TARGET_CLASSIFIER.
#
# Usage:
#   ./scripts/build-rocksdb.sh <output-resources-dir> <target-classifier>
#
# target-classifier: osx-aarch64 | osx-x86_64 | linux-x86_64
#
# Example (Maven exec plugin):
#   ./scripts/build-rocksdb.sh /path/to/native/osx-aarch64/src/main/resources osx-aarch64
set -euo pipefail

if [ $# -lt 2 ]; then
    echo "Usage: $0 <output-resources-dir> <target-classifier>" >&2
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"   # multi-module root
ROCKSDB_DIR="$PROJECT_DIR/rocksdb"
# Resolve to absolute path before any cd changes the working directory
mkdir -p "$1"
OUTPUT_RESOURCES="$(cd "$1" && pwd)"
CLASSIFIER="$2"
JOBS="${ROCKSDB_BUILD_JOBS:-$(sysctl -n hw.logicalcpu 2>/dev/null || nproc)}"

# ---------------------------------------------------------------------------
# Map classifier → (zig target triple, library name, RocksDB platform)
# ---------------------------------------------------------------------------
case "$CLASSIFIER" in
    osx-aarch64)
        ZIG_TARGET="aarch64-macos"
        LIB_NAME="librocksdb.dylib"
        ROCKSDB_OS="Darwin"
        ;;
    osx-x86_64)
        ZIG_TARGET="x86_64-macos"
        LIB_NAME="librocksdb.dylib"
        ROCKSDB_OS="Darwin"
        ;;
    linux-x86_64)
        ZIG_TARGET="x86_64-linux-gnu"
        LIB_NAME="librocksdb.so"
        ROCKSDB_OS="Linux"
        ;;
    *)
        echo "Unsupported classifier: $CLASSIFIER" >&2
        exit 1
        ;;
esac

DEST_DIR="$OUTPUT_RESOURCES/native/$CLASSIFIER"
mkdir -p "$DEST_DIR"

# Skip if already built (CI cache or repeated local builds)
if [ -f "$DEST_DIR/$LIB_NAME" ]; then
    echo "[build-rocksdb] $DEST_DIR/$LIB_NAME already exists, skipping build."
    exit 0
fi

# ---------------------------------------------------------------------------
# Detect whether we are cross-compiling
# ---------------------------------------------------------------------------
HOST_OS=$(uname -s)
HOST_ARCH=$(uname -m)
case "$HOST_OS" in Darwin) HOST_OS_NAME="osx"   ;; Linux) HOST_OS_NAME="linux" ;; esac
case "$HOST_ARCH" in arm64|aarch64) HOST_ARCH_NAME="aarch64" ;; x86_64) HOST_ARCH_NAME="x86_64" ;; esac
HOST_CLASSIFIER="${HOST_OS_NAME}-${HOST_ARCH_NAME}"

CROSS=""
if [ "$CLASSIFIER" != "$HOST_CLASSIFIER" ]; then
    CROSS=" (cross from $HOST_CLASSIFIER)"
fi

# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------
echo "[build-rocksdb] Building RocksDB $CLASSIFIER$CROSS with zig cc/c++ (jobs=$JOBS)..."

export CC="zig cc -target $ZIG_TARGET"
export CXX="zig c++ -target $ZIG_TARGET"
export PORTABLE=1
# TODO: to have hermetic zig build, disable external libs for now
export ROCKSDB_DISABLE_SNAPPY=1
export ROCKSDB_DISABLE_BZ2=1
export ROCKSDB_DISABLE_ZLIB=1

cd "$ROCKSDB_DIR"

# zig cc/c++ treats some warnings as errors that RocksDB's own build does not
# expect (e.g. -Wunused-parameter in util/compression.cc). Suppress them for
# all builds so the Makefile does not abort on RocksDB's own code.
EXTRA_FLAGS="-Wno-error"

if [ -n "$CROSS" ]; then
    # Cross-compilation: existing .o files and make_config.mk are for the host
    # architecture. Remove them so RocksDB's build_detect_platform regenerates
    # the config and Make recompiles everything with the cross target.
    rm -f make_config.mk
    make clean -j"$JOBS" 2>/dev/null || true

    # build_detect_platform reads TARGET_OS from the environment (falls back to
    # uname -s). Export it so platform detection targets the right OS.
    export TARGET_OS="$ROCKSDB_OS"
fi

make shared_lib EXTRA_CXXFLAGS="$EXTRA_FLAGS" EXTRA_CFLAGS="$EXTRA_FLAGS" -j"$JOBS"

# ---------------------------------------------------------------------------
# Install
# ---------------------------------------------------------------------------
cp "$ROCKSDB_DIR/$LIB_NAME" "$DEST_DIR/$LIB_NAME"
echo "[build-rocksdb] Installed: $DEST_DIR/$LIB_NAME"
