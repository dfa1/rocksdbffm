#!/usr/bin/env bash
# Build RocksDB shared library for Windows using CMake + zig cc/c++ and
# install it into the caller's resources directory so Maven bundles it in
# the JAR.
#
# RocksDB's POSIX Makefile (used by build-rocksdb.sh) has no Windows target,
# so Windows goes through RocksDB's CMake build instead, with zig cc/c++
# acting as a MinGW-w64-compatible cross compiler (target triple
# <arch>-windows-gnu). CMake requires CC/CXX/AR/RANLIB to be single
# executables (unlike `make`, which accepts `CC="zig cc -target ..."`
# directly); see the comments below for how each is resolved differently on
# a POSIX vs. a native Windows host.
#
# Usage:
#   ./scripts/build-rocksdb-windows.sh <output-resources-dir> <target-classifier>
#
# target-classifier: windows-x86_64 | windows-aarch64
#
# Example (Maven exec plugin):
#   ./scripts/build-rocksdb-windows.sh /path/to/native/windows-x86_64/src/main/resources windows-x86_64
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
JOBS="${ROCKSDB_BUILD_JOBS:-$(sysctl -n hw.logicalcpu 2>/dev/null || nproc 2>/dev/null || echo "${NUMBER_OF_PROCESSORS:-4}")}"
LIB_NAME="librocksdb.dll"

# ---------------------------------------------------------------------------
# Map classifier → (zig target triple, CMake system processor)
# ---------------------------------------------------------------------------
case "$CLASSIFIER" in
    windows-x86_64)
        ZIG_TARGET="x86_64-windows-gnu"
        CMAKE_PROCESSOR="AMD64"
        ;;
    windows-aarch64)
        ZIG_TARGET="aarch64-windows-gnu"
        CMAKE_PROCESSOR="ARM64"
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
    echo "[build-rocksdb-windows] $DEST_DIR/$LIB_NAME already exists, skipping build."
    exit 0
fi

echo "[build-rocksdb-windows] Building RocksDB $CLASSIFIER with CMake + zig cc/c++ (jobs=$JOBS)..."

# ---------------------------------------------------------------------------
# zig invokes its C/C++ compiler as a subcommand ("zig cc"/"zig c++"), but
# CMAKE_C_COMPILER/CMAKE_CXX_COMPILER must each be a single executable with
# no baked-in subcommand.
#
# When this script itself runs on a native Windows host (e.g. building
# windows-x86_64 on a windows-latest runner), avoid a wrapper *script*
# entirely for CC/CXX: a batch-file wrapper hits a longstanding cmd.exe
# quoting bug where `cmd /C` mis-parses a command line with more than one
# quoted segment, silently dropping the opening quote around the .bat path
# and gluing it to the next argument ("cxx.bat" "-DNOMINMAX" ... becomes one
# unrecognized token) — reproduced in CI regardless of whether the
# underlying generator is Ninja or GNU Make, so it's cmd.exe itself, not the
# build tool. CMake's CMAKE_<LANG>_COMPILER_ARG1 (originally meant for
# ccache/distcc-style wrapping) sidesteps this: it inserts a fixed first
# argument after the compiler on every invocation, so CMAKE_C_COMPILER can
# point straight at the real zig(.exe) binary with no intermediary script
# for CreateProcess to launch. Off Windows, keep the proven POSIX
# shell-script wrapper (unaffected by any of this).
# ---------------------------------------------------------------------------
IS_WINDOWS_HOST=false
case "$(uname -s)" in
    MINGW* | MSYS* | CYGWIN*) IS_WINDOWS_HOST=true ;;
esac

CMAKE_COMPILER_ARGS=()
if [ "$IS_WINDOWS_HOST" = true ]; then
    ZIG_EXE="$(command -v zig)"
    # Git Bash's `command -v` resolves zig.exe but reports the path without
    # the extension; CMake checks for a literal existing file at the given
    # path (no PATHEXT-style resolution the way a shell does), so append it
    # back when present.
    if [ -f "${ZIG_EXE}.exe" ]; then
        ZIG_EXE="${ZIG_EXE}.exe"
    fi
    CC_COMPILER="$ZIG_EXE"
    CXX_COMPILER="$ZIG_EXE"
    CMAKE_COMPILER_ARGS=(
        -DCMAKE_C_COMPILER_ARG1="cc -target $ZIG_TARGET"
        -DCMAKE_CXX_COMPILER_ARG1="c++ -target $ZIG_TARGET"
    )
    # AR *is* invoked even for the shared-lib target: CMake's Makefile
    # generator bundles objects into an intermediate CMakeFiles/.../objects.a
    # via `ar qc ... @objects1.rsp`, then links the shared lib with
    # `-Wl,--whole-archive objects.a`. There's no CMAKE_AR_ARG1 equivalent to
    # point straight at zig(.exe) the way CC/CXX do, so a wrapper is
    # unavoidable.
    #
    # A plain .bat wrapper was tried here once before (see #39) and failed
    # in CI with "The system cannot find the path specified", assumed at the
    # time to be the same cmd.exe multi-quoted-segment bug CC/CXX hit. That
    # prior attempt embedded $ZIG_EXE untranslated, though: Git Bash's MSYS
    # path form (e.g. /c/hostedtoolcache/...) only auto-translates to native
    # Windows form when passed as a *command-line argument* to a native
    # executable, not when written into a generated file's text — the
    # cmd.exe theory was never confirmed, and cygpath -m below (drive-letter,
    # forward-slash "mixed" form, safe with no backslash-escaping) was the
    # actual fix. Confirmed in CI for both windows-x86_64 and
    # windows-aarch64 -- a compiled C stub was never necessary.
    ZIG_EXE_WIN="$(cygpath -m "$ZIG_EXE")"
    WRAPPER_DIR="$(mktemp -d)"
    trap 'rm -rf "$WRAPPER_DIR"' EXIT
    AR_WRAPPER="$WRAPPER_DIR/ar.bat"
    RANLIB_WRAPPER="$WRAPPER_DIR/ranlib.bat"
    cat > "$AR_WRAPPER" <<EOF
@echo off
"$ZIG_EXE_WIN" ar %*
EOF
    cat > "$RANLIB_WRAPPER" <<EOF
@echo off
"$ZIG_EXE_WIN" ranlib %*
EOF
else
    WRAPPER_DIR="$(mktemp -d)"
    trap 'rm -rf "$WRAPPER_DIR"' EXIT
    CC_COMPILER="$WRAPPER_DIR/cc"
    CXX_COMPILER="$WRAPPER_DIR/cxx"
    AR_WRAPPER="$WRAPPER_DIR/ar"
    RANLIB_WRAPPER="$WRAPPER_DIR/ranlib"
    cat > "$CC_COMPILER" <<EOF
#!/usr/bin/env bash
exec zig cc -target $ZIG_TARGET "\$@"
EOF
    cat > "$CXX_COMPILER" <<EOF
#!/usr/bin/env bash
exec zig c++ -target $ZIG_TARGET "\$@"
EOF
    cat > "$AR_WRAPPER" <<'EOF'
#!/usr/bin/env bash
exec zig ar "$@"
EOF
    cat > "$RANLIB_WRAPPER" <<'EOF'
#!/usr/bin/env bash
exec zig ranlib "$@"
EOF
    chmod +x "$CC_COMPILER" "$CXX_COMPILER" "$AR_WRAPPER" "$RANLIB_WRAPPER"
fi

# ---------------------------------------------------------------------------
# ZSTD/LZ4: build hermetically for this Windows target, same recipe and
# rationale as build-rocksdb.sh (see there for the long version). CMake's
# WITH_ZSTD/WITH_LZ4 normally expect find_package(zstd)/find_package(lz4) to
# locate a host-installed copy via Findzstd.cmake/Findlz4.cmake -- there is
# no such thing in this hermetic cross-compile, and zig cc's linker has no
# dynamic-library search fallback to find one even if there were. Build the
# same static archives build-rocksdb.sh already builds for the POSIX
# classifiers, just targeting this Windows triple, then pre-seed the exact
# cache variables those Find modules populate (ZSTD_INCLUDE_DIRS/
# ZSTD_LIBRARIES, lz4_INCLUDE_DIRS/lz4_LIBRARIES -- casing matches each
# module's own convention), pointed straight at the extracted source lib/
# dirs (which is what build-rocksdb.sh's own -I flags already do) rather
# than a curated header copy -- zstd.h #includes a sibling zstd_errors.h
# from the same directory, so copying just zstd.h out on its own breaks the
# build. find_path/find_library are no-ops once their target variable is
# already cached, so this sidesteps entirely trusting find_library's
# NAME-based .lib/.dll-vs-.a suffix guessing to do the right thing for a
# MinGW-ABI target CMake has no built-in platform file for.
#
# Requires GNU make; skip gracefully (no compression, matching the previous
# behavior) if it is not on PATH -- e.g. a native Windows host without Git
# Bash's bundled make.
# ---------------------------------------------------------------------------
ZSTD_LZ4_CMAKE_ARGS=(-DWITH_ZSTD=OFF -DWITH_LZ4=OFF)
if command -v make >/dev/null 2>&1; then
    if [ "$IS_WINDOWS_HOST" = true ]; then
        ZSTD_LZ4_MAKE_CC="$ZIG_EXE cc -target $ZIG_TARGET"
        ZSTD_LZ4_MAKE_CXX="$ZIG_EXE c++ -target $ZIG_TARGET"
    else
        ZSTD_LZ4_MAKE_CC="$CC_COMPILER"
        ZSTD_LZ4_MAKE_CXX="$CXX_COMPILER"
    fi

    # lz4's lib/Makefile compiles all of its sources in a single
    # `$(CC) ... -c file1.c file2.c ...` invocation with no per-file -o,
    # relying on the compiler auto-naming each output basename.o in the
    # CWD -- standard clang/gcc behavior. zig cc's Windows/COFF-target
    # driver does not support it: reproduced directly, `zig cc -target
    # x86_64-windows-gnu -c a.c b.c` (no -o) fails with "coff does not
    # support linking multiple objects into one", the same error the full
    # build hit. zstd's own Makefile already compiles one file at a time
    # with an explicit -o and is unaffected. Detect the multi-source/no -o
    # shape here and loop instead, degrading to what should have happened
    # anyway -- one compile per source, explicit -o each time.
    ZSTD_LZ4_CC_WRAPPER="$WRAPPER_DIR/zstd-lz4-cc"
    cat > "$ZSTD_LZ4_CC_WRAPPER" <<EOF
#!/usr/bin/env bash
set -euo pipefail
has_c=false
has_o=false
sources=()
flags=()
for arg in "\$@"; do
    case "\$arg" in
        -c) has_c=true; flags+=("\$arg") ;;
        -o) has_o=true; flags+=("\$arg") ;;
        *.c) sources+=("\$arg") ;;
        *) flags+=("\$arg") ;;
    esac
done
if [ "\$has_c" = true ] && [ "\$has_o" = false ] && [ "\${#sources[@]}" -gt 1 ]; then
    for src in "\${sources[@]}"; do
        base="\$(basename "\$src")"
        $ZSTD_LZ4_MAKE_CC "\${flags[@]}" "\$src" -o "\${base%.c}.o"
    done
else
    $ZSTD_LZ4_MAKE_CC "\$@"
fi
EOF
    chmod +x "$ZSTD_LZ4_CC_WRAPPER"
    ZSTD_LZ4_MAKE_CC="$ZSTD_LZ4_CC_WRAPPER"

    (
        cd "$ROCKSDB_DIR"
        # Cross-compilation: a make_config.mk left by a previous classifier's
        # build-rocksdb.sh run in this same checkout was generated for a
        # different target; regenerate it, and let the parameter change
        # through (these targets never touch RocksDB's own object dir).
        rm -f make_config.mk
        # libzstd.a/liblz4.a are plain file targets in rocksdb/Makefile, not
        # .PHONY -- if a previous classifier's build-rocksdb.sh run in this
        # same checkout already left a libzstd.a newer than its tarball
        # prerequisite, make sees it as "up to date" and silently skips
        # rebuilding it for *this* target, reusing the wrong-architecture
        # archive with no error (confirmed locally: a stale macOS-built
        # libzstd.a linked "successfully" against a Windows target and
        # failed at the final DLL link with pages of undefined ZSTD_*/LZ4_*
        # symbols). build-rocksdb.sh dodges this by running `make clean`
        # first, which cascades into clean-ext-libraries-all; run just that
        # narrower target here for the same effect without also touching
        # RocksDB's own (irrelevant to this CMake build) object dir.
        make clean-ext-libraries-all
        # libzstd.a's recipe does a plain, hardcoded `tar xvzf
        # zstd-*.tar.gz` (vendored, not ours to edit). zstd's tarball ships
        # a few CLI-alias symlinks under tests/cli-tests/bin/ (e.g.
        # unzstd -> zstd); creating those requires a privilege Windows
        # doesn't grant by default, so on the native Windows host that tar
        # invocation exits nonzero and make aborts the whole recipe --
        # confirmed in CI, even though everything actually needed (lib/)
        # had already extracted fine first. A wrapper shadowing `tar` on
        # PATH was tried first and did not take effect (still failed
        # identically) -- rather than chase why, repack the tarball
        # ourselves first, stripping the one directory that isn't needed to
        # build libzstd.a anyway, so the vendored recipe's own tar
        # invocation has nothing left to trip over.
        if [ "$IS_WINDOWS_HOST" = true ]; then
            # Root cause of every earlier attempt at this fix silently not
            # taking effect (confirmed with a full `set -x` trace): deriving
            # the tarball name via `make -n libzstd.a` was never a safe dry
            # run for this target. GNU Make's documented -n/-t/-q exception
            # -- a recipe line containing $(MAKE) always executes for real,
            # even under -n, to support recursive make -- applies here: the
            # 3rd line of libzstd.a's own recipe is `cd zstd-.../lib && ...
            # $(MAKE) ... libzstd.a`, which invokes $(MAKE). So the "dry
            # run" was actually attempting the cd for real on every attempt,
            # before the tarball had ever been extracted, and failing there
            # -- while its two prior lines (rm -rf / tar xvzf) genuinely
            # were skipped as text-only, which is why no download/extract
            # output ever appeared even though ZSTD_TARBALL still ended up
            # with the right value (grep matched the printed-but-not-run
            # tar line's text). Sidestep entirely: hardcode the filename --
            # the rocksdb submodule is pinned (v11.8.1), so this is stable
            # until that pin is deliberately bumped.
            ZSTD_TARBALL="zstd-1.5.7.tar.gz"
            make "$ZSTD_TARBALL"
            ZSTD_REPACK_DIR="$(mktemp -d)"
            # Extract with tar's own exit code ignored: the one symlink it
            # can't create on Windows is the only thing that makes it
            # nonzero -- confirmed in the first CI attempt, tar extracts
            # every other file first (all of lib/, all that's actually
            # needed) before reporting failure at the very end.
            tar xzf "$ZSTD_TARBALL" -C "$ZSTD_REPACK_DIR" || true
            ZSTD_TOPDIR="$(ls "$ZSTD_REPACK_DIR")"
            if [ -z "$ZSTD_TOPDIR" ] || [ ! -d "$ZSTD_REPACK_DIR/$ZSTD_TOPDIR/lib" ]; then
                echo "[build-rocksdb-windows] ERROR: zstd extraction produced no usable lib/ directory" >&2
                exit 1
            fi
            rm -rf "${ZSTD_REPACK_DIR:?}/$ZSTD_TOPDIR/tests"
            (cd "$ZSTD_REPACK_DIR" && tar czf "$ROCKSDB_DIR/$ZSTD_TARBALL" "$ZSTD_TOPDIR")
            rm -rf "$ZSTD_REPACK_DIR"
        fi
        # rocksdb/Makefile's own libzstd.a recipe already targets just the
        # static lib, but its liblz4.a recipe invokes lz4's `all`, which
        # also builds the *shared* liblz4 -- and that step fails cross-
        # compiling to a COFF/Windows target ("coff does not support
        # linking multiple objects into one", an lld COFF-driver limitation
        # that plain ELF/Mach-O linking on the POSIX classifiers never hits).
        # BUILD_SHARED=no is lz4's own lib/Makefile switch for this, but that
        # Makefile sets it with plain `:=`, which unconditionally overwrites
        # anything inherited from the environment -- confirmed locally,
        # `export BUILD_SHARED=no` had no effect. A command-line-origin make
        # variable outranks even a `:=` makefile assignment (short of an
        # explicit `override` there, which lz4's Makefile does not use), and
        # command-line variables propagate to recursive $(MAKE) calls
        # automatically via MAKEFLAGS, so pass it on the invocation instead.
        # RANLIB was first tried the same way as CC/CXX/AR here (an
        # environment-variable prefix on the `make` invocation) and that
        # was not enough -- confirmed in CI, still failed identically
        # ("C:\Strawberry\c\bin\ccache: command not found" building
        # liblz4.a). Same root cause as BUILD_SHARED above: lz4's own
        # lib/Makefile reassigns RANLIB itself, and a plain environment
        # variable loses to *any* makefile assignment (even non-`:=`) by
        # GNU Make's default precedence -- only a command-line-origin
        # variable always wins. AR apparently isn't reassigned there (an
        # env-var AR keeps working), but pass everything as command-line
        # variables here for a uniform, precedence-proof invocation.
        echo "[build-rocksdb-windows] before libzstd.a/liblz4.a build:"
        ls -la "$ROCKSDB_DIR"/zstd* "$ROCKSDB_DIR"/lz4* 2>&1 || true
        # DEBUG_LEVEL=0: this target name isn't among the ones rocksdb/Makefile
        # auto-forces to 0 (clean/release/install/shared_lib/...), so it would
        # otherwise fall through to the file's debug default (unoptimized,
        # assertions on) and silently ship a slower zstd/lz4 inside an
        # otherwise-release CMake build.
        make libzstd.a liblz4.a BUILD_SHARED=no ALLOW_BUILD_PARAMETER_CHANGE=1 DEBUG_LEVEL=0 \
            CC="$ZSTD_LZ4_MAKE_CC" CXX="$ZSTD_LZ4_MAKE_CXX" AR="$AR_WRAPPER" \
            RANLIB="$RANLIB_WRAPPER" -j"$JOBS"
    )
    ZSTD_SRC_DIR="$(ls -d "$ROCKSDB_DIR"/zstd-*/ | head -1)"
    LZ4_SRC_DIR="$(ls -d "$ROCKSDB_DIR"/lz4-*/ | head -1)"

    ZSTD_LZ4_CMAKE_ARGS=(
        -DWITH_ZSTD=ON -DWITH_LZ4=ON
        -DZSTD_INCLUDE_DIRS="${ZSTD_SRC_DIR}lib"
        -DZSTD_LIBRARIES="$ROCKSDB_DIR/libzstd.a"
        -Dlz4_INCLUDE_DIRS="${LZ4_SRC_DIR}lib"
        -Dlz4_LIBRARIES="$ROCKSDB_DIR/liblz4.a"
    )
else
    echo "[build-rocksdb-windows] 'make' not found on PATH; building without ZSTD/LZ4." >&2
fi

# ---------------------------------------------------------------------------
# Pick a CMake generator the host actually has. GitHub's windows-latest
# runner has Ninja preinstalled; macOS/Linux runners have `make` via their
# standard toolchain. Prefer make where available since it's already the
# tool build-rocksdb.sh relies on, and fall back to Ninja everywhere else.
# ---------------------------------------------------------------------------
if command -v make >/dev/null 2>&1; then
    CMAKE_GENERATOR="Unix Makefiles"
elif command -v ninja >/dev/null 2>&1; then
    CMAKE_GENERATOR="Ninja"
else
    echo "Neither 'make' nor 'ninja' found on PATH; cannot configure the CMake build." >&2
    exit 1
fi

# ---------------------------------------------------------------------------
# Configure + build. ZSTD/LZ4 come from $ZSTD_LZ4_CMAKE_ARGS above; Snappy/
# Zlib/liburing stay disabled to keep the cross-compile otherwise hermetic
# (same as build-rocksdb.sh). Tests/tools are disabled since only the shared
# lib is needed. FAIL_ON_WARNINGS is off because zig/MinGW headers trigger
# warnings RocksDB's own CMake build does not expect.
# ---------------------------------------------------------------------------
BUILD_DIR="$(mktemp -d)"
trap 'rm -rf "$WRAPPER_DIR" "$BUILD_DIR"' EXIT

cmake -S "$ROCKSDB_DIR" -B "$BUILD_DIR" -G "$CMAKE_GENERATOR" \
    -DCMAKE_SYSTEM_NAME=Windows \
    -DCMAKE_SYSTEM_PROCESSOR="$CMAKE_PROCESSOR" \
    -DCMAKE_C_COMPILER="$CC_COMPILER" \
    -DCMAKE_CXX_COMPILER="$CXX_COMPILER" \
    "${CMAKE_COMPILER_ARGS[@]+"${CMAKE_COMPILER_ARGS[@]}"}" \
    -DCMAKE_AR="$AR_WRAPPER" \
    -DCMAKE_RANLIB="$RANLIB_WRAPPER" \
    -DCMAKE_BUILD_TYPE=Release \
    -DPORTABLE=1 \
    -DROCKSDB_BUILD_SHARED=ON \
    "${ZSTD_LZ4_CMAKE_ARGS[@]}" \
    -DWITH_SNAPPY=OFF -DWITH_ZLIB=OFF -DWITH_LIBURING=OFF \
    -DWITH_TESTS=OFF -DWITH_TOOLS=OFF -DWITH_BENCHMARK_TOOLS=OFF -DWITH_CORE_TOOLS=OFF -DWITH_TRACE_TOOLS=OFF \
    -DFAIL_ON_WARNINGS=OFF

cmake --build "$BUILD_DIR" --target rocksdb-shared -j"$JOBS"

# ---------------------------------------------------------------------------
# Install. CMake's default Windows/GNU naming is "librocksdb-shared.dll"
# (RocksDB's OUTPUT_NAME override to "rocksdb.dll" only applies when
# NOT WIN32), so rename it to match the librocksdb.<ext> convention used by
# every other classifier.
# ---------------------------------------------------------------------------
cp "$BUILD_DIR/librocksdb-shared.dll" "$DEST_DIR/$LIB_NAME"
echo "[build-rocksdb-windows] Installed: $DEST_DIR/$LIB_NAME"
