# Native Library Loading

## Architecture

The native loading model is: **one build, all platforms, runtime dispatch**.

```
build (Zig cross-compiles all targets)
  └── native/osx-aarch64/  → librocksdb.dylib  → JAR resource /native/osx-aarch64/librocksdb.dylib
  └── native/linux-x86_64/ → librocksdb.so     → JAR resource /native/linux-x86_64/librocksdb.so

runtime (NativeLibrary.java)
  └── detect OS + arch → classifier → extract resource → load
```

## Cross-compilation

`scripts/build-rocksdb.sh` accepts a `<target-classifier>` argument and uses
`zig cc / zig c++` with `-target <zig-triple>` to cross-compile for any
supported platform from any host:

| Classifier      | Zig target triple   | Library          |
|-----------------|---------------------|------------------|
| `osx-aarch64`   | `aarch64-macos`     | `librocksdb.dylib` |
| `linux-x86_64`  | `x86_64-linux-gnu`  | `librocksdb.so`  |

Zig bundles clang, libc++, and macOS/Linux SDK headers — no separate sysroot
needed. A single CI job (or local machine) can build all targets.

Each `native/<classifier>` Maven module invokes the script at the
`generate-resources` phase and packages the resulting library as a classpath
resource.

## Runtime dispatch (`NativeLibrary.java`)

On startup, `NativeLibrary` detects the current platform:

```java
String osName  = os.contains("mac") ? "osx" : "linux";
String archName = arch.equals("aarch64") || arch.equals("arm64") ? "aarch64" : "x86_64";
// → e.g. "linux-x86_64"
```

It then loads `/native/<classifier>/librocksdb.<ext>` from the classpath,
extracts it to a temp file, and calls `SymbolLookup.libraryLookup()`. If no
bundled library matches the current platform, an `UnsatisfiedLinkError` is thrown
with a clear message.

An override is available for testing or custom builds:

```
-Drocksdb.lib.path=/path/to/librocksdb.so
```

## Distribution

Consumers add the native module(s) they need as runtime dependencies:

```xml
<!-- macOS Apple Silicon -->
<dependency>
    <groupId>io.github.dfa1</groupId>
    <artifactId>rocksdbffm-native-osx-aarch64</artifactId>
    <scope>runtime</scope>
</dependency>

<!-- Linux x86-64 -->
<dependency>
    <groupId>io.github.dfa1</groupId>
    <artifactId>rocksdbffm-native-linux-x86_64</artifactId>
    <scope>runtime</scope>
</dependency>
```

Including multiple native modules on the classpath at once is safe — `NativeLibrary`
only loads the one matching the current platform.
