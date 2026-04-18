# AGENTS.md: Project Context & AI-Driven Guidelines

## 🤖 AI-Driven Project Mandate

This project is heavily AI-driven. As an agent, your goal is to:

- **Be Autonomous:** Research C headers (rocksdb/include/rocksdb/c.h) and identify the best mapping to Java FFM.
- **Stay Technical:** Prioritize performance, zero-copy, and manual memory safety.
- **Maintain Consistency:** Follow established naming and ownership patterns.

## 🛠 Tech Stack

- **Language:** Java 25+.
- **Core API:** `java.lang.foreign` (Foreign Function & Memory API).
- **Native Library:** RocksDB (C API via `include/rocksdb/c.h`), built from the `rocksdb/` git submodule (pinned to
  v10.10.1).
- **Native Compiler:** `zig cc` / `zig c++` — used as a drop-in C/C++ compiler via
  `CC="zig cc" CXX="zig c++" PORTABLE=1 make shared_lib`. Zig bundles clang + libc++ for every target, enabling
  cross-compilation without a separate sysroot.
- **Build System:** Maven Wrapper (`./mvnw`). Run `./mvnw generate-resources -Pnative-build` once to build the native lib; `./mvnw test`
  thereafter. Use `./mvnw` (not `mvn`) to ensure the correct Maven version is used.
- **Testing:** JUnit 5, AssertJ.
- **Benchmarking:** JMH (Java Microbenchmark Harness).

## 🏗 Architectural Standards

### 1. Manual Memory Management & Lifecycle

Every class wrapping a native pointer **must** implement `AutoCloseable`.

- **Zero Leaks:** Native resources must be destroyed in `close()`.
- **Ownership Transfer:** When one native object takes ownership of another (e.g., `FilterPolicy` →
  `BlockBasedTableConfig`), the transferring object must mark the ownership as transferred using a boolean flag. Its
  `close()` method should then become a no-op to prevent double-frees.
- **Transfer Marker:** Use a method like `transferOwnership()` inside the setter that takes ownership.

### 2. Data Types & Path Handling

To ensure type safety and consistent units across the API:

- **C API Only:** We use the RocksDB C interface (`rocksdb/c.h`). Do not attempt to link directly to C++ symbols.
- **Read-only headers:** NEVER modify system include files (e.g. `/opt/homebrew/...`, `/usr/include/...`). They are
  read-only references; all mappings live in Java source.
- **Library loading:** `RocksDB.java` loads the native library from the classpath resource
  `/native/<os>-<arch>/librocksdb.<ext>` (bundled by the `native-build` Maven profile). There is no brew/system
  fallback. NEVER add hardcoded system paths back.
- **Paths:** Never use raw `String` for file system paths. Always use `java.nio.file.Path` for any API surface that
  accepts paths (open, backup, checkpoint).
- **Memory Sizes:** Never use raw `long` for byte counts (e.g., cache size, write buffer size). Always use the project's
  `MemorySize` type.
- **Sequence Numbers:** Never use raw `long` for RocksDB sequence numbers. Always use the project's `SequenceNumber`
  type.
- **BackupId:**: Never use raw uint32, use a wrapper Java type that hides this from the user.

### 3. API Surface Design

For every feature, provide three tiers of access:

1. **`MemorySegment` Version:** Native-first, for performance-critical usage.
2. **`ByteBuffer` Version:** For compatibility with existing NIO-based clients.
3. **`byte[]` Version:** Quick access for convenience (explicitly documented as slower).

## ⚡ FFM Performance & Patterns

### 1. Centralized Error Handling

**NEVER use ThreadLocals for error pointers.** Use the centralized `Native` utility with the caller's `Arena`:

```java
try (Arena arena = Arena.ofConfined()) {
    MemorySegment err = Native.errHolder(arena);
    MH_DO_SOMETHING.invokeExact(handle, ..., err);
    Native.checkError(err);
}
```

### 2. Zero-Copy Patterns

- **PinnableSlice:** Use `rocksdb_get_pinned` for reads to avoid intermediate copies from the block cache.
- **Direct Buffers:** Use `MemorySegment.ofBuffer(directByteBuffer)` to wrap existing native memory without copies.

## 🧪 Validation & Workflow

### 1. Comparative Testing

For every new feature:

1. Write unit tests in JUnit 5 using `@TempDir`.
2. **Always** follow the `// Given / // When / // Then` structure — every test, no exceptions.
   - `// Given` sets up state.
   - `// When` performs the action under test — **never combine with `// Then`**. Extract the result into a local variable:
     ```java
     // When
     var result = db.get("k".getBytes());

     // Then
     assertThat(result).isEqualTo("v".getBytes());
     ```
   - `// Then` asserts the outcome. The assertion always operates on the variable captured in `// When`, never inline.
   - For void actions (`flush`, `put`, …) there is no return value to capture; just place the call under `// When` and put assertions (if any) under `// Then`.
   - For tests with no meaningful setup, use `// Given` with a blank line or a comment explaining why there is none.
3. **Run tests:** `./mvnw test`

### 2. Benchmark First

Performance gains are a primary goal. Use `JMH` to validate changes.

- **Run benchmarks:**
  ```bash
  ./mvnw test-compile -q
  ./scripts/benchmark.sh
  ```
  This builds everything, runs both FFM and JNI suites, and prints a side-by-side comparison table.

## 🗺 Source Map

For the full feature status and roadmap see `README.md`.

| Feature                 | Java source files                                                                                                         |
|:------------------------|:--------------------------------------------------------------------------------------------------------------------------|
| DB Open/Close/Put/Get/Delete | `RocksDB.java`                                                                                                       |
| Options                 | `Options.java`, `ReadOptions.java`, `WriteOptions.java`                                                                   |
| WriteBatch              | `WriteBatch.java`                                                                                                         |
| Transactions            | `Transaction.java`, `TransactionDB.java`, `TransactionDBOptions.java`, `TransactionOptions.java`                          |
| Optimistic Transactions | `OptimisticTransactionDB.java`, `OptimisticTransactionOptions.java`                                                       |
| Checkpoints             | `Checkpoint.java`                                                                                                         |
| Table Options           | `BlockBasedTableConfig.java`, `LRUCache.java`, `FilterPolicy.java`                                                        |
| Iterators               | `RocksIterator.java`                                                                                                      |
| Snapshots               | `Snapshot.java`; `ReadOptions.setSnapshot`; `RocksDB.getSnapshot`, `TransactionDB.getSnapshot`, `Transaction.getSnapshot` |
| Flush                   | `FlushOptions.java`; `RocksDB.flush`, `RocksDB.flushWal`, `TransactionDB.flush`, `TransactionDB.flushWal`                 |
| KeyMayExist             | `RocksDB.keyMayExist` — byte[], ByteBuffer, MemorySegment, ReadOptions overload                                           |
| DB Properties           | `DBProperty.java` (enum of well-known names); `RocksDB.getProperty`, `RocksDB.getLongProperty`, same on `TransactionDB`   |
| Statistics              | `HistogramType.java`, `TickerType.java`, `StatsLevel.java`, `StatisticsHistogramData.java`                                |
| Shared utilities        | `Native.java` (`errHolder`, `checkError`, `toNative`), `MemorySize.java`, `RocksDBException.java`                         |
| Compaction control      | `CompactOptions.java`; `RocksDB.compactRange`, `suggestCompactRange`, `disableFileDeletions`, `enableFileDeletions`       |
| Secondary DB            | `SecondaryDB.java`                                                                                                        |
| Blob DB                 | `BlobDB.java`; blob options in `Options.java`; `PrepopulateBlobCache.java`; `RocksDB.openWithBlobFiles`                   |
| Rate Limiter            | `RateLimiter.java`; `Options.setRateLimiter`                                                                              |
| SST File Manager        | `SstFileManager.java`, `Env.java`; `Options.setSstFileManager`, `Options.setEnv`                                          |
| Backup Engine           | `BackupEngine.java`, `BackupEngineOptions.java`, `RestoreOptions.java`, `BackupInfo.java`, `BackupId.java`                |
| Column Families         | `ColumnFamilyHandle.java`, `ColumnFamilyDescriptor.java`; `RocksDB.openWithColumnFamilies`, `listColumnFamilies`; CF overloads on `ReadWriteDB` and `WriteBatch`; CF overloads on `ReadOnlyDB`, `TtlDB`, `TransactionDB`, `OptimisticTransactionDB`; `Transaction` CF put/delete/get/getForUpdate/newIterator; multi-CF open for all DB types |
| Perf Context            | `PerfContext.java`, `PerfLevel.java`, `PerfMetric.java`; thread-local; `setPerfLevel`, `reset`, `metric`, `report`       |

## Documentation

- Javadoc is written in the Markdown format to keep same format everywhere
- some documentation lives in docs/; the intent is to document decisions there

## Code

- code is indented with tabs (enforced by checkstyle)
- always keep the MethodHandles private static final
- every `MH_` field must have a `/// \`<c prototype>\`` comment on the line immediately above it, copied verbatim from `rocksdb/include/rocksdb/c.h` (strip the `extern ROCKSDB_LIBRARY_API` prefix); no duplicate comment in the `static` block
- don't map multiple times the same symbol from C library of rocksdb
    - try to create always a java wrapper for that (i.e. PinnableSlice)
- use NativePointer as base class for all managed objects
    - this is needed to avoid double close() crashing the JVM
- don't expose public constructors, like CompactOptions.newCompactOptions(), CompactOptions.newCompactOptions()
    - why? to be able to call super in the private constructor and to have more freedom in the static factory method
