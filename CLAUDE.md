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
  v11.8.1).
- **Native Compiler:** `zig cc` / `zig c++` — used as a drop-in C/C++ compiler via
  `CC="zig cc" CXX="zig c++" PORTABLE=1 make shared_lib`. Zig bundles clang + libc++ for every target, enabling
  cross-compilation without a separate sysroot.
- **Build System:** Maven Wrapper (`./mvnw`). `./mvnw generate-resources` (or `test`, `compile`, ...) auto-detects the
  host OS/arch and cross-compiles RocksDB for just that one `native/*` classifier via zig cc — a plain local build no
  longer compiles all 5 targets. Add `-Pall-natives` to build every classifier regardless of host (what CI and
  releases use). Use `./mvnw` (not `mvn`) to ensure the correct Maven version is used.
  **NEVER run `mvn install` or `./mvnw install`** — it pollutes `~/.m2` with local artifacts. Use `compile`, `test`, or `package` instead.
- **Testing:** JUnit 5, AssertJ.
- **Benchmarking:** JMH (Java Microbenchmark Harness).

## 🏗 Architectural Standards

### 1. Manual Memory Management & Lifecycle

Every class wrapping a native pointer **must** implement `AutoCloseable`.

- **Zero Leaks:** Native resources must be destroyed in `close()`.
- **Ownership Transfer:** When one native object takes ownership of another (e.g., `FilterPolicy` →
  `BlockBasedTableOptions`), the transferred object must be marked so its `close()` becomes a no-op and cannot
  double-free.
- **Transfer Marker:** Call `transferOwnership()` (package-private on `NativeObject`) inside the setter that takes
  ownership. It sets the held pointer to `MemorySegment.NULL`, which makes `close()` a no-op and any later `ptr()`
  throw `IllegalStateException`.

### 2. Data Types & Path Handling

To ensure type safety and consistent units across the API:

- **C API Only:** We use the RocksDB C interface (`rocksdb/c.h`). Do not attempt to link directly to C++ symbols.
- **Read-only headers:** NEVER modify system include files (e.g. `/opt/homebrew/...`, `/usr/include/...`). They are
  read-only references; all mappings live in Java source.
- **Library loading:** `NativeLibrary.java` loads the native library from the classpath resource
  `/native/<os>-<arch>/librocksdb.<ext>` (bundled by each `native/*` module's `exec-maven-plugin` execution). There is
  no brew/system fallback. NEVER add hardcoded system paths back.
- **Paths:** Never use raw `String` for file system paths. Always use `java.nio.file.Path` for any API surface that
  accepts paths (open, backup, checkpoint).
- **Memory Sizes:** Never use raw `long` for byte counts (e.g., cache size, write buffer size). Always use the project's
  `MemorySize` type.
- **Sequence Numbers:** Never use raw `long` for RocksDB sequence numbers. Always use the project's `SequenceNumber`
  type.
- **BackupId:**: Never use raw uint32, use a wrapper Java type that hides this from the user.
- **Timeouts:** Never use raw `long` for a timeout field with a negative-sentinel meaning in the C API (e.g. `-1` =
  wait forever/disabled). Use `Duration`, with `null` — not `Duration.ZERO` — as the sentinel, since `ZERO` usually
  already means something else ("fail immediately"). Reject a non-null negative `Duration`. Verify sentinel semantics
  against the actual `rocksdb/utilities/**/*.h`/`.cc` source, not just existing javadoc (it can be wrong); if the C++
  side documents no negative-sentinel meaning, still convert to `Duration` but require it non-null.

### 3. API Surface Design

For every feature, provide three tiers of access:

1. **`MemorySegment` Version:** Native-first, for performance-critical usage.
2. **`ByteBuffer` Version:** For compatibility with existing NIO-based clients.
3. **`byte[]` Version:** Quick access for convenience (explicitly documented as slower).

## ⚡ FFM Performance & Patterns

### 1. Centralized Error Handling

**NEVER use ThreadLocals for error pointers.** Use the shared helpers on `RocksDB` with the caller's `Arena`:

```java
try (Arena arena = Arena.ofConfined()) {
    MemorySegment err = RocksDB.errHolder(arena);
    MH_DO_SOMETHING.invokeExact(handle, ..., err);
    RocksDB.checkError(err);
} catch (Throwable t) {
    throw RocksDB.wrapInvokeFailure("doSomething failed", t);
}
```

`RocksDB.errHolder`, `RocksDB.checkError`, `RocksDB.toNative`, and `RocksDB.wrapInvokeFailure` are the shared FFM
plumbing used by every wrapper class.

**`RocksDBException` is only ever constructed by `RocksDB.checkError`**, for a genuine `errptr`-reported RocksDB
error. Never construct it — or call something you wrote yourself that would — from an `invokeExact` catch block;
use `RocksDB.wrapInvokeFailure(message, t)` there instead, which rethrows any `RuntimeException` (including a
`RocksDBException` `checkError` already threw earlier in the same `try`) unwrapped, an `IOException` as
`UncheckedIOException`, and anything else — which should never happen for a correctly configured downcall handle —
as `AssertionError`. See [ADR 0004](docs/adr/0004-error-handling.md) for why: a bug in this library's own FFM
plumbing must never be indistinguishable from a genuine RocksDB error.

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

### 3. Javadoc

Every public method must have complete Javadoc. The build enforces this via
`failOnError=true` + `failOnWarnings=true` in the `maven-javadoc-plugin`.

Rules:
- Every public method needs a main description, `@param` for each parameter, and `@return` (unless `void`).
- Every public record needs `@param` entries on the class-level doc (one per component).
- Cross-references use `[ClassName#method(ParamType)]` — verify the target exists before writing it. Wrong references are **errors**, not warnings.
- `@see`-only Javadoc counts as "no main description" — always add a prose sentence.

**Check:** `./mvnw javadoc:javadoc -pl core` — must produce zero output.

### 4. Releasing

```bash
./mvnw --batch-mode release:clean release:prepare \
    -DreleaseVersion=<version> \
    -DdevelopmentVersion=<next>-SNAPSHOT
git push && git push --tags
```

GitHub Actions picks up the tag and deploys to Maven Central.

### 5. Benchmark First

Performance gains are a primary goal. Use `JMH` to validate changes.

- **Run benchmarks:**
  ```bash
  ./mvnw test-compile -q
  ./scripts/benchmark.sh
  ```
  This builds everything, runs both FFM and JNI suites, and prints a side-by-side comparison table.

## 🗺 Source Map

For the full feature status and roadmap see `docs/reference.md#feature-status`.

`RocksDB.java` holds **only** the static open/list factories plus the shared FFM helpers; every
instance method listed below lives on the DB type (`ReadWriteDB`, `TtlDB`, …), not on `RocksDB`.

| Feature                 | Java source files                                                                                                         |
|:------------------------|:--------------------------------------------------------------------------------------------------------------------------|
| DB Open/Close/Put/Get/Delete | `RocksDB.java`                                                                                                       |
| Options                 | `Options.java`, `ReadOptions.java`, `WriteOptions.java`                                                                   |
| WriteBatch              | `WriteBatch.java`                                                                                                         |
| Transactions            | `Transaction.java`, `TransactionDB.java`, `TransactionDBOptions.java`, `TransactionOptions.java`                          |
| Optimistic Transactions | `OptimisticTransactionDB.java`, `OptimisticTransactionOptions.java`                                                       |
| Checkpoints             | `Checkpoint.java`                                                                                                         |
| Table Options           | `BlockBasedTableOptions.java`, `Cache.java`, `LRUCache.java`, `HyperClockCache.java`, `FilterPolicy.java`                 |
| Compression             | `CompressionType.java`; `Options.setCompression`, `Options.getCompression`                                                |
| Temperature hints       | `Temperature.java`; 5 setter/getter pairs on `Options` (metadata/WAL/last-level/default-write/default write temperature)  |
| Iterators               | `RocksIterator.java`                                                                                                      |
| Snapshots               | `Snapshot.java`; `ReadOptions.setSnapshot`; `ReadWriteDB.getSnapshot`, `TransactionDB.getSnapshot`, `Transaction.getSnapshot` |
| Flush                   | `FlushOptions.java`; `ReadWriteDB.flush`, `ReadWriteDB.flushWal`, `TransactionDB.flush`, `TransactionDB.flushWal`                 |
| KeyMayExist             | `RocksDBReadOperations.keyMayExist` — byte[], ByteBuffer, MemorySegment, ReadOptions overload; shared across all five direct-DB types (see Shared utilities row)                                           |
| DB Properties           | `Property.java` (enum of well-known names); `RocksDBReadOperations.getProperty`/`getLongProperty`, shared across all five direct-DB types; same methods hand-mapped separately on `TransactionDB`/`OptimisticTransactionDB` |
| Statistics              | `HistogramType.java`, `TickerType.java`, `StatsLevel.java`, `StatisticsHistogramData.java`                                |
| Shared utilities        | `RocksDB.java` statics (`errHolder`, `checkError`, `toNative`), `NativeObject.java`, `NativeLibrary.java`, `MemorySize.java`, `SequenceNumber.java`, `RocksDBException.java`, `UpcallRegistry.java` (id-registry smuggling a Java callback through a native `void*` state pointer; used by `Logger`/`MergeOperator.Custom`); `RocksDBReadOperations.java`/`RocksDBWriteOperations.java` — default-method interfaces sharing the direct put/get/merge/delete/iterate/flush/compaction/CF-management surface. `RocksDBReadOperations` includes CF-scoped reads and is implemented by all five direct-DB types plus `OptimisticTransactionDB` (`ReadWriteDB`/`TtlDB`/`BlobDB`/`ReadOnlyDB`/`SecondaryDB`/`OptimisticTransactionDB` — `RocksDB.openSecondary` has a CF-descriptor overload via `rocksdb_open_as_secondary_column_families`). `RocksDBWriteOperations` does NOT extend the read interface — `ReadWriteDB`/`TtlDB`/`BlobDB`/`OptimisticTransactionDB` implement both directly. Not implemented by `TransactionDB`, whose direct ops bind their own `MethodHandle`s per the no-shared-call-site rule (`rocksdb_transactiondb_put` etc. are genuinely different native symbols); `OptimisticTransactionDB` has no such dedicated C API for direct ops — it always goes through the base `rocksdb_t*` (`NativeObjectWithBaseDb#dbPtr()`), the same calls the other five types already share |
| Compaction control      | `CompactOptions.java`, `WaitForCompactOptions.java`; `RocksDBWriteOperations.compactRange`, `suggestCompactRange`, `disableFileDeletions`, `enableFileDeletions`, `waitForCompact` — shared across `ReadWriteDB`/`TtlDB`/`BlobDB` |
| DeleteRange             | `RocksDBWriteOperations.deleteRange` (all three tiers), shared across `ReadWriteDB`/`TtlDB`/`BlobDB`; `WriteBatch.deleteRange`                                                     |
| Merge                   | `merge()` on `ReadWriteDB`, `TtlDB`, `BlobDB`, `OptimisticTransactionDB`, `TransactionDB`, `Transaction`, `WriteBatch` (all tiers + CF variants); `MergeOperator.java` (`uint64Add()` built-in, `custom(String, FullMergeFn)` callback-based — `fn` receives zero-copy `MemorySegment` views of key/existing-value/operands), wired via `Options.setMergeOperator` |
| SST File Ingest         | `SstFileWriter.java`, `IngestExternalFileOptions.java`; `ReadWriteDB.ingestExternalFile`                                     |
| WAL Iterator            | `WalIterator.java`, `WalBatchResult.java`; `ReadWriteDB.getUpdatesSince`, `getLatestSequenceNumber`                          |
| Read-only DB            | `ReadOnlyDB.java`                                                                                                         |
| TTL DB                  | `TtlDB.java`; `RocksDB.openTtl(Path, Duration)`                                                                       |
| Logger                  | `Logger.java`, `LogLevel.java`; `Logger.newStderrLogger`, `Logger.newCallbackLogger`                                      |
| Secondary DB            | `SecondaryDB.java`                                                                                                        |
| Blob DB                 | `BlobDB.java`; blob options in `Options.java`; `PrepopulateBlobCache.java`; `RocksDB.openBlob`                   |
| Rate Limiter            | `RateLimiter.java`; `Options.setRateLimiter`                                                                              |
| SST File Manager        | `SstFileManager.java`, `Env.java`; `Options.setSstFileManager`, `Options.setEnv`                                          |
| Backup Engine           | `BackupEngine.java`, `BackupEngineOptions.java`, `RestoreOptions.java`, `BackupInfo.java`, `BackupId.java`                |
| Column Families         | `ColumnFamilyHandle.java`, `ColumnFamilyDescriptor.java`; `RocksDB.openReadWrite`, `listColumnFamilies`; CF overloads on `ReadWriteDB` and `WriteBatch`; CF overloads on `ReadOnlyDB`, `TtlDB`, `BlobDB`, `SecondaryDB`, `TransactionDB`, `OptimisticTransactionDB`; `Transaction` CF put/delete/get/getForUpdate/newIterator; multi-CF open for all DB types (including `RocksDB.openSecondary`) |
| Perf Context            | `PerfContext.java`, `PerfLevel.java`, `PerfMetric.java`; thread-local; `setPerfLevel`, `reset`, `metric`, `report`       |
| Event listeners         | `EventNotifier.java` (8 no-op-default callbacks), `EventNotifierBridge.java` (FFM plumbing, one global upcall stub per callback + `UpcallRegistry` id-through-`state`); payload views `FlushJobInfo.java`, `CompactionJobInfo.java`, `ExternalFileIngestionInfo.java`, `MemTableInfo.java`, `WriteStallInfo.java`; enums `FlushReason.java`, `CompactionReason.java`, `BackgroundErrorReason.java`, `WriteStallCondition.java`; wired via `Options.addEventListener`. The two subcompaction callbacks are deliberately not exposed, but still get a shared no-op stub — `db/c.cc` calls them without a null check |
| Upcall thread lifetime  | `BackgroundUpcallThreads.java` — shutdown hook that drains RocksDB's background threads so `System.exit()` cannot deadlock once an upcall has attached one to the JVM. `installShutdownDrain()` from every callback-registration site (`EventNotifierBridge.attach`, `Logger.newCallbackLogger`, `MergeOperator.Custom.create`), `track()` from every upcall dispatch that RocksDB can invoke on a background thread. Any new callback-based feature must call both |

## Documentation

- Javadoc is written in the Markdown format to keep same format everywhere
- `docs/` follows [Diataxis](https://diataxis.fr/) — put new prose in the page matching its mode, and cross-link
  rather than repeat:

  | File                   | Put here                                                                |
  |:-----------------------|:------------------------------------------------------------------------|
  | `docs/tutorial.md`     | The single linear newcomer walkthrough                                  |
  | `docs/how-to.md`       | Task recipes ("how do I take a backup?")                                |
  | `docs/reference.md`    | Artifacts, API surface by area, options/enum tables, feature status     |
  | `docs/explanation.md`  | Design rationale, ownership model, native loading, build decisions      |
  | `docs/benchmarks.md`   | Benchmark numbers and methodology                                        |
  | `docs/c-api-gaps.md`   | Type A/B gap catalogue against `rocksdb/c.h`                            |

- `README.md` links to `docs/` and must not duplicate their content: it holds badges, a minimal quickstart, the docs
  index, contributing, and releasing only
- Java snippets in `docs/` must compile against `core` before being committed

## Code

- American English everywhere (javadoc, comments, identifiers): recognize/optimize/finalize/serialize/normalize/behavior/color — never -ise/-isation/-our. Matches the JDK (Object.finalize, Serializable).
- code is indented with tabs (enforced by checkstyle)
- always keep the MethodHandles private static final
    - never pass a `MethodHandle` as a method parameter, even internally: `invokeExact` on a `static final` field lets the JIT treat the target as a compile-time constant; routed through a parameter, that constant-folding is lost. Each call site must invoke its own `MH_` field directly, even if that means near-duplicate call sites instead of one shared helper
- every `MH_` field must have a `/// \`<c prototype>\`` comment on the line immediately above it, copied verbatim from `rocksdb/include/rocksdb/c.h` (strip the `extern ROCKSDB_LIBRARY_API` prefix); no duplicate comment in the `static` block
- don't map multiple times the same symbol from C library of rocksdb
    - try to create always a java wrapper for that (i.e. PinnableSlice)
- use NativeObject as base class for all managed objects
    - this is needed to avoid double close() crashing the JVM
- don't expose public constructors, like CompactOptions.newCompactOptions(), CompactOptions.newCompactOptions()
    - why? to be able to call super in the private constructor and to have more freedom in the static factory method
- int-backed enums map native int → enum via a package-private `fromValue(int)` using a `switch` expression (see
  `LogLevel`), never a `for (X x : X.values())` loop — `values()` allocates a fresh array on every call. Unmapped
  input either falls back to a documented default constant or throws `IllegalArgumentException`, matching whatever
  the existing enum already did — `fromValue` changes the lookup mechanism, not the fallback behavior.
