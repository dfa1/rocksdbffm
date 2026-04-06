# AGENTS.md: Project Context & AI-Driven Guidelines

This file serves as the primary context for AI agents working on **rocksdbffm**. It contains the project's technical architecture, safety constraints, and performance-critical patterns.

## 🤖 AI-Driven Project Mandate
This project is heavily AI-driven. As an agent, your goal is to:
- **Be Autonomous:** Research C headers and identify the best mapping to Java FFM.
- **Stay Technical:** Prioritize performance, zero-copy, and manual memory safety.
- **Maintain Consistency:** Follow established naming and ownership patterns.

---

## 🛠 Tech Stack
- **Language:** Java 25+.
- **Core API:** `java.lang.foreign` (Foreign Function & Memory API).
- **Native Library:** RocksDB (C API via `include/rocksdb/c.h`).
- **Build System:** Maven.
- **Testing:** JUnit 5, AssertJ.
- **Benchmarking:** JMH (Java Microbenchmark Harness).

---

## 🏗 Architectural Standards

### 1. Manual Memory Management & Lifecycle
Every class wrapping a native pointer **must** implement `AutoCloseable`.
- **Zero Leaks:** Native resources must be destroyed in `close()`.
- **Ownership Transfer:** When one native object takes ownership of another (e.g., `FilterPolicy` → `BlockBasedTableConfig`), the transferring object must mark the ownership as transferred using a boolean flag. Its `close()` method should then become a no-op to prevent double-frees.
- **Transfer Marker:** Use a method like `transferOwnership()` inside the setter that takes ownership.

### 2. Data Types & Path Handling
To ensure type safety and consistent units across the API:
- **C API Only:** We use the RocksDB C interface (`rocksdb/c.h`). Do not attempt to link directly to C++ symbols.
- **Read-only headers:** NEVER modify system include files (e.g. `/opt/homebrew/...`, `/usr/include/...`). They are read-only references; all mappings live in Java source.
- **Paths:** Never use raw `String` for file system paths. Always use `java.nio.file.Path` for any API surface that accepts paths (open, backup, checkpoint).
- **Memory Sizes:** Never use raw `long` for byte counts (e.g., cache size, write buffer size). Always use the project's `MemorySize` type.
- **Sequence Numbers:** Never use raw `long` for RocksDB sequence numbers. Always use the project's `SequenceNumber` type.

### 3. API Surface Design
For every feature, provide three tiers of access:
1. **`MemorySegment` Version:** Native-first, for performance-critical usage.
2. **`ByteBuffer` Version:** For compatibility with existing NIO-based clients.
3. **`byte[]` Version:** Quick access for convenience (explicitly documented as slower).

---

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

---

## 🧪 Validation & Workflow

### 1. Comparative Testing
For every new feature:
1. Compare the behavior against `rocksdbjni` if possible.
2. Write unit tests in JUnit 5 using `@TempDir`.
3. Follow the `// Given / // When / // Then` structure.

### 2. Benchmark First
Performance gains are a primary goal. Use `JMH` to validate changes.
- **Run tests:** `mvn test`
- **Run benchmarks:**
  ```bash
  mvn test-compile -q
  CP="target/test-classes:target/classes:$(mvn dependency:build-classpath -DforceStdout 2>&1 | grep '^/')"
  java --enable-native-access=ALL-UNNAMED -cp "$CP" io.github.dfa1.rocksdbffm.benchmark.FfmBenchmark
  java --enable-native-access=ALL-UNNAMED -cp "$CP" io.github.dfa1.rocksdbffm.benchmark.JniBenchmark
  ```

---

## 🗺 Source Map

For the full feature status and roadmap see `README.md`. This section maps each implemented feature to its Java source files so agents can quickly find the right file to extend.

### Implemented features → source files

| Feature | Java source files |
| :--- | :--- |
| DB Open/Close | `RocksDB.java` |
| Put/Get/Delete | `RocksDB.java` |
| Options | `Options.java`, `ReadOptions.java`, `WriteOptions.java` |
| WriteBatch | `WriteBatch.java` |
| Transactions | `Transaction.java`, `TransactionDB.java`, `TransactionDBOptions.java`, `TransactionOptions.java` |
| Optimistic Transactions | `OptimisticTransactionDB.java`, `OptimisticTransactionOptions.java` |
| Checkpoints | `Checkpoint.java` |
| Table Options | `BlockBasedTableConfig.java`, `LRUCache.java`, `FilterPolicy.java` |
| Iterators | `RocksIterator.java` |
| Snapshots | `Snapshot.java`; `ReadOptions.setSnapshot`; `RocksDB.getSnapshot`, `TransactionDB.getSnapshot`, `Transaction.getSnapshot` |
| Flush | `FlushOptions.java`; `RocksDB.flush`, `RocksDB.flushWal`, `TransactionDB.flush`, `TransactionDB.flushWal` |
| KeyMayExist | `RocksDB.keyMayExist` — byte[], ByteBuffer, MemorySegment, ReadOptions overload |
| DB Properties | `DBProperty.java` (enum of well-known names); `RocksDB.getProperty`, `RocksDB.getLongProperty`, same on `TransactionDB` |
| Statistics | `HistogramType.java`, `TickerType.java`, `StatsLevel.java`, `StatisticsHistogramData.java` |
| Shared utilities | `Native.java` (`errHolder`, `checkError`, `toNative`), `MemorySize.java`, `RocksDBException.java` |
| Compaction control | `CompactOptions.java`; `RocksDB.compactRange`, `suggestCompactRange`, `disableFileDeletions`, `enableFileDeletions` |
| Secondary DB | `SecondaryDB.java` |

### Missing features — key C API entry points

| Feature | Key C API symbols |
| :--- | :--- |
| ~~Snapshots~~ | ✅ Done — see `Snapshot.java` |
| **Column Families** | `rocksdb_open_column_families`, `rocksdb_create_column_family`, `rocksdb_drop_column_family` |
| **Merge / MergeOperator** | `rocksdb_merge`, `rocksdb_writebatch_merge`, `rocksdb_mergeoperator_create` |
| ~~Flush~~ | ✅ Done — see `FlushOptions.java` |
| ~~KeyMayExist~~ | ✅ Done — `RocksDB.keyMayExist` (byte[], ByteBuffer, MemorySegment, ReadOptions overload) |
| ~~Compaction control~~ | ✅ Done — `CompactOptions.java`; `RocksDB.compactRange`, `suggestCompactRange`, `disableFileDeletions`, `enableFileDeletions` |
| **MultiGet** | `rocksdb_multi_get`, batched `rocksdb_slice_t` variant |
| ~~DB Properties~~ | ✅ Done — `DBProperty.java`, `RocksDB.getProperty`, `RocksDB.getLongProperty` (`rocksdb_approximate_sizes` not yet) |
| ~~KeyMayExist~~ | ✅ Done — see above |
| **DeleteRange** | `rocksdb_delete_range`, `rocksdb_writebatch_delete_range` |
| **SST File Ingest** | `rocksdb_ingestexternalfile`, `rocksdb_sstfilewriter_*` |
| **Backup Engine** | `rocksdb_backup_engine_open`, `rocksdb_backup_engine_create_new_backup` |
| **TTL DB** | `rocksdb_open_with_ttl` |
| ~~Optimistic Transactions~~ | ✅ Done — `OptimisticTransactionDB.java`, `OptimisticTransactionOptions.java` |
| **CompactionFilter** | `rocksdb_compactionfilter_create` |
| **WAL Iterator** | `rocksdb_get_updates_since`, `rocksdb_wal_iterator_*` |
| **Rate Limiter** | `rocksdb_ratelimiter_create`, `rocksdb_options_set_ratelimiter` |
| ~~Secondary DB~~ | ✅ Done — `SecondaryDB.java`; `tryCatchUpWithPrimary`, get (pinnable slice), iterator, snapshot, properties |

---

## 🏗 Technical Debt
- **Native Build Integration:** Currently relies on local `brew install rocksdb`. We need an in-tree build of RocksDB to support cross-architecture distribution.
