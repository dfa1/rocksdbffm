# RocksDB FFM

![RocksDB](https://img.shields.io/badge/RocksDB-11.0.4-green.svg)
![MacOS](https://img.shields.io/badge/macOS-fully_supported-green.svg)
![Linux](https://img.shields.io/badge/linux-partial_support-orange.svg)
![Window](https://img.shields.io/badge/window-not_supported-red.svg)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![CI](https://github.com/dfa1/rocksdbffm/workflows/CI/badge.svg?branch:master)](https://github.com/dfa1/rocksdbffm/actions?query=branch:master)

**rocksdbffm** is an experimental Java wrapper for [RocksDB](https://rocksdb.org/) using the **Foreign
Function & Memory (FFM) API**.

The project aims to provide a more maintainable alternative to the traditional JNI-based `rocksdbjni`.
The target is JDK 25+ because of `java.lang.foreign`.

> **AI-assisted development:** This project uses [Claude Code](https://claude.ai/code) heavily for implementation
> work — C header mapping, test generation, and documentation. **Architecture, API design, and all decisions are
> human-driven.** We think this is an honest and productive way to build a library, and we're open about it.

The native library is built from the RocksDB source via **`zig cc` / `zig c++`** as a drop-in C/C++ compiler
(`PORTABLE=1 make shared_lib`). Zig bundles clang and libc++ for every target, enabling hermetic cross-compilation
without a separate sysroot or system toolchain.

## Getting Started

### Requirements

- JDK 25+.
- [Zig](https://ziglang.org/) (any 0.15.x build).

### Build and Test

```bash
# Build RocksDB from the submodule (first time or after a clean)
./mvnw generate-resources -Pnative-build

# Run unit tests
./mvnw test
```

## Why This Project Exists

### 1. Reducing JNI Maintenance Lag

There is often a significant delay between new features appearing in the RocksDB C++ core and their availability in
the Java JNI wrappers. This is largely due to the complexity of maintaining C++ glue code. By using FFM, we can map
C headers directly in Java, simplifying the process of supporting new C++ features.

### 2. Safety

FFM improves safety over JNI on the Java side: accessing a closed or out-of-bounds `MemorySegment`
throws an exception rather than silently corrupting memory. However, a bad pointer passed into a
native call can still crash the JVM — FFM does not sandbox native execution.

## Design Choices

Several deliberate decisions set this library apart from `rocksdbjni`.

### Modern Java

The API uses `java.lang.foreign` (FFM), records, sealed types, and pattern matching where they reduce
boilerplate or improve safety. There is no legacy compatibility shim.

### Expose only valid operations

Every type of RocksDB instance exposes only relevant operations in Java.
For example, `rocksdb_open_for_read_only` is exposed as `ReadOnlyDB`, which
does not expose any `put` or `delete` method.

In `rocksdbjni`, the same `RocksDB` type is used for both read-write and read-only opens.
Calling `put()` on a read-only instance compiles and runs, but fails at runtime:

1. `RocksDB.put(byte[], byte[])` calls through JNI into `db->Put(...)`.
2. The underlying C++ object is a `DBImplReadOnly`, which overrides every write method to return `Status::NotSupported("Not supported operation in read only mode.")`.
3. The JNI layer converts that status into a thrown `RocksDBException`.

Here the constraint is enforced at compile time — `ReadOnlyDB` simply has no `put`, `delete`, `merge`, or `write` method, so an invalid call is a build error rather than a runtime failure.

### Exceptions for all errors

Every operation that can fail throws `RocksDBException` (an unchecked exception). `rocksdbjni` historically returned
`null`, `-1`, or relied on status objects that callers could silently ignore. Here a failure is always loud.

### Domain primitives instead of raw scalars

Raw numeric types carry no unit information and cannot be validated at construction time.

| Concept              | rocksdbjni               | rocksdbffm            |
|:---------------------|:-------------------------|:----------------------|
| Cache / buffer sizes | `long` (bytes, silently) | `MemorySize.ofMB(64)` |
| Snapshot position    | `long`                   | `SequenceNumber`      |

Both types are immutable, `Comparable`, and reject invalid values at construction — an illegal value cannot be created
and therefore cannot be passed anywhere.

### `Path` for filesystem operations

All methods that accept a filesystem location (open, checkpoint, backup, …) take `java.nio.file.Path` instead of
`String`. This prevents confusion between absolute and relative paths, integrates naturally with the NIO file API, and
rules out accidentally passing non-path strings.

### Performance through Zero-Copy

- **Pinnable Slices:** Uses `rocksdb_get_pinned` to avoid intermediate copies from the block cache.
- **MemorySegment & ByteBuffer:** Support for `java.lang.foreign.MemorySegment` and direct `ByteBuffer` for data
  transfer between Java and native code.

## Performance Results

Benchmarks performed on JDK 25 (Apple M5), RocksDB v11.0.4. Each tier uses the same pre-seeded key so the
numbers reflect pure call overhead, not cache miss variance.

| Operation              | API tier           | FFM (ops/s) | JNI (ops/s) |   Gain    |
|:-----------------------|:-------------------|:-----------:|:-----------:|:---------:|
| Reads                  | `byte[]`           |  7,196,554  |  3,619,125  | **+99%**  |
| Reads                  | `DirectByteBuffer` |  8,077,135  |  3,656,113  | **+121%** |
| Reads                  | `MemorySegment`    |  8,149,510  |      —      |     —     |
| Writes                 | `byte[]`           |   671,213   |   608,496   | **+10%**  |
| Writes                 | `DirectByteBuffer` |   694,166   |   590,923   | **+17%**  |
| Writes                 | `MemorySegment`    |   686,889   |      —      |     —     |
| Batch writes (100 ops) | `byte[]`           |   23,936    |   16,813    | **+42%**  |

*Both libraries use `PinnableSlice` for reads. Read gains (~2×) come from the absence of JNI frame setup and
thread-state transitions — FFM downcall stubs are JIT-compiled directly. `MemorySegment` is the fastest read tier
because segments backed by a confined arena carry no GC scope-check overhead on the hot path. Write gains are smaller
because WAL/memtable I/O dominates. Batch write gains multiply because per-call overhead is paid 100× per iteration.*

### Running benchmarks

```bash
./scripts/benchmark.sh
```

Builds everything, runs both FFM and JNI suites, and prints a side-by-side comparison table.

## Roadmap

This project is currently experimental. The table below tracks parity with `rocksdbjni`.

| Feature                    | Status | Notes                                                                                                                                            |
|:---------------------------|:------:|:-------------------------------------------------------------------------------------------------------------------------------------------------|
| DB Open/Create             |   ✅    | Options, CreateIfMissing, ReadOnly                                                                                                               |
| Put/Get/Delete             |   ✅    | byte[], ByteBuffer, MemorySegment; zero-copy via PinnableSlice                                                                                   |
| WriteBatch                 |   ✅    | Atomic multi-op writes                                                                                                                           |
| Transactions (pessimistic) |   ✅    | TransactionDB, savepoints, get-for-update                                                                                                        |
| Checkpoints                |   ✅    | Point-in-time on-disk snapshot                                                                                                                   |
| Table Options              |   ✅    | BlockBasedTableConfig, LRUCache, FilterPolicy (Bloom)                                                                                            |
| Iterators                  |   ✅    | seekToFirst/Last, seek, seekForPrev, next/prev; all three access tiers                                                                           |
| Snapshots                  |   ✅    | Point-in-time consistent reads; `ReadOptions.setSnapshot`, sequence numbers                                                                      |
| Flush                      |   ✅    | `flush(FlushOptions)`, `flushWal(boolean sync)`; sync/async modes                                                                                |
| DB Properties              |   ✅    | `getProperty(DBProperty)` → `Optional<String>`, `getLongProperty(DBProperty)` → `OptionalLong`                                                   |
| Statistics                 |   ✅    | TickerType, HistogramType, StatsLevel                                                                                                            |
| Compression                |   ✅    | `CompressionType` enum (NO/Snappy/zlib/bz2/LZ4/LZ4HC/Xpress/Zstd); `Options.setCompression`; `CompressionType.getSupportedTypes()` runtime probe |
| Column Families            |   ✅    | `openWithColumnFamilies`, `listColumnFamilies`, `createColumnFamily`, `dropColumnFamily`; `ColumnFamilyHandle`, `ColumnFamilyDescriptor`; put/get/delete/deleteRange/keyMayExist/flush/getProperty/newIterator all three tiers; `WriteBatch` CF overloads; CF overloads on `ReadOnlyDB`, `TtlDB`, `TransactionDB`, `OptimisticTransactionDB`; `Transaction` put/delete/get/getForUpdate/newIterator per-CF; multi-CF open for all DB types |
| MultiGet                   |   ❌    | Bulk reads                                                                                                                                       |
| DeleteRange                |   ✅    | Range tombstones; `deleteRange` on `RocksDB` and `WriteBatch`; all three access tiers                                                            |
| Compaction control         |   ✅    | `compactRange` (all three tiers + `CompactOptions`), `suggestCompactRange`, `disableFileDeletions`, `enableFileDeletions`                        |
| SST File Ingest            |   ✅    | `SstFileWriter` (put/delete/deleteRange/merge), `RocksDB.ingestExternalFile`; `IngestExternalFileOptions`                                        |
| Backup Engine              |   ✅    | `BackupEngine`, `BackupEngineOptions`, `RestoreOptions`, `BackupInfo`, `BackupId`; incremental backup/restore; purge; verify                     |
| TTL DB                     |   ✅    | `openWithTtl(path, Duration)`; lazy expiry via compaction; full API available                                                                    |
| Optimistic Transactions    |   ✅    | `OptimisticTransactionDB`; conflict detection at commit; `OptimisticTransactionOptions`                                                          |
| Merge / MergeOperator      |   🚧    | `merge` on `RocksDB`, `WriteBatch`, `SstFileWriter`; `setUInt64AddMergeOperator` on `Options`; custom `MergeOperator` via FFM upcall stubs       |
| CompactionFilter           |   ❌    | Custom compaction logic                                                                                                                          |
| WAL Iterator               |   ✅    | `WalIterator`, `WalBatchResult`; `getUpdatesSince(SequenceNumber)`, `getLatestSequenceNumber`; CDC/replication/auditing                          |
| Rate Limiter               |   ✅    | `RateLimiter`; writes-only, reads-only, all-IO modes; auto-tuned variant; `Options.setRateLimiter`                                               |
| SST File Manager           |   ✅    | `SstFileManager`; disk-space limits, trash-deletion rate, compaction buffer; `Env`; `Options.setSstFileManager`, `Options.setEnv`                |
| Secondary DB               |   ✅    | `SecondaryDB`; `tryCatchUpWithPrimary`, get, iterator, snapshot, properties                                                                      |
| Blob DB                    |   ✅    | `BlobDB`; blob options on `Options`; blob properties (`BLOB_STATS`, `NUM_BLOB_FILES`, …); `PrepopulateBlobCache`                                 |
| Logger                     |   ✅    | Logger + callback                                                                                                                                |
| Custom Comparators         |   ❌    | Custom comparators                                                                                                                               |
| Advanced column family     |   ❌    |                                                                                                                                                  |
| Advanced memtable config   |   ❌    |                                                                                                                                                  |
| Perf Context               |   ✅    | `PerfContext`, `PerfLevel`, `PerfMetric`; `setPerfLevel`, `reset`, `metric`, `report`                                                            |
| Persistent Cache           |   🚫    | Not exposed in `rocksdb/c.h` — C++ only (`NewPersistentCache`); requires a custom C shim to bridge                                               |
| Background Jobs            |   🚧    | Tier 1: `cancelAllBackgroundWork`, `disableManualCompaction`, `enableManualCompaction`, `waitForCompact(WaitForCompactOptions)`; Tier 3–5 (Options tuning, Env thread pools, FIFO/Universal options) pending |

## License

This project is licensed under the same terms as RocksDB (LevelDB/Apache 2.0).

## See Also

- [Expanding RocksDB's Java FFI](https://rocksdb.org/blog/2024/02/20/foreign-function-interface.html)
- [Rocksjava: present and future](https://evolvedbinary.slides.com/adamretter/rocksjava-present-and-future#/1)
