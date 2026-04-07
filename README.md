# RocksDB FFM

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![CI](https://github.com/dfa1/rocksdbffm/workflows/CI/badge.svg?branch:master)](https://github.com/dfa1/rocksdbffm/actions?query=branch:master)

**rocksdbffm** is an experimental Java wrapper for [RocksDB](https://rocksdb.org/) **v10.10.1** using the **Foreign
Function & Memory (FFM) API** (Project Panama).

The project aims to provide a more maintainable alternative to the traditional JNI-based `rocksdbjni`.
The target is JDK 25+ because of `java.lang.foreign`.

The native library is built from the RocksDB source via **`zig cc` / `zig c++`** as a drop-in C/C++ compiler (
`PORTABLE=1 make shared_lib`). Zig bundles clang and libc++ for every target, enabling hermetic cross-compilation
without a separate sysroot or system toolchain.

## Why This Project Exists

### 1. Inspired by community work

Especially this
post [Expanding RocksDB’s Java FFI](https://rocksdb.org/blog/2024/02/20/foreign-function-interface.html).

And this:
[Rocksjava-presentation](https://evolvedbinary.slides.com/adamretter/rocksjava-present-and-future#/1)

### 2. Reducing JNI Maintenance Lag

There is often a significant delay between new features appearing in the RocksDB C++ core and their availability in
the Java JNI wrappers. This is largely due to the complexity of maintaining C++ glue code. By using FFM, we can map
C headers directly in Java, simplifying the process of supporting new C++ features.

The code is mechanically generated, and it can be inspected easily (as it is normal Java code).

### 3. Safety

FFM is much more safe than JNI: memory errors are not crashing the whole JVM.

### 4. Performance through Zero-Copy

Exposing `MemorySegment` methods:

- **Pinnable Slices:** Utilizes `rocksdb_get_pinned`
- **MemorySegment & ByteBuffer:** Support for `java.lang.foreign.MemorySegment` and direct `ByteBuffer` for data
  transfer between Java and native code.

## Performance Results

Benchmarks performed on JDK 25 (Apple M-series), RocksDB v10.10.1. Each tier uses the same single pre-seeded key so the
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
| Column Families            |   ❌    | Key namespace isolation                                                                                                                          |
| Merge                      |   ✅    | `merge` on `RocksDB` and `WriteBatch`                                                                                                            |
| MergeOperator              |   ❌    | custom `MergeOperator` via FFM upcall stubs; built-in uint64 add                                                                                 |
| MultiGet                   |   ❌    | Bulk reads                                                                                                                                       |
| DeleteRange                |   ✅    | Range tombstones; `deleteRange` on `RocksDB` and `WriteBatch`; all three access tiers                                                            |
| Compaction control         |   ✅    | `compactRange` (all three tiers + `CompactOptions`), `suggestCompactRange`, `disableFileDeletions`, `enableFileDeletions`                        |
| SST File Ingest            |   ✅    | `SstFileWriter` (put/delete/deleteRange/merge), `RocksDB.ingestExternalFile`; `IngestExternalFileOptions`                                        |
| Backup Engine              |   ❌    | Incremental backups                                                                                                                              |
| TTL DB                     |   ✅    | `openWithTtl(path, Duration)`; lazy expiry via compaction; full API available                                                                    |
| Optimistic Transactions    |   ✅    | `OptimisticTransactionDB`; conflict detection at commit; `OptimisticTransactionOptions`                                                          |
| CompactionFilter           |   ❌    | Custom compaction logic                                                                                                                          |
| WAL Iterator               |   ❌    | Change log streaming                                                                                                                             |
| Rate Limiter               |   ❌    | Write rate limiting                                                                                                                              |
| Secondary DB               |   ✅    | `SecondaryDB`; `tryCatchUpWithPrimary`, get, iterator, snapshot, properties                                                                      |

## Design Choices

Several deliberate decisions set this library apart from `rocksdbjni`.

### Modern Java

Requires JDK 25+. The API uses `java.lang.foreign` (FFM), records, sealed types, and pattern matching where they reduce
boilerplate or improve safety. There is no legacy compatibility shim.

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

## Development Approach

This is a heavily AI-driven project. We intend to continue using AI as a cornerstone of our development process, from
mapping C headers to optimizing the FFM implementation.

## Getting Started

### Requirements

- JDK 25+.
- [Zig](https://ziglang.org/) (any 0.15.x build).

### Build and Test

```bash
# Build RocksDB from the submodule (first time or after a clean)
mvn generate-resources -Pnative-build

# Run unit tests
mvn test
```

## License

This project is licensed under the same terms as RocksDB (LevelDB/Apache 2.0).

## Contributing

The project is open to contributions, particularly in the following areas:

- Implementing missing RocksDB C API features in Java.
- Benchmarking and performance profiling of the Java-to-Native boundary.
- Improving the safety and lifecycle management of native objects.

## TODO

- Create a community around this project with the intent to merge it back into rocksdb.
- If that fails and community is aligned:
    - Run it as separated project (like rust-rocksdb).
    - Deploy to maven central.
- Cover all features of RocksDB in idiomatic Java.
- Provide a pool for MemorySegment/ByteBuffer to make the library more
  "battery included".
- Add arena-accepting overloads to the `byte[]` API tier (Zig-style caller-owned allocator):
  `db.put(arena, key, value)` / `db.get(arena, key)` / `db.delete(arena, key)`.
  Lets callers amortize arena create/destroy over a batch of calls instead of paying it per call.
- RocksDB, OptimisticTransactionDB, TransactionDB reimplement the same get/put/delete methods
- find a way to refactor that
- some functions are mapping multiple times: document this for the AI
- PinnableSlice
- rocksdb_free
- most likely others
