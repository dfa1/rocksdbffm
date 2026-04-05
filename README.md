# rocksdbffm

**rocksdbffm** is a Java wrapper for [RocksDB](https://rocksdb.org/) using the **Foreign Function & Memory (FFM) API** (Project Panama).

The project aims to provide a more maintainable alternative to the traditional JNI-based `rocksdbjni`.
The target is JDK 25+ because of `java.lang.foreign`.

## Why This Project Exists

### 1. Inspired by community work

Especially this post [Expanding RocksDB’s Java FFI](https://rocksdb.org/blog/2024/02/20/foreign-function-interface.html).

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
- **MemorySegment & ByteBuffer:** Support for `java.lang.foreign.MemorySegment` and direct `ByteBuffer` for data transfer between Java and native code.


## Performance Results
> [!WARNING]
> This needs much more work and verification.

Benchmarks performed on JDK 25 (Apple M-series) compared to JNI:

| API Tier | Operation | FFM (ops/s) | JNI (ops/s) | Gain |
| :--- | :--- | :---: | :---: | :---: |
| byte[] | Reads | 2.75M | 2.10M | +30% |
| Direct ByteBuffer | Reads | 3.02M | 2.04M | +48% |
| Pinnable + TL Arena | Reads | 3.37M | 1.93M | +74% |
| Pinnable + TL Arena | Writes | 0.67M | 0.58M | +17% |

*Note: Reads benefit from `PinnableSlice` which eliminates intermediate copies. Writes are bound by WAL/memtable performance.*

## Project Status

This project is currently experimental. Core features are implemented, but several advanced RocksDB features are still in progress.

### Implemented

| Feature | Notes |
| :--- | :--- |
| DB Open/Create | Options, CreateIfMissing, ReadOnly |
| Put/Get/Delete | byte[], ByteBuffer, MemorySegment; zero-copy via PinnableSlice |
| WriteBatch | Atomic multi-op writes |
| Transactions (pessimistic) | TransactionDB, savepoints, get-for-update |
| Checkpoints | Point-in-time on-disk snapshot |
| Table Options | BlockBasedTableConfig, LRUCache, FilterPolicy (Bloom) |
| Iterators | seekToFirst/Last, seek, seekForPrev, next/prev; all three access tiers |
| Snapshots | Point-in-time consistent reads; `ReadOptions.setSnapshot`, sequence numbers |
| Flush | `flush(FlushOptions)`, `flushWal(boolean sync)`; sync/async modes |
| DB Properties | `getProperty(DBProperty)` → `Optional<String>`, `getLongProperty(DBProperty)` → `OptionalLong`; well-known names in `DBProperty` enum |
| Statistics | TickerType, HistogramType, StatsLevel |

### Roadmap — rocksdbjni parity gaps

| Feature | Priority | Notes |
| :--- | :--- | :--- |
| **Column Families** | 🔴 High | Key namespace isolation |
| **Merge / MergeOperator** | 🔴 High | Aggregation semantics |
| **Compaction control** | 🟠 Medium | Manual compaction, space reclaim |
| **MultiGet** | 🟠 Medium | Bulk reads |
| **DeleteRange** | 🟠 Medium | Range tombstones |
| **SST File Ingest** | 🟠 Medium | High-speed bulk loading |
| **Backup Engine** | 🟡 Low | Incremental backups |
| **TTL DB** | 🟡 Low | Auto-expiring keys |
| **Optimistic Transactions** | 🟡 Low | Lock-free transactions |
| **CompactionFilter** | 🟡 Low | Custom compaction logic |
| **WAL Iterator** | 🟡 Low | Change log streaming |
| **Rate Limiter** | 🟡 Low | Write rate limiting |
| **Secondary DB** | 🟡 Low | Read-only replicas |

## Design Choices

Several deliberate decisions set this library apart from `rocksdbjni`.

### Modern Java
Requires JDK 25+. The API uses `java.lang.foreign` (FFM), records, sealed types, and pattern matching where they reduce boilerplate or improve safety. There is no legacy compatibility shim.

### Exceptions for all errors
Every operation that can fail throws `RocksDBException` (an unchecked exception). `rocksdbjni` historically returned `null`, `-1`, or relied on status objects that callers could silently ignore. Here a failure is always loud.

### Domain primitives instead of raw scalars
Raw numeric types carry no unit information and cannot be validated at construction time.

| Concept | rocksdbjni | rocksdbffm |
| :--- | :--- | :--- |
| Cache / buffer sizes | `long` (bytes, silently) | `MemorySize.ofMB(64)` |
| Snapshot position | `long` | `SequenceNumber` |

Both types are immutable, `Comparable`, and reject invalid values at construction — an illegal value cannot be created and therefore cannot be passed anywhere.

### `Path` for filesystem operations
All methods that accept a filesystem location (open, checkpoint, backup, …) take `java.nio.file.Path` instead of `String`. This prevents confusion between absolute and relative paths, integrates naturally with the NIO file API, and rules out accidentally passing non-path strings.

## Development Approach

This is a heavily AI-driven project. We intend to continue using AI as a cornerstone of our development process, from mapping C headers to optimizing the FFM implementation.

## Getting Started

### Requirements
- JDK 25+.
- RocksDB installed locally (e.g., `brew install rocksdb` on macOS).
> [!WARNING]
>  this works only in macOS when rocksdb is installed

### Build and Test
```bash
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
- Cover all features of RocksDB in idiomatic Java.
- Provide a pool for MemorySegment/ByteBuffer to make the library more
  "battery included".
- If community is aligned: run it as separated incubating project
  - Use zig to cross-compile rocksdb for all major platforms (to simplify the build for windows/macOS/linux).
  - Deploy to maven central.
