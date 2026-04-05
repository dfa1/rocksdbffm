# rocksdbffm

**rocksdbffm** is a Java wrapper for [RocksDB](https://rocksdb.org/) using the **Foreign Function & Memory (FFM) API** (Project Panama).

The project provides a maintainable alternative to the traditional JNI-based `rocksdbjni`, targeting JDK 21 and above.

## Why This Project Exists

### 1. Reducing JNI Maintenance Lag
There is often a significant delay between new features appearing in the RocksDB C++ core and their availability in the Java JNI wrappers. This is largely due to the complexity of maintaining C++ glue code. By using FFM, we can map C headers directly in Java, simplifying the process of supporting new C++ features.

### 2. Performance through Zero-Copy
JNI typically requires copying data between native memory and the JVM heap. This wrapper is designed to minimize these copies:
- **Pinnable Slices:** Utilizes `rocksdb_get_pinned` to access data directly from the RocksDB block cache.
- **MemorySegment & ByteBuffer:** Support for `java.lang.foreign.MemorySegment` and direct `ByteBuffer` for data transfer between Java and native code.
- **Thread-Local Optimizations:** Reuses thread-local auxiliary segments for error handling and value lengths to reduce allocations on hot paths.

## Performance Results

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
| Statistics | TickerType, HistogramType, StatsLevel |

### Roadmap — rocksdbjni parity gaps

| Feature | Priority | Notes |
| :--- | :--- | :--- |
| ~~Snapshots~~ | ✅ Done | |
| **Column Families** | 🔴 High | Key namespace isolation |
| **Merge / MergeOperator** | 🔴 High | Aggregation semantics |
| ~~Flush~~ | ✅ Done | |
| **Compaction control** | 🟠 Medium | Manual compaction, space reclaim |
| **MultiGet** | 🟠 Medium | Bulk reads |
| **DB Properties** | 🟠 Medium | Introspection and monitoring |
| ~~KeyMayExist~~ | ✅ Done | |
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
Requires JDK 21+. The API uses `java.lang.foreign` (FFM), records, sealed types, and pattern matching where they reduce boilerplate or improve safety. There is no legacy compatibility shim.

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

## Contributing

The project is open to contributions, particularly in the following areas:
- Implementing missing RocksDB C API features in Java.
- Benchmarking and performance profiling of the Java-to-Native boundary.
- Improving the safety and lifecycle management of native objects.

## Getting Started

### Requirements
- JDK 25 (recommended) or JDK 21+.
- RocksDB installed locally (e.g., `brew install rocksdb` on macOS).

### Build and Test
```bash
# Run unit tests
mvn test

# Run performance benchmarks
mvn package -DskipTests
java --enable-native-access=ALL-UNNAMED -jar target/benchmarks.jar
```

## License
This project is licensed under the same terms as RocksDB (GPLv2/Apache 2.0).

---
*Reference: [Expanding RocksDB’s Java FFI](https://rocksdb.org/blog/2024/02/20/foreign-function-interface.html)*
