# RocksDB C API Gaps

rocksdbffm wraps `rocksdb/c.h` — the official RocksDB C API — not C++ directly. This has two consequences:

- **Type A gaps**: the C API exposes the feature but rocksdbffm has no Java wrapper yet. Actionable now.
- **Type B gaps**: the C API does not expose the feature at all. Requires an upstream PR to `facebook/rocksdb` (`include/rocksdb/c.h` + `db/c.cc` + `db/c_test.c`) before a Java wrapper is possible.

A third category covers features that are C++-only with no viable C shim path.

---

## Type A — C API exists, Java wrapper missing

| Feature | Key C functions | Priority | Notes |
|:---|:---|:---:|:---|
| MultiGet | `rocksdb_multi_get()` | High | Bulk key lookup; important for throughput |
| CompactionFilter | `rocksdb_compactionfilter_create()`, `rocksdb_compactionfilterfactory_create()` | High | Callback-based; enables custom retention/deletion policies during compaction |
| EventListener | `rocksdb_eventlistener_create()` (~12 callbacks) | High | Flush, compaction, file creation/deletion events; needed for monitoring |
| Custom Comparator | `rocksdb_comparator_create()`, `rocksdb_comparator_with_ts_create()` | High | Custom key ordering; note: key shortening not exposed in C API |
| Custom MergeOperator | `rocksdb_mergeoperator_create()` | Medium | C API exists; Java side partially started per roadmap |
| JemallocNodumpAllocator | `rocksdb_jemalloc_nodump_allocator_create()` | Medium | Jemalloc allocator for caches; avoids coredump leaking sensitive data |
| CuckooTable options | `rocksdb_cuckoo_table_options_t` + setters | Medium | Hash-based SST format; efficient for fixed-size keys |
| Advanced memtable config | Various `rocksdb_options_set_*` memtable setters | Low | SkipList tuning, hash-memtable variants |
| Advanced column family options | CF-scoped option setters | Low | Per-CF compaction style, level multiplier, etc. |

---

## Type B — No C API yet (upstream contribution needed)

Each entry requires adding an opaque type, factory function(s), and option setters to `rocksdb/c.h` and implementing them in `db/c.cc`.

| Feature | C++ header | What c.h needs | Notes |
|:---|:---|:---|:---|
| SecondaryCache | `rocksdb/cache.h` | `rocksdb_secondary_cache_t`, `rocksdb_compressed_secondary_cache_create()`, `rocksdb_lru_cache_options_set_secondary_cache()` | Compressed in-memory L2 tier; discussed in [secondary-cache PR discussion](https://github.com/facebook/rocksdb/issues) |
| TieredCache | `rocksdb/cache.h` | `rocksdb_cache_create_tiered()` + `rocksdb_tiered_cache_options_t` | Combines primary LRU with compressed secondary; single cache_t result |
| SST File Reader | `rocksdb/sst_file_reader.h` | `rocksdb_sst_file_reader_t`, open, new_iterator, get_table_properties, verify_checksum, destroy | Offline SST inspection; not exposed in c.h at all |
| Statistics ticker read | `rocksdb/statistics.h` | `rocksdb_statistics_get_ticker_count(stats, ticker_id)` | Histogram access exists; ticker (counter) read is missing |
| PlainTable | `rocksdb/table.h` | `rocksdb_plain_table_options_t` + factory setter on options | Memory-mapped hash-index format; good for read-heavy in-memory use |
| WAL Filter | `rocksdb/wal_filter.h` | `rocksdb_wal_filter_t`, callback create, options setter | Selective WAL replay at recovery time |
| Trace reader/writer | `rocksdb/trace_reader_writer.h` | File-based factory functions, read/write/close wrappers | Operation tracing and replay for debugging |

---

## C++-only — blocked

These features have no C API and cannot be bridged without forking RocksDB to add custom shims:

| Feature | C++ entry point | Reason |
|:---|:---|:---|
| Persistent Cache | `NewPersistentCache()` | C++ only; not in `c.h` |
| Wide Columns | `DB::PutEntity()`, `DB::GetEntity()` | C++ only; not in `c.h` |

---

## Contributing

For **Type A**: add a Java wrapper class following the `NativePointer` pattern, wire FFM upcall stubs for callback-based APIs.

For **Type B**: open a PR to `facebook/rocksdb` adding the C shim, then add the Java wrapper. The secondary cache PR is a good starting point and was discussed in the project issue tracker.
