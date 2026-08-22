# RocksDB C API Gaps

Supplement to [explanation.md](explanation.md), which explains
[why the C API is the whole contract](explanation.md#the-c-api-is-the-whole-contract). For what is
implemented today see [reference.md#feature-status](reference.md#feature-status).

rocksdbffm wraps `rocksdb/c.h` — the official RocksDB C API — not C++ directly. This has two consequences:

- **Type A gaps**: the C API exposes the feature but rocksdbffm has no Java wrapper yet. Actionable now.
- **Type B gaps**: the C API does not expose the feature at all. Requires an upstream PR to `facebook/rocksdb` (`include/rocksdb/c.h` + `db/c.cc` + `db/c_test.c`) before a Java wrapper is possible. Every entry below only touches types that already have a C-API analogue (`Slice`, `Env`, `Logger`, flat parallel arrays as used by `rocksdb_multi_get()`), so a shim is mechanical, not architecturally blocked.

---

## Type A — C API exists, Java wrapper missing

| Feature | Key C functions | Priority | Notes |
|:---|:---|:---:|:---|
| MultiGet | `rocksdb_multi_get()` | High | Bulk key lookup; important for throughput |
| CompactFiles | `rocksdb_compact_files()`, `rocksdb_compaction_options_t` + setters (incl. `output_temperature_override`) | Low | A second, unrelated options opaque type from `CompactOptions`'s `rocksdb_compactoptions_t` (used by `compactRange`) — needs its own wrapper class, not an extension of `CompactOptions.java` |
| CompactionFilter | `rocksdb_compactionfilter_create()`, `rocksdb_compactionfilterfactory_create()` | High | Callback-based; enables custom retention/deletion policies during compaction |
| Custom Comparator | `rocksdb_comparator_create()`, `rocksdb_comparator_with_ts_create()` | High | Custom key ordering; note: key shortening not exposed in C API |
| JemallocNodumpAllocator | `rocksdb_jemalloc_nodump_allocator_create()` | Medium | Jemalloc allocator for caches; avoids coredump leaking sensitive data |
| CuckooTable options | `rocksdb_cuckoo_table_options_t` + setters | Medium | Hash-based SST format; efficient for fixed-size keys |
| Advanced memtable config | Various `rocksdb_options_set_*` memtable setters | Low | SkipList tuning, hash-memtable variants |
| Advanced column family options | CF-scoped option setters | Low | Per-CF compaction style, level multiplier, etc. |
| `rocksdb.live_sst_files_size_at_temperature` property | `rocksdb_property_value()` (existing, generic) | Low | Needs a `Temperature` enum (no Java type yet, matches C++'s `kUnknown`/`kHot`/`kWarm`/`kCold`) and a suffixed-property call shape (`name:kWarm`) `getProperty(Property)`'s flat enum doesn't support — every other property is a plain constant, this one alone takes a parameter |

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
| Persistent Cache | `rocksdb/persistent_cache.h` | `rocksdb_persistent_cache_t`, `rocksdb_persistent_cache_create(env, path, size, log, optimized_for_nvm)` | `NewPersistentCache()` only takes `Env*`/`Logger` (both already have `rocksdb_env_t`/`rocksdb_logger_t` shims) plus scalars — same shape as `rocksdb_rate_limiter_create()` |
| Wide Columns | `rocksdb/wide_columns.h` | `rocksdb_put_entity()`, `rocksdb_get_entity()`, `rocksdb_pinnablewidecolumns_t` | `WideColumn` is a name/value `Slice` pair; `WideColumns` is a vector of those — expressible as parallel `char**`/`size_t*` arrays exactly like `rocksdb_multi_get()` already does |
| Library version query | `rocksdb/version.h` | A `rocksdb_version()`-style function returning `ROCKSDB_MAJOR`/`ROCKSDB_MINOR`/`ROCKSDB_PATCH` | These are C++ preprocessor macros, not runtime-queryable through `c.h`. The official JNI binding gets this via a custom native method in its own `rocksjni.cc` that reads the macros directly — not available to a C-API-only wrapper. We currently only know the version via the pinned submodule tag (see `CLAUDE.md`); no way to ask the loaded native library at runtime |

---

## Contributing

For **Type A**: add a Java wrapper class following the `NativeObject` pattern, wire FFM upcall stubs for callback-based APIs.

For **Type B**: open a PR to `facebook/rocksdb` adding the C shim, then add the Java wrapper. The secondary cache PR is a good starting point and was discussed in the project issue tracker.
