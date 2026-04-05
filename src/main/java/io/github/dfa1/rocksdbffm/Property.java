package io.github.dfa1.rocksdbffm;

/**
 * Well-known RocksDB property names for use with
 * {@link RocksDB#getProperty(Property)} and {@link RocksDB#getLongProperty(Property)}.
 *
 * <p>String properties (text blobs) are only available via {@code getProperty}.
 * Numeric properties (uint64) are available via both {@code getProperty} and
 * {@code getLongProperty}. If the property does not match the requested type
 * the method returns empty.
 *
 * <p>For level-indexed properties ({@code num-files-at-level<N>},
 * {@code compression-ratio-at-level<N>}, {@code aggregated-table-properties-at-level<N>})
 * use the {@code _L0} … {@code _L6} suffixed constants.
 */
public enum Property {

    // -----------------------------------------------------------------------
    // String-only properties (stats / info blobs)
    // -----------------------------------------------------------------------

    /** Multi-line human-readable statistics summary. */
    STATS("rocksdb.stats"),
    /** Detailed listing of all SST files in the DB. */
    SSTABLES("rocksdb.sstables"),
    /** Per-column-family statistics (includes file histogram). */
    CF_STATS("rocksdb.cfstats"),
    /** Per-column-family statistics without the file histogram. */
    CF_STATS_NO_FILE_HISTOGRAM("rocksdb.cfstats-no-file-histogram"),
    /** Per-column-family file-read latency histogram. */
    CF_FILE_HISTOGRAM("rocksdb.cf-file-histogram"),
    /** Per-column-family write-stall statistics. */
    CF_WRITE_STALL_STATS("rocksdb.cf-write-stall-stats"),
    /** Database-level write-stall statistics. */
    DB_WRITE_STALL_STATS("rocksdb.db-write-stall-stats"),
    /** Database-level statistics (uptime, flush, compaction rates). */
    DB_STATS("rocksdb.dbstats"),
    /** Per-level file count and size summary. */
    LEVEL_STATS("rocksdb.levelstats"),
    /** Detailed block-cache entry statistics (string or map). */
    BLOCK_CACHE_ENTRY_STATS("rocksdb.block-cache-entry-stats"),
    /** Same as {@link #BLOCK_CACHE_ENTRY_STATS} but skips full recalculation. */
    FAST_BLOCK_CACHE_ENTRY_STATS("rocksdb.fast-block-cache-entry-stats"),
    /** Aggregated table properties across all SST files. */
    AGGREGATED_TABLE_PROPERTIES("rocksdb.aggregated-table-properties"),
    /** Statistics from the options object (if statistics are enabled). */
    OPTIONS_STATISTICS("rocksdb.options-statistics"),
    /** Total number and size of all blob files in the DB. */
    BLOB_STATS("rocksdb.blob-stats"),

    // -----------------------------------------------------------------------
    // Numeric properties (also available as strings)
    // -----------------------------------------------------------------------

    /** Number of immutable memtables that have not yet been flushed. */
    NUM_IMMUTABLE_MEM_TABLE("rocksdb.num-immutable-mem-table"),
    /** Number of immutable memtables that have already been flushed. */
    NUM_IMMUTABLE_MEM_TABLE_FLUSHED("rocksdb.num-immutable-mem-table-flushed"),
    /** 1 if a memtable flush is pending, 0 otherwise. */
    MEM_TABLE_FLUSH_PENDING("rocksdb.mem-table-flush-pending"),
    /** Number of flushes currently running in background threads. */
    NUM_RUNNING_FLUSHES("rocksdb.num-running-flushes"),
    /** 1 if at least one compaction is pending, 0 otherwise. */
    COMPACTION_PENDING("rocksdb.compaction-pending"),
    /** Number of compactions currently running in background threads. */
    NUM_RUNNING_COMPACTIONS("rocksdb.num-running-compactions"),
    /** Number of sorted runs in compactions currently running. */
    NUM_RUNNING_COMPACTION_SORTED_RUNS("rocksdb.num-running-compaction-sorted-runs"),
    /** Accumulated number of background errors since the DB was opened. */
    BACKGROUND_ERRORS("rocksdb.background-errors"),
    /** Approximate size (bytes) of the active memtable. */
    CUR_SIZE_ACTIVE_MEM_TABLE("rocksdb.cur-size-active-mem-table"),
    /** Approximate size (bytes) of active + unflushed immutable memtables. */
    CUR_SIZE_ALL_MEM_TABLES("rocksdb.cur-size-all-mem-tables"),
    /** Approximate size (bytes) of active + unflushed + pinned immutable memtables. */
    SIZE_ALL_MEM_TABLES("rocksdb.size-all-mem-tables"),
    /** Total number of entries in the active memtable. */
    NUM_ENTRIES_ACTIVE_MEM_TABLE("rocksdb.num-entries-active-mem-table"),
    /** Total number of entries in the unflushed immutable memtables. */
    NUM_ENTRIES_IMM_MEM_TABLES("rocksdb.num-entries-imm-mem-tables"),
    /** Total number of delete entries in the active memtable. */
    NUM_DELETES_ACTIVE_MEM_TABLE("rocksdb.num-deletes-active-mem-table"),
    /** Total number of delete entries in the unflushed immutable memtables. */
    NUM_DELETES_IMM_MEM_TABLES("rocksdb.num-deletes-imm-mem-tables"),
    /** Estimated number of keys in the DB (not exact). */
    ESTIMATE_NUM_KEYS("rocksdb.estimate-num-keys"),
    /** Estimated memory (bytes) used by index and filter blocks held in memory. */
    ESTIMATE_TABLE_READERS_MEM("rocksdb.estimate-table-readers-mem"),
    /** 0 if file deletions are disabled, 1 otherwise. */
    IS_FILE_DELETIONS_ENABLED("rocksdb.is-file-deletions-enabled"),
    /** Number of unreleased snapshots. */
    NUM_SNAPSHOTS("rocksdb.num-snapshots"),
    /** Unix timestamp of the oldest open snapshot. */
    OLDEST_SNAPSHOT_TIME("rocksdb.oldest-snapshot-time"),
    /** Sequence number of the oldest open snapshot. */
    OLDEST_SNAPSHOT_SEQUENCE("rocksdb.oldest-snapshot-sequence"),
    /** Number of live SST file versions (high = many open iterators/snapshots). */
    NUM_LIVE_VERSIONS("rocksdb.num-live-versions"),
    /** Sequence number of the current LSM version. */
    CURRENT_SUPER_VERSION_NUMBER("rocksdb.current-super-version-number"),
    /** Estimated total size (bytes) of live data. */
    ESTIMATE_LIVE_DATA_SIZE("rocksdb.estimate-live-data-size"),
    /** Minimum WAL log number that must be kept (not yet flushed to SST). */
    MIN_LOG_NUMBER_TO_KEEP("rocksdb.min-log-number-to-keep"),
    /** Minimum SST file number that must be kept (referenced by a checkpoint). */
    MIN_OBSOLETE_SST_NUMBER_TO_KEEP("rocksdb.min-obsolete-sst-number-to-keep"),
    /** Total file size (bytes) of all SST files, including obsolete ones. */
    TOTAL_SST_FILES_SIZE("rocksdb.total-sst-files-size"),
    /** Total file size (bytes) of all live SST files. */
    LIVE_SST_FILES_SIZE("rocksdb.live-sst-files-size"),
    /** Total file size (bytes) of obsolete SST files not yet deleted. */
    OBSOLETE_SST_FILES_SIZE("rocksdb.obsolete-sst-files-size"),
    /** Compaction base level (L0 data is compacted into this level). */
    BASE_LEVEL("rocksdb.base-level"),
    /** Estimated bytes of pending compaction at and above the base level. */
    ESTIMATE_PENDING_COMPACTION_BYTES("rocksdb.estimate-pending-compaction-bytes"),
    /** Current write throttle rate (bytes/s); 0 if not throttled. */
    ACTUAL_DELAYED_WRITE_RATE("rocksdb.actual-delayed-write-rate"),
    /** 1 if writes are completely stopped (stop condition), 0 otherwise. */
    IS_WRITE_STOPPED("rocksdb.is-write-stopped"),
    /** Estimated oldest key timestamp (for compaction of time-series data). */
    ESTIMATE_OLDEST_KEY_TIME("rocksdb.estimate-oldest-key-time"),
    /** Block cache capacity (bytes). */
    BLOCK_CACHE_CAPACITY("rocksdb.block-cache-capacity"),
    /** Block cache current usage (bytes). */
    BLOCK_CACHE_USAGE("rocksdb.block-cache-usage"),
    /** Block cache bytes pinned by iterators or block handles. */
    BLOCK_CACHE_PINNED_USAGE("rocksdb.block-cache-pinned-usage"),
    /** Number of blob files in the current version. */
    NUM_BLOB_FILES("rocksdb.num-blob-files"),
    /** Total size (bytes) of all blob files. */
    TOTAL_BLOB_FILE_SIZE("rocksdb.total-blob-file-size"),
    /** Total size (bytes) of all live blob files. */
    LIVE_BLOB_FILE_SIZE("rocksdb.live-blob-file-size"),
    /** Total garbage size (bytes) in blob files. */
    LIVE_BLOB_FILE_GARBAGE_SIZE("rocksdb.live-blob-file-garbage-size"),
    /** Blob cache capacity (bytes). */
    BLOB_CACHE_CAPACITY("rocksdb.blob-cache-capacity"),
    /** Blob cache current usage (bytes). */
    BLOB_CACHE_USAGE("rocksdb.blob-cache-usage"),
    /** Blob cache bytes pinned. */
    BLOB_CACHE_PINNED_USAGE("rocksdb.blob-cache-pinned-usage"),

    // -----------------------------------------------------------------------
    // Level-indexed: num-files-at-level<N>  (L0 – L6)
    // -----------------------------------------------------------------------

    NUM_FILES_AT_LEVEL_L0("rocksdb.num-files-at-level0"),
    NUM_FILES_AT_LEVEL_L1("rocksdb.num-files-at-level1"),
    NUM_FILES_AT_LEVEL_L2("rocksdb.num-files-at-level2"),
    NUM_FILES_AT_LEVEL_L3("rocksdb.num-files-at-level3"),
    NUM_FILES_AT_LEVEL_L4("rocksdb.num-files-at-level4"),
    NUM_FILES_AT_LEVEL_L5("rocksdb.num-files-at-level5"),
    NUM_FILES_AT_LEVEL_L6("rocksdb.num-files-at-level6"),

    // -----------------------------------------------------------------------
    // Level-indexed: compression-ratio-at-level<N>  (L0 – L6)
    // -----------------------------------------------------------------------

    COMPRESSION_RATIO_AT_LEVEL_L0("rocksdb.compression-ratio-at-level0"),
    COMPRESSION_RATIO_AT_LEVEL_L1("rocksdb.compression-ratio-at-level1"),
    COMPRESSION_RATIO_AT_LEVEL_L2("rocksdb.compression-ratio-at-level2"),
    COMPRESSION_RATIO_AT_LEVEL_L3("rocksdb.compression-ratio-at-level3"),
    COMPRESSION_RATIO_AT_LEVEL_L4("rocksdb.compression-ratio-at-level4"),
    COMPRESSION_RATIO_AT_LEVEL_L5("rocksdb.compression-ratio-at-level5"),
    COMPRESSION_RATIO_AT_LEVEL_L6("rocksdb.compression-ratio-at-level6"),

    // -----------------------------------------------------------------------
    // Level-indexed: aggregated-table-properties-at-level<N>  (L0 – L6)
    // -----------------------------------------------------------------------

    AGGREGATED_TABLE_PROPERTIES_AT_LEVEL_L0("rocksdb.aggregated-table-properties-at-level0"),
    AGGREGATED_TABLE_PROPERTIES_AT_LEVEL_L1("rocksdb.aggregated-table-properties-at-level1"),
    AGGREGATED_TABLE_PROPERTIES_AT_LEVEL_L2("rocksdb.aggregated-table-properties-at-level2"),
    AGGREGATED_TABLE_PROPERTIES_AT_LEVEL_L3("rocksdb.aggregated-table-properties-at-level3"),
    AGGREGATED_TABLE_PROPERTIES_AT_LEVEL_L4("rocksdb.aggregated-table-properties-at-level4"),
    AGGREGATED_TABLE_PROPERTIES_AT_LEVEL_L5("rocksdb.aggregated-table-properties-at-level5"),
    AGGREGATED_TABLE_PROPERTIES_AT_LEVEL_L6("rocksdb.aggregated-table-properties-at-level6");

    private final String propertyName;

    Property(String propertyName) {
        this.propertyName = propertyName;
    }

    /** Returns the RocksDB property string passed to the C API. */
    public String propertyName() {
        return propertyName;
    }
}
