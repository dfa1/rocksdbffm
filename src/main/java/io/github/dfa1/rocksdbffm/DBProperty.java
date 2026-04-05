package io.github.dfa1.rocksdbffm;

/**
 * Well-known RocksDB property names for use with
 * {@link RocksDB#getProperty(DBProperty)} and
 * {@link RocksDB#getLongProperty(DBProperty)}.
 *
 * <p>String properties (text blobs) are available via {@code getProperty}.
 * Numeric properties (uint64) are available via both {@code getProperty} and
 * {@code getLongProperty}. Not all properties support both; if the property
 * is not recognized or does not match the requested type, the method returns
 * empty.
 */
public enum DBProperty {

    // -----------------------------------------------------------------------
    // String-only properties (stats blobs)
    // -----------------------------------------------------------------------

    /** Multi-line human-readable statistics summary. */
    STATS("rocksdb.stats"),
    /** Detailed listing of all SST files in the DB. */
    SSTABLES("rocksdb.sstables"),
    /** Per-column-family statistics. */
    CF_STATS("rocksdb.cfstats"),
    /** Database-level statistics. */
    DB_STATS("rocksdb.dbstats"),
    /** Per-level file count and size summary. */
    LEVEL_STATS("rocksdb.levelstats"),

    // -----------------------------------------------------------------------
    // Numeric properties (also available as strings)
    // -----------------------------------------------------------------------

    /** Number of immutable memtables that have not yet been flushed. */
    NUM_IMMUTABLE_MEM_TABLE("rocksdb.num-immutable-mem-table"),
    /** Estimated number of total keys in the active and unflushed immutable memtables. */
    NUM_ENTRIES_ACTIVE_MEM_TABLE("rocksdb.num-entries-active-mem-table"),
    /** Estimated number of total keys in the unflushed immutable memtables. */
    NUM_ENTRIES_IMM_MEM_TABLES("rocksdb.num-entries-imm-mem-tables"),
    /** Estimated number of keys in the DB (not exact). */
    ESTIMATE_NUM_KEYS("rocksdb.estimate-num-keys"),
    /** Estimated total size (bytes) of live data. */
    ESTIMATE_LIVE_DATA_SIZE("rocksdb.estimate-live-data-size"),
    /** Total file size (bytes) of all live SST files. */
    LIVE_SST_FILES_SIZE("rocksdb.live-sst-files-size"),
    /** Total file size (bytes) of all SST files, including obsolete ones not yet deleted. */
    TOTAL_SST_FILES_SIZE("rocksdb.total-sst-files-size"),
    /** Total size (bytes) of all mem-tables (active + immutable). */
    SIZE_ALL_MEM_TABLES("rocksdb.size-all-mem-tables"),
    /** Size (bytes) of active + unflushed immutable mem-tables. */
    CUR_SIZE_ALL_MEM_TABLES("rocksdb.cur-size-all-mem-tables"),
    /** Number of open snapshots. */
    NUM_SNAPSHOTS("rocksdb.num-snapshots"),
    /** Number of live SST file versions retained due to open iterators or snapshots. */
    NUM_LIVE_VERSIONS("rocksdb.num-live-versions"),
    /** Compactions currently running in background threads. */
    NUM_RUNNING_COMPACTIONS("rocksdb.num-running-compactions"),
    /** Flushes currently running in background threads. */
    NUM_RUNNING_FLUSHES("rocksdb.num-running-flushes"),
    /** Current write rate (bytes/s) applied when write throttling is active. */
    ACTUAL_DELAYED_WRITE_RATE("rocksdb.actual-delayed-write-rate"),
    /** 1 if writes are completely stopped (stall-level == stop), 0 otherwise. */
    IS_WRITE_STOPPED("rocksdb.is-write-stopped"),
    /** 1 if writes are being throttled (stall-level >= delay), 0 otherwise. */
    IS_WRITE_STALLED("rocksdb.is-write-stalled"),
    /** Block cache capacity in bytes. */
    BLOCK_CACHE_CAPACITY("rocksdb.block-cache-capacity"),
    /** Block cache current usage in bytes. */
    BLOCK_CACHE_USAGE("rocksdb.block-cache-usage"),
    /** Block cache bytes pinned by iterators or block handles. */
    BLOCK_CACHE_PINNED_USAGE("rocksdb.block-cache-pinned-usage"),
    /** Base compaction level (used for leveled compaction). */
    BASE_LEVEL("rocksdb.base-level"),
    /** Estimated bytes pending compaction at base level and above. */
    ESTIMATE_PENDING_COMPACTION_BYTES("rocksdb.estimate-pending-compaction-bytes"),
    /** Number of background errors that have occurred since the DB was opened. */
    BACKGROUND_ERRORS("rocksdb.background-errors");

    private final String propertyName;

    DBProperty(String propertyName) {
        this.propertyName = propertyName;
    }

    /** Returns the RocksDB property string passed to the C API. */
    public String propertyName() {
        return propertyName;
    }

    /**
     * Returns the property name for {@code rocksdb.num-files-at-level{N}}.
     * This property is parameterized by level and therefore not a fixed enum constant.
     *
     * @param level the compaction level (0 = L0)
     */
    public static String numFilesAtLevel(int level) {
        return "rocksdb.num-files-at-level" + level;
    }
}
