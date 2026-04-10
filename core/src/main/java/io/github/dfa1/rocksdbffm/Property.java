package io.github.dfa1.rocksdbffm;

/// Well-known RocksDB property names.
/// [Type#STRING] properties are available only via `getProperty`.
/// [Type#NUMERIC] properties (uint64) are available via both `getProperty`
/// and `getLongProperty`. Calling `getLongProperty` on a `STRING`
/// property throws [IllegalArgumentException].
///
/// For level-indexed properties (`num-files-at-level<N>`,
/// `compression-ratio-at-level<N>`, `aggregated-table-properties-at-level<N>`)
/// use the `_L0` … `_L6` suffixed constants.
public enum Property {

	// -----------------------------------------------------------------------
	// String-only properties (stats / info blobs)
	// -----------------------------------------------------------------------

	/// Multi-line human-readable statistics summary.
	STATS("rocksdb.stats", Type.STRING),
	/// Detailed listing of all SST files in the DB.
	SSTABLES("rocksdb.sstables", Type.STRING),
	/// Per-column-family statistics (includes file histogram).
	CF_STATS("rocksdb.cfstats", Type.STRING),
	/// Per-column-family statistics without the file histogram.
	CF_STATS_NO_FILE_HISTOGRAM("rocksdb.cfstats-no-file-histogram", Type.STRING),
	/// Per-column-family file-read latency histogram.
	CF_FILE_HISTOGRAM("rocksdb.cf-file-histogram", Type.STRING),
	/// Per-column-family write-stall statistics.
	CF_WRITE_STALL_STATS("rocksdb.cf-write-stall-stats", Type.STRING),
	/// Database-level write-stall statistics.
	DB_WRITE_STALL_STATS("rocksdb.db-write-stall-stats", Type.STRING),
	/// Database-level statistics (uptime, flush, compaction rates).
	DB_STATS("rocksdb.dbstats", Type.STRING),
	/// Per-level file count and size summary.
	LEVEL_STATS("rocksdb.levelstats", Type.STRING),
	/// Detailed block-cache entry statistics.
	BLOCK_CACHE_ENTRY_STATS("rocksdb.block-cache-entry-stats", Type.STRING),
	/// Same as [#BLOCK_CACHE_ENTRY_STATS] but skips full recalculation.
	FAST_BLOCK_CACHE_ENTRY_STATS("rocksdb.fast-block-cache-entry-stats", Type.STRING),
	/// Aggregated table properties across all SST files.
	AGGREGATED_TABLE_PROPERTIES("rocksdb.aggregated-table-properties", Type.STRING),
	/// Statistics from the options object (if statistics are enabled).
	OPTIONS_STATISTICS("rocksdb.options-statistics", Type.STRING),
	/// Total number and size of all blob files in the DB.
	BLOB_STATS("rocksdb.blob-stats", Type.STRING),

	// -----------------------------------------------------------------------
	// Numeric properties (uint64; also available as strings via getProperty)
	// -----------------------------------------------------------------------

	/// Number of immutable memtables that have not yet been flushed.
	NUM_IMMUTABLE_MEM_TABLE("rocksdb.num-immutable-mem-table", Type.NUMERIC),
	/// Number of immutable memtables that have already been flushed.
	NUM_IMMUTABLE_MEM_TABLE_FLUSHED("rocksdb.num-immutable-mem-table-flushed", Type.NUMERIC),
	/// 1 if a memtable flush is pending, 0 otherwise.
	MEM_TABLE_FLUSH_PENDING("rocksdb.mem-table-flush-pending", Type.NUMERIC),
	/// Number of flushes currently running in background threads.
	NUM_RUNNING_FLUSHES("rocksdb.num-running-flushes", Type.NUMERIC),
	/// 1 if at least one compaction is pending, 0 otherwise.
	COMPACTION_PENDING("rocksdb.compaction-pending", Type.NUMERIC),
	/// Number of compactions currently running in background threads.
	NUM_RUNNING_COMPACTIONS("rocksdb.num-running-compactions", Type.NUMERIC),
	/// Number of sorted runs in compactions currently running.
	NUM_RUNNING_COMPACTION_SORTED_RUNS("rocksdb.num-running-compaction-sorted-runs", Type.NUMERIC),
	/// Accumulated number of background errors since the DB was opened.
	BACKGROUND_ERRORS("rocksdb.background-errors", Type.NUMERIC),
	/// Approximate size (bytes) of the active memtable.
	CUR_SIZE_ACTIVE_MEM_TABLE("rocksdb.cur-size-active-mem-table", Type.NUMERIC),
	/// Approximate size (bytes) of active + unflushed immutable memtables.
	CUR_SIZE_ALL_MEM_TABLES("rocksdb.cur-size-all-mem-tables", Type.NUMERIC),
	/// Approximate size (bytes) of active + unflushed + pinned immutable memtables.
	SIZE_ALL_MEM_TABLES("rocksdb.size-all-mem-tables", Type.NUMERIC),
	/// Total number of entries in the active memtable.
	NUM_ENTRIES_ACTIVE_MEM_TABLE("rocksdb.num-entries-active-mem-table", Type.NUMERIC),
	/// Total number of entries in the unflushed immutable memtables.
	NUM_ENTRIES_IMM_MEM_TABLES("rocksdb.num-entries-imm-mem-tables", Type.NUMERIC),
	/// Total number of delete entries in the active memtable.
	NUM_DELETES_ACTIVE_MEM_TABLE("rocksdb.num-deletes-active-mem-table", Type.NUMERIC),
	/// Total number of delete entries in the unflushed immutable memtables.
	NUM_DELETES_IMM_MEM_TABLES("rocksdb.num-deletes-imm-mem-tables", Type.NUMERIC),
	/// Estimated number of keys in the DB (not exact).
	ESTIMATE_NUM_KEYS("rocksdb.estimate-num-keys", Type.NUMERIC),
	/// Estimated memory (bytes) used by index and filter blocks held in memory.
	ESTIMATE_TABLE_READERS_MEM("rocksdb.estimate-table-readers-mem", Type.NUMERIC),
	/// 0 if file deletions are disabled, 1 otherwise.
	IS_FILE_DELETIONS_ENABLED("rocksdb.is-file-deletions-enabled", Type.NUMERIC),
	/// Number of unreleased snapshots.
	NUM_SNAPSHOTS("rocksdb.num-snapshots", Type.NUMERIC),
	/// Unix timestamp of the oldest open snapshot.
	OLDEST_SNAPSHOT_TIME("rocksdb.oldest-snapshot-time", Type.NUMERIC),
	/// Sequence number of the oldest open snapshot.
	OLDEST_SNAPSHOT_SEQUENCE("rocksdb.oldest-snapshot-sequence", Type.NUMERIC),
	/// Number of live SST file versions (high = many open iterators/snapshots).
	NUM_LIVE_VERSIONS("rocksdb.num-live-versions", Type.NUMERIC),
	/// Sequence number of the current LSM version.
	CURRENT_SUPER_VERSION_NUMBER("rocksdb.current-super-version-number", Type.NUMERIC),
	/// Estimated total size (bytes) of live data.
	ESTIMATE_LIVE_DATA_SIZE("rocksdb.estimate-live-data-size", Type.NUMERIC),
	/// Minimum WAL log number that must be kept (not yet flushed to SST).
	MIN_LOG_NUMBER_TO_KEEP("rocksdb.min-log-number-to-keep", Type.NUMERIC),
	/// Minimum SST file number that must be kept (referenced by a checkpoint).
	MIN_OBSOLETE_SST_NUMBER_TO_KEEP("rocksdb.min-obsolete-sst-number-to-keep", Type.NUMERIC),
	/// Total file size (bytes) of all SST files, including obsolete ones.
	TOTAL_SST_FILES_SIZE("rocksdb.total-sst-files-size", Type.NUMERIC),
	/// Total file size (bytes) of all live SST files.
	LIVE_SST_FILES_SIZE("rocksdb.live-sst-files-size", Type.NUMERIC),
	/// Total file size (bytes) of obsolete SST files not yet deleted.
	OBSOLETE_SST_FILES_SIZE("rocksdb.obsolete-sst-files-size", Type.NUMERIC),
	/// Compaction base level (L0 data is compacted into this level).
	BASE_LEVEL("rocksdb.base-level", Type.NUMERIC),
	/// Estimated bytes of pending compaction at and above the base level.
	ESTIMATE_PENDING_COMPACTION_BYTES("rocksdb.estimate-pending-compaction-bytes", Type.NUMERIC),
	/// Current write throttle rate (bytes/s); 0 if not throttled.
	ACTUAL_DELAYED_WRITE_RATE("rocksdb.actual-delayed-write-rate", Type.NUMERIC),
	/// 1 if writes are completely stopped (stop condition), 0 otherwise.
	IS_WRITE_STOPPED("rocksdb.is-write-stopped", Type.NUMERIC),
	/// Estimated oldest key timestamp (for compaction of time-series data).
	ESTIMATE_OLDEST_KEY_TIME("rocksdb.estimate-oldest-key-time", Type.NUMERIC),
	/// Block cache capacity (bytes).
	BLOCK_CACHE_CAPACITY("rocksdb.block-cache-capacity", Type.NUMERIC),
	/// Block cache current usage (bytes).
	BLOCK_CACHE_USAGE("rocksdb.block-cache-usage", Type.NUMERIC),
	/// Block cache bytes pinned by iterators or block handles.
	BLOCK_CACHE_PINNED_USAGE("rocksdb.block-cache-pinned-usage", Type.NUMERIC),
	/// Number of blob files in the current version.
	NUM_BLOB_FILES("rocksdb.num-blob-files", Type.NUMERIC),
	/// Total size (bytes) of all blob files.
	TOTAL_BLOB_FILE_SIZE("rocksdb.total-blob-file-size", Type.NUMERIC),
	/// Total size (bytes) of all live blob files.
	LIVE_BLOB_FILE_SIZE("rocksdb.live-blob-file-size", Type.NUMERIC),
	/// Total garbage size (bytes) in blob files.
	LIVE_BLOB_FILE_GARBAGE_SIZE("rocksdb.live-blob-file-garbage-size", Type.NUMERIC),
	/// Blob cache capacity (bytes).
	BLOB_CACHE_CAPACITY("rocksdb.blob-cache-capacity", Type.NUMERIC),
	/// Blob cache current usage (bytes).
	BLOB_CACHE_USAGE("rocksdb.blob-cache-usage", Type.NUMERIC),
	/// Blob cache bytes pinned.
	BLOB_CACHE_PINNED_USAGE("rocksdb.blob-cache-pinned-usage", Type.NUMERIC),

	// -----------------------------------------------------------------------
	// Level-indexed: num-files-at-level<N>  (L0 – L6)  — NUMERIC
	// -----------------------------------------------------------------------

	NUM_FILES_AT_LEVEL_L0("rocksdb.num-files-at-level0", Type.NUMERIC),
	NUM_FILES_AT_LEVEL_L1("rocksdb.num-files-at-level1", Type.NUMERIC),
	NUM_FILES_AT_LEVEL_L2("rocksdb.num-files-at-level2", Type.NUMERIC),
	NUM_FILES_AT_LEVEL_L3("rocksdb.num-files-at-level3", Type.NUMERIC),
	NUM_FILES_AT_LEVEL_L4("rocksdb.num-files-at-level4", Type.NUMERIC),
	NUM_FILES_AT_LEVEL_L5("rocksdb.num-files-at-level5", Type.NUMERIC),
	NUM_FILES_AT_LEVEL_L6("rocksdb.num-files-at-level6", Type.NUMERIC),

	// -----------------------------------------------------------------------
	// Level-indexed: compression-ratio-at-level<N>  (L0 – L6)  — STRING (float)
	// -----------------------------------------------------------------------

	COMPRESSION_RATIO_AT_LEVEL_L0("rocksdb.compression-ratio-at-level0", Type.STRING),
	COMPRESSION_RATIO_AT_LEVEL_L1("rocksdb.compression-ratio-at-level1", Type.STRING),
	COMPRESSION_RATIO_AT_LEVEL_L2("rocksdb.compression-ratio-at-level2", Type.STRING),
	COMPRESSION_RATIO_AT_LEVEL_L3("rocksdb.compression-ratio-at-level3", Type.STRING),
	COMPRESSION_RATIO_AT_LEVEL_L4("rocksdb.compression-ratio-at-level4", Type.STRING),
	COMPRESSION_RATIO_AT_LEVEL_L5("rocksdb.compression-ratio-at-level5", Type.STRING),
	COMPRESSION_RATIO_AT_LEVEL_L6("rocksdb.compression-ratio-at-level6", Type.STRING),

	// -----------------------------------------------------------------------
	// Level-indexed: aggregated-table-properties-at-level<N>  (L0 – L6)  — STRING
	// -----------------------------------------------------------------------

	AGGREGATED_TABLE_PROPERTIES_AT_LEVEL_L0("rocksdb.aggregated-table-properties-at-level0", Type.STRING),
	AGGREGATED_TABLE_PROPERTIES_AT_LEVEL_L1("rocksdb.aggregated-table-properties-at-level1", Type.STRING),
	AGGREGATED_TABLE_PROPERTIES_AT_LEVEL_L2("rocksdb.aggregated-table-properties-at-level2", Type.STRING),
	AGGREGATED_TABLE_PROPERTIES_AT_LEVEL_L3("rocksdb.aggregated-table-properties-at-level3", Type.STRING),
	AGGREGATED_TABLE_PROPERTIES_AT_LEVEL_L4("rocksdb.aggregated-table-properties-at-level4", Type.STRING),
	AGGREGATED_TABLE_PROPERTIES_AT_LEVEL_L5("rocksdb.aggregated-table-properties-at-level5", Type.STRING),
	AGGREGATED_TABLE_PROPERTIES_AT_LEVEL_L6("rocksdb.aggregated-table-properties-at-level6", Type.STRING);

	// -----------------------------------------------------------------------

	/// The value type of RocksDB property.
	///
	///   - [#STRING] — human-readable text blob; only available via
	///     [RocksDB#getProperty(Property)].
	///   - [#NUMERIC] — uint64 counter or size; available via both
	///     [RocksDB#getProperty(Property)] and
	///     [RocksDB#getLongProperty(Property)].
	///
	public enum Type {
		STRING,
		NUMERIC
	}

	private final String propertyName;
	private final Type type;

	Property(String propertyName, Type type) {
		this.propertyName = propertyName;
		this.type = type;
	}

	/// Returns the RocksDB property string passed to the C API.
	public String propertyName() {
		return propertyName;
	}

	/// Returns whether this property is string-only or also numeric.
	/// TODO: still not used, the idea was to create a sealed return type for Java (e.g. PropertyValue either String or Long)
	public Type type() {
		return type;
	}
}
