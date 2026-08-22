package io.github.dfa1.rocksdbffm;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/// FFM wrapper for `rocksdb_options_t`.
///
/// Usage:
///
/// ```
/// try (Options opts = Options.newOptions().setCreateIfMissing(true)) {
///     RocksDB db = RocksDB.openReadWrite(opts, path);
/// }
/// ```
///
/// Note: the Options object must remain open until after RocksDB.openReadWrite() returns;
/// it can be closed immediately after that call.
public final class Options extends NativeObject {

	/// `rocksdb_options_t* rocksdb_options_create(void);`
	private static final MethodHandle MH_CREATE;
	/// `void rocksdb_options_destroy(rocksdb_options_t*);`
	private static final MethodHandle MH_DESTROY;
	/// `void rocksdb_options_set_create_if_missing(rocksdb_options_t*, unsigned char);`
	private static final MethodHandle MH_SET_CREATE_IF_MISSING;
	/// `unsigned char rocksdb_options_get_create_if_missing(rocksdb_options_t*);`
	private static final MethodHandle MH_GET_CREATE_IF_MISSING;
	/// `void rocksdb_options_set_block_based_table_factory(rocksdb_options_t* opt, rocksdb_block_based_table_options_t* table_options);`
	private static final MethodHandle MH_SET_BLOCK_BASED_TABLE_FACTORY;
	/// `void rocksdb_options_enable_statistics(rocksdb_options_t*);`
	private static final MethodHandle MH_ENABLE_STATISTICS;
	/// `void rocksdb_options_set_statistics_level(rocksdb_options_t*, int level);`
	private static final MethodHandle MH_SET_STATISTICS_LEVEL;
	/// `int rocksdb_options_get_statistics_level(rocksdb_options_t*);`
	private static final MethodHandle MH_GET_STATISTICS_LEVEL;
	/// `char* rocksdb_options_statistics_get_string(rocksdb_options_t* opt);`
	private static final MethodHandle MH_STATISTICS_GET_STRING;
	/// `uint64_t rocksdb_options_statistics_get_ticker_count(rocksdb_options_t* opt, uint32_t ticker_type);`
	private static final MethodHandle MH_STATISTICS_GET_TICKER_COUNT;
	/// `void rocksdb_options_statistics_get_histogram_data(rocksdb_options_t* opt, uint32_t histogram_type, rocksdb_statistics_histogram_data_t* const data);`
	private static final MethodHandle MH_STATISTICS_GET_HISTOGRAM_DATA;
	/// `void rocksdb_options_set_compression(rocksdb_options_t*, int);`
	private static final MethodHandle MH_SET_COMPRESSION;
	/// `int rocksdb_options_get_compression(rocksdb_options_t*);`
	private static final MethodHandle MH_GET_COMPRESSION;
	/// `void rocksdb_options_set_enable_blob_files(rocksdb_options_t* opt, unsigned char val);`
	private static final MethodHandle MH_SET_ENABLE_BLOB_FILES;
	/// `unsigned char rocksdb_options_get_enable_blob_files(rocksdb_options_t* opt);`
	private static final MethodHandle MH_GET_ENABLE_BLOB_FILES;
	/// `void rocksdb_options_set_min_blob_size(rocksdb_options_t* opt, uint64_t val);`
	private static final MethodHandle MH_SET_MIN_BLOB_SIZE;
	/// `uint64_t rocksdb_options_get_min_blob_size(rocksdb_options_t* opt);`
	private static final MethodHandle MH_GET_MIN_BLOB_SIZE;
	/// `void rocksdb_options_set_blob_file_size(rocksdb_options_t* opt, uint64_t val);`
	private static final MethodHandle MH_SET_BLOB_FILE_SIZE;
	/// `uint64_t rocksdb_options_get_blob_file_size(rocksdb_options_t* opt);`
	private static final MethodHandle MH_GET_BLOB_FILE_SIZE;
	/// `void rocksdb_options_set_blob_compression_type(rocksdb_options_t* opt, int val);`
	private static final MethodHandle MH_SET_BLOB_COMPRESSION_TYPE;
	/// `int rocksdb_options_get_blob_compression_type(rocksdb_options_t* opt);`
	private static final MethodHandle MH_GET_BLOB_COMPRESSION_TYPE;
	/// `void rocksdb_options_set_enable_blob_gc(rocksdb_options_t* opt, unsigned char val);`
	private static final MethodHandle MH_SET_ENABLE_BLOB_GC;
	/// `unsigned char rocksdb_options_get_enable_blob_gc(rocksdb_options_t* opt);`
	private static final MethodHandle MH_GET_ENABLE_BLOB_GC;
	/// `void rocksdb_options_set_blob_gc_age_cutoff(rocksdb_options_t* opt, double val);`
	private static final MethodHandle MH_SET_BLOB_GC_AGE_CUTOFF;
	/// `double rocksdb_options_get_blob_gc_age_cutoff(rocksdb_options_t* opt);`
	private static final MethodHandle MH_GET_BLOB_GC_AGE_CUTOFF;
	/// `void rocksdb_options_set_blob_gc_force_threshold(rocksdb_options_t* opt, double val);`
	private static final MethodHandle MH_SET_BLOB_GC_FORCE_THRESHOLD;
	/// `double rocksdb_options_get_blob_gc_force_threshold(rocksdb_options_t* opt);`
	private static final MethodHandle MH_GET_BLOB_GC_FORCE_THRESHOLD;
	/// `void rocksdb_options_set_blob_compaction_readahead_size(rocksdb_options_t* opt, uint64_t val);`
	private static final MethodHandle MH_SET_BLOB_COMPACTION_READAHEAD_SIZE;
	/// `uint64_t rocksdb_options_get_blob_compaction_readahead_size(rocksdb_options_t* opt);`
	private static final MethodHandle MH_GET_BLOB_COMPACTION_READAHEAD_SIZE;
	/// `void rocksdb_options_set_blob_file_starting_level(rocksdb_options_t* opt, int val);`
	private static final MethodHandle MH_SET_BLOB_FILE_STARTING_LEVEL;
	/// `int rocksdb_options_get_blob_file_starting_level(rocksdb_options_t* opt);`
	private static final MethodHandle MH_GET_BLOB_FILE_STARTING_LEVEL;
	/// `void rocksdb_options_set_blob_cache(rocksdb_options_t* opt, rocksdb_cache_t* blob_cache);`
	private static final MethodHandle MH_SET_BLOB_CACHE;
	/// `void rocksdb_options_set_prepopulate_blob_cache(rocksdb_options_t* opt, int val);`
	private static final MethodHandle MH_SET_PREPOPULATE_BLOB_CACHE;
	/// `int rocksdb_options_get_prepopulate_blob_cache(rocksdb_options_t* opt);`
	private static final MethodHandle MH_GET_PREPOPULATE_BLOB_CACHE;
	/// `void rocksdb_options_set_info_log(rocksdb_options_t*, rocksdb_logger_t*);`
	private static final MethodHandle MH_SET_INFO_LOG;
	/// `void rocksdb_options_set_info_log_level(rocksdb_options_t*, int);`
	private static final MethodHandle MH_SET_INFO_LOG_LEVEL;
	/// `int rocksdb_options_get_info_log_level(rocksdb_options_t*);`
	private static final MethodHandle MH_GET_INFO_LOG_LEVEL;
	/// `void rocksdb_options_set_ratelimiter(rocksdb_options_t* opt, rocksdb_ratelimiter_t* limiter);`
	private static final MethodHandle MH_SET_RATELIMITER;
	/// `void rocksdb_options_set_env(rocksdb_options_t*, rocksdb_env_t*);`
	private static final MethodHandle MH_SET_ENV;
	/// `void rocksdb_options_set_sst_file_manager(rocksdb_options_t* opt, rocksdb_sst_file_manager_t* sfm);`
	private static final MethodHandle MH_SET_SST_FILE_MANAGER;
	/// `void rocksdb_options_set_metadata_write_temperature(rocksdb_options_t* opt, int v);`
	private static final MethodHandle MH_SET_METADATA_WRITE_TEMPERATURE;
	/// `int rocksdb_options_get_metadata_write_temperature(rocksdb_options_t* opt);`
	private static final MethodHandle MH_GET_METADATA_WRITE_TEMPERATURE;
	/// `void rocksdb_options_set_wal_write_temperature(rocksdb_options_t* opt, int v);`
	private static final MethodHandle MH_SET_WAL_WRITE_TEMPERATURE;
	/// `int rocksdb_options_get_wal_write_temperature(rocksdb_options_t* opt);`
	private static final MethodHandle MH_GET_WAL_WRITE_TEMPERATURE;
	/// `void rocksdb_options_set_last_level_temperature(rocksdb_options_t* opt, int v);`
	private static final MethodHandle MH_SET_LAST_LEVEL_TEMPERATURE;
	/// `int rocksdb_options_get_last_level_temperature(rocksdb_options_t* opt);`
	private static final MethodHandle MH_GET_LAST_LEVEL_TEMPERATURE;
	/// `void rocksdb_options_set_default_write_temperature(rocksdb_options_t* opt, int v);`
	private static final MethodHandle MH_SET_DEFAULT_WRITE_TEMPERATURE;
	/// `int rocksdb_options_get_default_write_temperature(rocksdb_options_t* opt);`
	private static final MethodHandle MH_GET_DEFAULT_WRITE_TEMPERATURE;
	/// `void rocksdb_options_set_default_temperature(rocksdb_options_t* opt, int v);`
	private static final MethodHandle MH_SET_DEFAULT_TEMPERATURE;
	/// `int rocksdb_options_get_default_temperature(rocksdb_options_t* opt);`
	private static final MethodHandle MH_GET_DEFAULT_TEMPERATURE;
	static {
		MH_CREATE = NativeLibrary.lookup("rocksdb_options_create",
				FunctionDescriptor.of(ValueLayout.ADDRESS));

		MH_DESTROY = NativeLibrary.lookup("rocksdb_options_destroy",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

		MH_SET_CREATE_IF_MISSING = NativeLibrary.lookup("rocksdb_options_set_create_if_missing",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_BYTE));

		MH_GET_CREATE_IF_MISSING = NativeLibrary.lookup("rocksdb_options_get_create_if_missing",
				FunctionDescriptor.of(ValueLayout.JAVA_BYTE, ValueLayout.ADDRESS));

		MH_SET_BLOCK_BASED_TABLE_FACTORY = NativeLibrary.lookup(
				"rocksdb_options_set_block_based_table_factory",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_ENABLE_STATISTICS = NativeLibrary.lookup("rocksdb_options_enable_statistics",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

		MH_SET_STATISTICS_LEVEL = NativeLibrary.lookup("rocksdb_options_set_statistics_level",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_INT));

		MH_GET_STATISTICS_LEVEL = NativeLibrary.lookup("rocksdb_options_get_statistics_level",
				FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));

		MH_STATISTICS_GET_STRING = NativeLibrary.lookup("rocksdb_options_statistics_get_string",
				FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_STATISTICS_GET_TICKER_COUNT = NativeLibrary.lookup("rocksdb_options_statistics_get_ticker_count",
				FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.JAVA_INT));

		MH_STATISTICS_GET_HISTOGRAM_DATA = NativeLibrary.lookup("rocksdb_options_statistics_get_histogram_data",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_INT, ValueLayout.ADDRESS));

		MH_SET_COMPRESSION = NativeLibrary.lookup("rocksdb_options_set_compression",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_INT));

		MH_GET_COMPRESSION = NativeLibrary.lookup("rocksdb_options_get_compression",
				FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));

		MH_SET_ENABLE_BLOB_FILES = NativeLibrary.lookup("rocksdb_options_set_enable_blob_files",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_BYTE));

		MH_GET_ENABLE_BLOB_FILES = NativeLibrary.lookup("rocksdb_options_get_enable_blob_files",
				FunctionDescriptor.of(ValueLayout.JAVA_BYTE, ValueLayout.ADDRESS));

		MH_SET_MIN_BLOB_SIZE = NativeLibrary.lookup("rocksdb_options_set_min_blob_size",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));

		MH_GET_MIN_BLOB_SIZE = NativeLibrary.lookup("rocksdb_options_get_min_blob_size",
				FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));

		MH_SET_BLOB_FILE_SIZE = NativeLibrary.lookup("rocksdb_options_set_blob_file_size",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));

		MH_GET_BLOB_FILE_SIZE = NativeLibrary.lookup("rocksdb_options_get_blob_file_size",
				FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));

		MH_SET_BLOB_COMPRESSION_TYPE = NativeLibrary.lookup("rocksdb_options_set_blob_compression_type",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_INT));

		MH_GET_BLOB_COMPRESSION_TYPE = NativeLibrary.lookup("rocksdb_options_get_blob_compression_type",
				FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));

		MH_SET_ENABLE_BLOB_GC = NativeLibrary.lookup("rocksdb_options_set_enable_blob_gc",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_BYTE));

		MH_GET_ENABLE_BLOB_GC = NativeLibrary.lookup("rocksdb_options_get_enable_blob_gc",
				FunctionDescriptor.of(ValueLayout.JAVA_BYTE, ValueLayout.ADDRESS));

		MH_SET_BLOB_GC_AGE_CUTOFF = NativeLibrary.lookup("rocksdb_options_set_blob_gc_age_cutoff",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_DOUBLE));

		MH_GET_BLOB_GC_AGE_CUTOFF = NativeLibrary.lookup("rocksdb_options_get_blob_gc_age_cutoff",
				FunctionDescriptor.of(ValueLayout.JAVA_DOUBLE, ValueLayout.ADDRESS));

		MH_SET_BLOB_GC_FORCE_THRESHOLD = NativeLibrary.lookup("rocksdb_options_set_blob_gc_force_threshold",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_DOUBLE));

		MH_GET_BLOB_GC_FORCE_THRESHOLD = NativeLibrary.lookup("rocksdb_options_get_blob_gc_force_threshold",
				FunctionDescriptor.of(ValueLayout.JAVA_DOUBLE, ValueLayout.ADDRESS));

		MH_SET_BLOB_COMPACTION_READAHEAD_SIZE = NativeLibrary.lookup(
				"rocksdb_options_set_blob_compaction_readahead_size",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));

		MH_GET_BLOB_COMPACTION_READAHEAD_SIZE = NativeLibrary.lookup(
				"rocksdb_options_get_blob_compaction_readahead_size",
				FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));

		MH_SET_BLOB_FILE_STARTING_LEVEL = NativeLibrary.lookup("rocksdb_options_set_blob_file_starting_level",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_INT));

		MH_GET_BLOB_FILE_STARTING_LEVEL = NativeLibrary.lookup("rocksdb_options_get_blob_file_starting_level",
				FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));

		MH_SET_BLOB_CACHE = NativeLibrary.lookup("rocksdb_options_set_blob_cache",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_SET_PREPOPULATE_BLOB_CACHE = NativeLibrary.lookup("rocksdb_options_set_prepopulate_blob_cache",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_INT));

		MH_GET_PREPOPULATE_BLOB_CACHE = NativeLibrary.lookup("rocksdb_options_get_prepopulate_blob_cache",
				FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));

		MH_SET_INFO_LOG = NativeLibrary.lookup("rocksdb_options_set_info_log",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_SET_INFO_LOG_LEVEL = NativeLibrary.lookup("rocksdb_options_set_info_log_level",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_INT));

		MH_GET_INFO_LOG_LEVEL = NativeLibrary.lookup("rocksdb_options_get_info_log_level",
				FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));

		MH_SET_RATELIMITER = NativeLibrary.lookup("rocksdb_options_set_ratelimiter",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_SET_ENV = NativeLibrary.lookup("rocksdb_options_set_env",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_SET_SST_FILE_MANAGER = NativeLibrary.lookup("rocksdb_options_set_sst_file_manager",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_SET_METADATA_WRITE_TEMPERATURE = NativeLibrary.lookup("rocksdb_options_set_metadata_write_temperature",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_INT));

		MH_GET_METADATA_WRITE_TEMPERATURE = NativeLibrary.lookup("rocksdb_options_get_metadata_write_temperature",
				FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));

		MH_SET_WAL_WRITE_TEMPERATURE = NativeLibrary.lookup("rocksdb_options_set_wal_write_temperature",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_INT));

		MH_GET_WAL_WRITE_TEMPERATURE = NativeLibrary.lookup("rocksdb_options_get_wal_write_temperature",
				FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));

		MH_SET_LAST_LEVEL_TEMPERATURE = NativeLibrary.lookup("rocksdb_options_set_last_level_temperature",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_INT));

		MH_GET_LAST_LEVEL_TEMPERATURE = NativeLibrary.lookup("rocksdb_options_get_last_level_temperature",
				FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));

		MH_SET_DEFAULT_WRITE_TEMPERATURE = NativeLibrary.lookup("rocksdb_options_set_default_write_temperature",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_INT));

		MH_GET_DEFAULT_WRITE_TEMPERATURE = NativeLibrary.lookup("rocksdb_options_get_default_write_temperature",
				FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));

		MH_SET_DEFAULT_TEMPERATURE = NativeLibrary.lookup("rocksdb_options_set_default_temperature",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_INT));

		MH_GET_DEFAULT_TEMPERATURE = NativeLibrary.lookup("rocksdb_options_get_default_temperature",
				FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));

	}

	private Options(MemorySegment ptr) {
		super(ptr);
	}

	/// Creates [Options] with RocksDB defaults.
	///
	/// @return a new instance; caller must close it
	public static Options newOptions() {
		try {
			return new Options((MemorySegment) MH_CREATE.invokeExact());
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("options create failed", t);
		}
	}

	/// If true, the database will be created if it does not already exist.
	/// Default: false (same as RocksDB C++ default).
	///
	/// @param value `true` to create the DB if absent
	/// @return `this` for chaining
	public Options setCreateIfMissing(boolean value) {
		try {
			MH_SET_CREATE_IF_MISSING.invokeExact(ptr(), RocksDB.toByte(value));
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("setCreateIfMissing failed", t);
		}
		return this;
	}

	/// Returns whether the DB is created if it does not already exist.
	///
	/// @return `true` if the DB is created on open when absent
	public boolean getCreateIfMissing() {
		try {
			return RocksDB.fromByte((byte) MH_GET_CREATE_IF_MISSING.invokeExact(ptr()));
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("getCreateIfMissing failed", t);
		}
	}

	/// Enables statistics gathering for this DB.
	///
	/// @return `this` for chaining
	public Options enableStatistics() {
		try {
			MH_ENABLE_STATISTICS.invokeExact(ptr());
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("enableStatistics failed", t);
		}
		return this;
	}

	/// Sets the statistics collection level. Only effective after [#enableStatistics] is called.
	///
	/// @param level the desired statistics collection level
	/// @return `this` for chaining
	public Options setStatisticsLevel(StatsLevel level) {
		try {
			MH_SET_STATISTICS_LEVEL.invokeExact(ptr(), level.getValue());
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("setStatisticsLevel failed", t);
		}
		return this;
	}

	/// Returns the current statistics collection level.
	///
	/// @return the active [StatsLevel]
	public StatsLevel getStatisticsLevel() {
		try {
			int level = (int) MH_GET_STATISTICS_LEVEL.invokeExact(ptr());
			return StatsLevel.fromValue(level);
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("getStatisticsLevel failed", t);
		}
	}

	/// Returns a human-readable statistics summary, or `null` if statistics are not enabled.
	///
	/// @return formatted statistics string, or `null` if not available
	public String getStatisticsString() {
		try {
			MemorySegment strPtr = (MemorySegment) MH_STATISTICS_GET_STRING.invokeExact(ptr());
			if (MemorySegment.NULL.equals(strPtr)) {
				return null;
			}
			return RocksDB.toJavaString(strPtr);
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("getStatisticsString failed", t);
		}
	}

	/// Returns the accumulated count for a ticker statistic.
	///
	/// @param ticker the ticker to read
	/// @return accumulated count since the DB was opened
	public long getTickerCount(TickerType ticker) {
		try {
			return (long) MH_STATISTICS_GET_TICKER_COUNT.invokeExact(ptr(), ticker.getValue());
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("getTickerCount failed", t);
		}
	}

	/// Populates `data` with histogram statistics for `histogram`.
	///
	/// @param histogram the histogram to read
	/// @param data      output object to populate with the histogram values
	public void getHistogramData(HistogramType histogram, StatisticsHistogramData data) {
		try {
			MH_STATISTICS_GET_HISTOGRAM_DATA.invokeExact(ptr(), histogram.getValue(), data.ptr());
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("getHistogramData failed", t);
		}
	}


	/// Sets the compression algorithm for all levels.
	///
	/// @param type the compression algorithm to use
	/// @return `this` for chaining
	/// @throws UnsupportedOperationException if `type` isn't linked into the bundled native library
	public Options setCompression(CompressionType type) {
		if (!type.isSupported()) {
			throw new UnsupportedOperationException(type + " compression is not linked into the bundled native library");
		}
		try {
			MH_SET_COMPRESSION.invokeExact(ptr(), type.getValue());
			return this;
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("setCompression failed", t);
		}
	}

	/// Returns the compression algorithm configured for this Options.
	///
	/// @return the active compression type
	public CompressionType getCompression() {
		try {
			return CompressionType.fromValue((int) MH_GET_COMPRESSION.invokeExact(ptr()));
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("getCompression failed", t);
		}
	}

	/// Configures block-based table format for this DB.
	/// RocksDB copies the config internally; `tableConfig` may be closed after this call.
	///
	/// @param tableConfig the block-based table options to apply
	/// @return `this` for chaining
	public Options setTableFormatConfig(BlockBasedTableOptions tableConfig) {
		try {
			MH_SET_BLOCK_BASED_TABLE_FACTORY.invokeExact(ptr(), tableConfig.ptr());
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("setTableFormatConfig failed", t);
		}
		return this;
	}

	// -----------------------------------------------------------------------
	// Blob file options
	// -----------------------------------------------------------------------

	/// Enables storing large values in separate blob files instead of inline in SSTs.
	/// When enabled, values ≥ [#setMinBlobSize] are written to blob files.
	/// Default: `false`.
	///
	/// @param value `true` to enable blob file storage
	/// @return `this` for chaining
	public Options setEnableBlobFiles(boolean value) {
		try {
			MH_SET_ENABLE_BLOB_FILES.invokeExact(ptr(), RocksDB.toByte(value));
			return this;
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("setEnableBlobFiles failed", t);
		}
	}

	/// Returns whether blob file storage is enabled.
	///
	/// @return `true` if large values are stored in separate blob files
	public boolean getEnableBlobFiles() {
		try {
			return RocksDB.fromByte((byte) MH_GET_ENABLE_BLOB_FILES.invokeExact(ptr()));
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("getEnableBlobFiles failed", t);
		}
	}

	/// Values strictly smaller than this size are stored inline; larger values go to blob files.
	/// Only effective when [#setEnableBlobFiles] is `true`. Default: 0 (all values externalized).
	///
	/// @param size minimum value size to externalize into a blob file
	/// @return `this` for chaining
	public Options setMinBlobSize(MemorySize size) {
		try {
			MH_SET_MIN_BLOB_SIZE.invokeExact(ptr(), size.toBytes());
			return this;
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("setMinBlobSize failed", t);
		}
	}

	/// Returns the minimum value size that is stored in a blob file.
	///
	/// @return minimum blob size threshold
	public MemorySize getMinBlobSize() {
		try {
			return MemorySize.ofBytes((long) MH_GET_MIN_BLOB_SIZE.invokeExact(ptr()));
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("getMinBlobSize failed", t);
		}
	}

	/// Target size for individual blob files. RocksDB rolls to a new file when this is exceeded.
	/// Default: 256 MiB.
	///
	/// @param size target size per blob file
	/// @return `this` for chaining
	public Options setBlobFileSize(MemorySize size) {
		try {
			MH_SET_BLOB_FILE_SIZE.invokeExact(ptr(), size.toBytes());
			return this;
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("setBlobFileSize failed", t);
		}
	}

	/// Returns the target size for individual blob files.
	///
	/// @return target blob file size
	public MemorySize getBlobFileSize() {
		try {
			return MemorySize.ofBytes((long) MH_GET_BLOB_FILE_SIZE.invokeExact(ptr()));
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("getBlobFileSize failed", t);
		}
	}

	/// Compression algorithm applied to blob file values. Independent of SST compression.
	/// Default: [CompressionType#NO_COMPRESSION].
	///
	/// @param type the compression algorithm for blob values
	/// @return `this` for chaining
	/// @throws UnsupportedOperationException if `type` isn't linked into the bundled native library
	public Options setBlobCompressionType(CompressionType type) {
		if (!type.isSupported()) {
			throw new UnsupportedOperationException(type + " compression is not linked into the bundled native library");
		}
		try {
			MH_SET_BLOB_COMPRESSION_TYPE.invokeExact(ptr(), type.getValue());
			return this;
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("setBlobCompressionType failed", t);
		}
	}

	/// Returns the compression algorithm applied to blob file values.
	///
	/// @return compression type for blob values
	public CompressionType getBlobCompressionType() {
		try {
			return CompressionType.fromValue((int) MH_GET_BLOB_COMPRESSION_TYPE.invokeExact(ptr()));
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("getBlobCompressionType failed", t);
		}
	}

	/// Enables garbage collection of obsolete blob files during compaction.
	/// Default: `false`.
	///
	/// @param value `true` to enable blob GC during compaction
	/// @return `this` for chaining
	public Options setEnableBlobGc(boolean value) {
		try {
			MH_SET_ENABLE_BLOB_GC.invokeExact(ptr(), RocksDB.toByte(value));
			return this;
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("setEnableBlobGc failed", t);
		}
	}

	/// Returns whether blob garbage collection during compaction is enabled.
	///
	/// @return `true` if blob GC is enabled
	public boolean getEnableBlobGc() {
		try {
			return RocksDB.fromByte((byte) MH_GET_ENABLE_BLOB_GC.invokeExact(ptr()));
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("getEnableBlobGc failed", t);
		}
	}

	/// Blob files whose age is older than this fraction of the oldest snapshot are
	/// unconditionally GC'd, regardless of garbage ratio.
	/// Default: 0.5.
	///
	/// @param value age cutoff fraction
	/// @return `this` for chaining
	public Options setBlobGcAgeCutoff(Ratio value) {
		try {
			MH_SET_BLOB_GC_AGE_CUTOFF.invokeExact(ptr(), value.toDouble());
			return this;
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("setBlobGcAgeCutoff failed", t);
		}
	}

	/// Returns the blob GC age cutoff fraction.
	///
	/// @return age cutoff fraction
	public Ratio getBlobGcAgeCutoff() {
		try {
			return Ratio.of((double) MH_GET_BLOB_GC_AGE_CUTOFF.invokeExact(ptr()));
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("getBlobGcAgeCutoff failed", t);
		}
	}

	/// Blob files whose garbage ratio exceeds this threshold are force-compacted.
	/// Default: 1.0 (disabled).
	///
	/// @param value force-GC garbage ratio threshold
	/// @return `this` for chaining
	public Options setBlobGcForceThreshold(Ratio value) {
		try {
			MH_SET_BLOB_GC_FORCE_THRESHOLD.invokeExact(ptr(), value.toDouble());
			return this;
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("setBlobGcForceThreshold failed", t);
		}
	}

	/// Returns the blob GC force-compaction garbage ratio threshold.
	///
	/// @return force-GC threshold
	public Ratio getBlobGcForceThreshold() {
		try {
			return Ratio.of((double) MH_GET_BLOB_GC_FORCE_THRESHOLD.invokeExact(ptr()));
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("getBlobGcForceThreshold failed", t);
		}
	}

	/// Read-ahead size when reading blob files during compaction.
	/// `0` disables read-ahead. Default: 0.
	///
	/// @param size read-ahead buffer size; `MemorySize.ofBytes(0)` disables it
	/// @return `this` for chaining
	public Options setBlobCompactionReadaheadSize(MemorySize size) {
		try {
			MH_SET_BLOB_COMPACTION_READAHEAD_SIZE.invokeExact(ptr(), size.toBytes());
			return this;
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("setBlobCompactionReadaheadSize failed", t);
		}
	}

	/// Returns the read-ahead size used when reading blob files during compaction.
	///
	/// @return read-ahead size; [MemorySize#ZERO] means disabled
	public MemorySize getBlobCompactionReadaheadSize() {
		try {
			return MemorySize.ofBytes((long) MH_GET_BLOB_COMPACTION_READAHEAD_SIZE.invokeExact(ptr()));
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("getBlobCompactionReadaheadSize failed", t);
		}
	}

	/// LSM level at which blob file separation begins. Keys in levels below this
	/// threshold are stored inline. Default: 0 (all levels externalize blobs).
	///
	/// @param level first LSM level where blobs are externalized (0 = all levels)
	/// @return `this` for chaining
	public Options setBlobFileStartingLevel(int level) {
		try {
			MH_SET_BLOB_FILE_STARTING_LEVEL.invokeExact(ptr(), level);
			return this;
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("setBlobFileStartingLevel failed", t);
		}
	}

	/// Returns the LSM level at which blob file separation begins.
	///
	/// @return first level where blobs are externalized (0 = all levels)
	public int getBlobFileStartingLevel() {
		try {
			return (int) MH_GET_BLOB_FILE_STARTING_LEVEL.invokeExact(ptr());
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("getBlobFileStartingLevel failed", t);
		}
	}

	/// Attaches a dedicated cache for blob values.
	/// Ownership of the cache is shared; the cache must outlive this Options object.
	///
	/// @param cache the blob cache to attach
	/// @return `this` for chaining
	public Options setBlobCache(Cache cache) {
		try {
			MH_SET_BLOB_CACHE.invokeExact(ptr(), cache.ptr());
			return this;
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("setBlobCache failed", t);
		}
	}

	/// Controls whether blob values are pre-populated into the blob cache on write.
	/// Default: [PrepopulateBlobCache#DISABLE].
	///
	/// @param mode the pre-population strategy
	/// @return `this` for chaining
	public Options setPrepopulateBlobCache(PrepopulateBlobCache mode) {
		try {
			MH_SET_PREPOPULATE_BLOB_CACHE.invokeExact(ptr(), mode.value);
			return this;
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("setPrepopulateBlobCache failed", t);
		}
	}

	/// Returns the blob cache pre-population strategy.
	///
	/// @return the current [PrepopulateBlobCache] mode
	public PrepopulateBlobCache getPrepopulateBlobCache() {
		try {
			return PrepopulateBlobCache.fromValue((int) MH_GET_PREPOPULATE_BLOB_CACHE.invokeExact(ptr()));
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("getPrepopulateBlobCache failed", t);
		}
	}

	// -----------------------------------------------------------------------
	// Logging options
	// -----------------------------------------------------------------------

	/// Sets the logger for this DB. RocksDB holds a shared reference; it is safe
	/// to close [Logger] after this call.
	///
	/// @param logger the logger instance to attach
	/// @return `this` for chaining
	public Options setInfoLog(Logger logger) {
		try {
			MH_SET_INFO_LOG.invokeExact(ptr(), logger.ptr());
			return this;
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("setInfoLog failed", t);
		}
	}

	/// Sets the minimum log level. Messages below this level are suppressed.
	///
	/// @param level the minimum log level to emit
	/// @return `this` for chaining
	public Options setInfoLogLevel(LogLevel level) {
		try {
			MH_SET_INFO_LOG_LEVEL.invokeExact(ptr(), level.value);
			return this;
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("setInfoLogLevel failed", t);
		}
	}

	/// Returns the minimum log level currently configured.
	///
	/// @return the active minimum [LogLevel]
	public LogLevel getInfoLogLevel() {
		try {
			return LogLevel.fromValue((int) MH_GET_INFO_LOG_LEVEL.invokeExact(ptr()));
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("getInfoLogLevel failed", t);
		}
	}

	/// Sets the [Env] used for all file-system and threading operations.
	///
	/// The [Env] must remain open for the lifetime of the database.
	/// No ownership transfer: both objects may be closed independently.
	///
	/// @param env the environment to use
	/// @return `this` for chaining
	public Options setEnv(Env env) {
		try {
			MH_SET_ENV.invokeExact(ptr(), env.ptr());
			return this;
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("setEnv failed", t);
		}
	}

	/// Attaches an [SstFileManager] to track SST files and enforce disk-space limits.
	///
	/// No ownership transfer: both objects may be closed independently.
	///
	/// @param sfm the SST file manager to attach
	/// @return `this` for chaining
	public Options setSstFileManager(SstFileManager sfm) {
		try {
			MH_SET_SST_FILE_MANAGER.invokeExact(ptr(), sfm.ptr());
			return this;
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("setSstFileManager failed", t);
		}
	}

	/// Attaches a [RateLimiter] to throttle compaction and flush I/O.
	///
	/// The rate limiter uses shared ownership: this call does not transfer
	/// ownership — both objects may be closed independently.
	///
	/// @param rateLimiter the rate limiter to attach
	/// @return `this` for chaining
	public Options setRateLimiter(RateLimiter rateLimiter) {
		try {
			MH_SET_RATELIMITER.invokeExact(ptr(), rateLimiter.ptr());
			return this;
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("setRateLimiter failed", t);
		}
	}

	/// Attaches a [MergeOperator] so `merge()` calls have a defined semantics for this column
	/// family. Without one configured, every `merge()` call fails with [RocksDBException].
	///
	/// A [MergeOperator.Custom] transfers ownership: its `close()` becomes a no-op afterward.
	/// [MergeOperator#uint64Add()] holds no native handle and needs no ownership transfer.
	///
	/// @param mergeOperator the merge operator to attach
	/// @return `this` for chaining
	public Options setMergeOperator(MergeOperator mergeOperator) {
		switch (mergeOperator) {
			case MergeOperator.Uint64Add u -> u.applyTo(ptr());
			case MergeOperator.Custom c -> c.applyTo(ptr());
		}
		return this;
	}

	/// Attaches an [EventNotifier] that receives RocksDB's internal lifecycle events (flushes,
	/// compactions, external file ingestion, background errors, write stalls, memtable seals).
	///
	/// May be called more than once; every attached notifier is dispatched to independently.
	///
	/// @param notifier the callback to attach
	/// @return `this` for chaining
	public Options addEventListener(EventNotifier notifier) {
		EventNotifierBridge.attach(ptr(), notifier);
		return this;
	}

	// -----------------------------------------------------------------------
	// Temperature options
	// -----------------------------------------------------------------------

	/// Sets the temperature hint for metadata block-based tables (index, filter, etc.).
	/// EXPERIMENTAL. Default: [Temperature#UNKNOWN].
	///
	/// @param temperature the temperature hint to use for metadata files
	/// @return `this` for chaining
	public Options setMetadataWriteTemperature(Temperature temperature) {
		try {
			MH_SET_METADATA_WRITE_TEMPERATURE.invokeExact(ptr(), temperature.getValue());
			return this;
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("setMetadataWriteTemperature failed", t);
		}
	}

	/// Returns the temperature hint configured for metadata block-based tables.
	///
	/// @return the active [Temperature] hint for metadata files
	public Temperature getMetadataWriteTemperature() {
		try {
			return Temperature.fromValue((int) MH_GET_METADATA_WRITE_TEMPERATURE.invokeExact(ptr()));
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("getMetadataWriteTemperature failed", t);
		}
	}

	/// Sets the temperature hint for WAL files.
	/// EXPERIMENTAL. Default: [Temperature#UNKNOWN].
	///
	/// @param temperature the temperature hint to use for WAL files
	/// @return `this` for chaining
	public Options setWalWriteTemperature(Temperature temperature) {
		try {
			MH_SET_WAL_WRITE_TEMPERATURE.invokeExact(ptr(), temperature.getValue());
			return this;
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("setWalWriteTemperature failed", t);
		}
	}

	/// Returns the temperature hint configured for WAL files.
	///
	/// @return the active [Temperature] hint for WAL files
	public Temperature getWalWriteTemperature() {
		try {
			return Temperature.fromValue((int) MH_GET_WAL_WRITE_TEMPERATURE.invokeExact(ptr()));
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("getWalWriteTemperature failed", t);
		}
	}

	/// Sets the temperature hint for SST files placed on the last level.
	/// EXPERIMENTAL. Default: [Temperature#UNKNOWN].
	///
	/// @param temperature the temperature hint to use for last-level files
	/// @return `this` for chaining
	public Options setLastLevelTemperature(Temperature temperature) {
		try {
			MH_SET_LAST_LEVEL_TEMPERATURE.invokeExact(ptr(), temperature.getValue());
			return this;
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("setLastLevelTemperature failed", t);
		}
	}

	/// Returns the temperature hint configured for SST files on the last level.
	///
	/// @return the active [Temperature] hint for last-level files
	public Temperature getLastLevelTemperature() {
		try {
			return Temperature.fromValue((int) MH_GET_LAST_LEVEL_TEMPERATURE.invokeExact(ptr()));
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("getLastLevelTemperature failed", t);
		}
	}

	/// Sets the temperature hint used when a new SST file is written, for levels
	/// that don't otherwise have an explicit temperature configured.
	/// EXPERIMENTAL. Default: [Temperature#UNKNOWN].
	///
	/// @param temperature the temperature hint to use for newly written files
	/// @return `this` for chaining
	public Options setDefaultWriteTemperature(Temperature temperature) {
		try {
			MH_SET_DEFAULT_WRITE_TEMPERATURE.invokeExact(ptr(), temperature.getValue());
			return this;
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("setDefaultWriteTemperature failed", t);
		}
	}

	/// Returns the temperature hint configured for newly written SST files.
	///
	/// @return the active default write [Temperature] hint
	public Temperature getDefaultWriteTemperature() {
		try {
			return Temperature.fromValue((int) MH_GET_DEFAULT_WRITE_TEMPERATURE.invokeExact(ptr()));
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("getDefaultWriteTemperature failed", t);
		}
	}

	/// Sets the temperature hint assumed for existing SST files that have no
	/// temperature recorded in their metadata (e.g. files created before this
	/// option existed).
	/// EXPERIMENTAL. Default: [Temperature#UNKNOWN].
	///
	/// @param temperature the fallback temperature hint for files without a recorded temperature
	/// @return `this` for chaining
	public Options setDefaultTemperature(Temperature temperature) {
		try {
			MH_SET_DEFAULT_TEMPERATURE.invokeExact(ptr(), temperature.getValue());
			return this;
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("setDefaultTemperature failed", t);
		}
	}

	/// Returns the fallback temperature hint for files without a recorded temperature.
	///
	/// @return the active default [Temperature] hint
	public Temperature getDefaultTemperature() {
		try {
			return Temperature.fromValue((int) MH_GET_DEFAULT_TEMPERATURE.invokeExact(ptr()));
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("getDefaultTemperature failed", t);
		}
	}

	@Override
	protected void tryClose(MemorySegment ptr) throws Throwable {
		MH_DESTROY.invokeExact(ptr);
	}
}
