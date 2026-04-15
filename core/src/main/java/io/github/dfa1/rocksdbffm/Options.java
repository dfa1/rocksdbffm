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
///     RocksDB db = RocksDB.open(opts, path);
/// }
/// ```
///
/// Note: the Options object must remain open until after RocksDB.open() returns;
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

	}

	private Options(MemorySegment ptr) {
		super(ptr);
	}

	public static Options newOptions() {
		try {
			return new Options((MemorySegment) MH_CREATE.invokeExact());
		} catch (Throwable t) {
			throw new RocksDBException("options create failed", t);
		}
	}

	/// If true, the database will be created if it does not already exist.
	/// Default: false (same as RocksDB C++ default).
	public Options setCreateIfMissing(boolean value) {
		try {
			MH_SET_CREATE_IF_MISSING.invokeExact(ptr(), value ? (byte) 1 : (byte) 0);
		} catch (Throwable t) {
			throw new RocksDBException("setCreateIfMissing failed", t);
		}
		return this;
	}

	public boolean getCreateIfMissing() {
		try {
			return ((byte) MH_GET_CREATE_IF_MISSING.invokeExact(ptr())) != 0;
		} catch (Throwable t) {
			throw new RocksDBException("getCreateIfMissing failed", t);
		}
	}

	/// Enables statistics gathering for this DB.
	public Options enableStatistics() {
		try {
			MH_ENABLE_STATISTICS.invokeExact(ptr());
		} catch (Throwable t) {
			throw new RocksDBException("enableStatistics failed", t);
		}
		return this;
	}

	public Options setStatisticsLevel(StatsLevel level) {
		try {
			MH_SET_STATISTICS_LEVEL.invokeExact(ptr(), level.getValue());
		} catch (Throwable t) {
			throw new RocksDBException("setStatisticsLevel failed", t);
		}
		return this;
	}

	public StatsLevel getStatisticsLevel() {
		try {
			int level = (int) MH_GET_STATISTICS_LEVEL.invokeExact(ptr());
			for (StatsLevel l : StatsLevel.values()) {
				if (l.getValue() == level) {
					return l;
				}
			}
			return StatsLevel.DISABLE_ALL;
		} catch (Throwable t) {
			throw new RocksDBException("getStatisticsLevel failed", t);
		}
	}

	public String getStatisticsString() {
		try {
			MemorySegment strPtr = (MemorySegment) MH_STATISTICS_GET_STRING.invokeExact(ptr());
			if (MemorySegment.NULL.equals(strPtr)) {
				return null;
			}
			String result = strPtr.reinterpret(Long.MAX_VALUE).getString(0);
			RocksDB.free(strPtr);
			return result;
		} catch (Throwable t) {
			throw new RocksDBException("getStatisticsString failed", t);
		}
	}

	public long getTickerCount(TickerType ticker) {
		try {
			return (long) MH_STATISTICS_GET_TICKER_COUNT.invokeExact(ptr(), ticker.getValue());
		} catch (Throwable t) {
			throw new RocksDBException("getTickerCount failed", t);
		}
	}

	public void getHistogramData(HistogramType histogram, StatisticsHistogramData data) {
		try {
			MH_STATISTICS_GET_HISTOGRAM_DATA.invokeExact(ptr(), histogram.getValue(), data.ptr());
		} catch (Throwable t) {
			throw new RocksDBException("getHistogramData failed", t);
		}
	}


	/// Sets the compression algorithm for all levels.
	///
	/// @return `this` for chaining
	public Options setCompression(CompressionType type) {
		try {
			MH_SET_COMPRESSION.invokeExact(ptr(), type.value);
			return this;
		} catch (Throwable t) {
			throw new RocksDBException("setCompression failed", t);
		}
	}

	/// Returns the compression algorithm configured for this Options.
	public CompressionType getCompression() {
		try {
			return CompressionType.fromValue((int) MH_GET_COMPRESSION.invokeExact(ptr()));
		} catch (Throwable t) {
			throw new RocksDBException("getCompression failed", t);
		}
	}

	/// Configures block-based table format for this DB.
	/// RocksDB copies the config internally; `tableConfig` may be closed after this call.
	public Options setTableFormatConfig(BlockBasedTableOptions tableConfig) {
		try {
			MH_SET_BLOCK_BASED_TABLE_FACTORY.invokeExact(ptr(), tableConfig.ptr());
		} catch (Throwable t) {
			throw new RocksDBException("setTableFormatConfig failed", t);
		}
		return this;
	}

	// -----------------------------------------------------------------------
	// Blob file options
	// -----------------------------------------------------------------------

	/// Enables storing large values in separate blob files instead of inline in SSTs.
	/// When enabled, values ≥ [#setMinBlobSize] are written to blob files.
	/// Default: `false`.
	public Options setEnableBlobFiles(boolean value) {
		try {
			MH_SET_ENABLE_BLOB_FILES.invokeExact(ptr(), value ? (byte) 1 : (byte) 0);
			return this;
		} catch (Throwable t) {
			throw new RocksDBException("setEnableBlobFiles failed", t);
		}
	}

	public boolean getEnableBlobFiles() {
		try {
			return ((byte) MH_GET_ENABLE_BLOB_FILES.invokeExact(ptr())) != 0;
		} catch (Throwable t) {
			throw new RocksDBException("getEnableBlobFiles failed", t);
		}
	}

	/// Values strictly smaller than this size are stored inline; larger values go to blob files.
	/// Only effective when [#setEnableBlobFiles] is `true`. Default: 0 (all values externalized).
	public Options setMinBlobSize(MemorySize size) {
		try {
			MH_SET_MIN_BLOB_SIZE.invokeExact(ptr(), size.toBytes());
			return this;
		} catch (Throwable t) {
			throw new RocksDBException("setMinBlobSize failed", t);
		}
	}

	public MemorySize getMinBlobSize() {
		try {
			return MemorySize.ofBytes((long) MH_GET_MIN_BLOB_SIZE.invokeExact(ptr()));
		} catch (Throwable t) {
			throw new RocksDBException("getMinBlobSize failed", t);
		}
	}

	/// Target size for individual blob files. RocksDB rolls to a new file when this is exceeded.
	/// Default: 256 MiB.
	public Options setBlobFileSize(MemorySize size) {
		try {
			MH_SET_BLOB_FILE_SIZE.invokeExact(ptr(), size.toBytes());
			return this;
		} catch (Throwable t) {
			throw new RocksDBException("setBlobFileSize failed", t);
		}
	}

	public MemorySize getBlobFileSize() {
		try {
			return MemorySize.ofBytes((long) MH_GET_BLOB_FILE_SIZE.invokeExact(ptr()));
		} catch (Throwable t) {
			throw new RocksDBException("getBlobFileSize failed", t);
		}
	}

	/// Compression algorithm applied to blob file values. Independent of SST compression.
	/// Default: [CompressionType#NO_COMPRESSION].
	public Options setBlobCompressionType(CompressionType type) {
		try {
			MH_SET_BLOB_COMPRESSION_TYPE.invokeExact(ptr(), type.value);
			return this;
		} catch (Throwable t) {
			throw new RocksDBException("setBlobCompressionType failed", t);
		}
	}

	public CompressionType getBlobCompressionType() {
		try {
			return CompressionType.fromValue((int) MH_GET_BLOB_COMPRESSION_TYPE.invokeExact(ptr()));
		} catch (Throwable t) {
			throw new RocksDBException("getBlobCompressionType failed", t);
		}
	}

	/// Enables garbage collection of obsolete blob files during compaction.
	/// Default: `false`.
	public Options setEnableBlobGc(boolean value) {
		try {
			MH_SET_ENABLE_BLOB_GC.invokeExact(ptr(), value ? (byte) 1 : (byte) 0);
			return this;
		} catch (Throwable t) {
			throw new RocksDBException("setEnableBlobGc failed", t);
		}
	}

	public boolean getEnableBlobGc() {
		try {
			return ((byte) MH_GET_ENABLE_BLOB_GC.invokeExact(ptr())) != 0;
		} catch (Throwable t) {
			throw new RocksDBException("getEnableBlobGc failed", t);
		}
	}

	/// Blob files whose age is older than this fraction of the oldest snapshot are
	/// unconditionally GC'd, regardless of garbage ratio.
	/// Range: [0.0, 1.0]. Default: 0.5.
	public Options setBlobGcAgeCutoff(double value) {
		try {
			MH_SET_BLOB_GC_AGE_CUTOFF.invokeExact(ptr(), value);
			return this;
		} catch (Throwable t) {
			throw new RocksDBException("setBlobGcAgeCutoff failed", t);
		}
	}

	public double getBlobGcAgeCutoff() {
		try {
			return (double) MH_GET_BLOB_GC_AGE_CUTOFF.invokeExact(ptr());
		} catch (Throwable t) {
			throw new RocksDBException("getBlobGcAgeCutoff failed", t);
		}
	}

	/// Blob files whose garbage ratio exceeds this threshold are force-compacted.
	/// Range: [0.0, 1.0]. Default: 1.0 (disabled).
	public Options setBlobGcForceThreshold(double value) {
		try {
			MH_SET_BLOB_GC_FORCE_THRESHOLD.invokeExact(ptr(), value);
			return this;
		} catch (Throwable t) {
			throw new RocksDBException("setBlobGcForceThreshold failed", t);
		}
	}

	public double getBlobGcForceThreshold() {
		try {
			return (double) MH_GET_BLOB_GC_FORCE_THRESHOLD.invokeExact(ptr());
		} catch (Throwable t) {
			throw new RocksDBException("getBlobGcForceThreshold failed", t);
		}
	}

	/// Read-ahead size when reading blob files during compaction.
	/// `0` disables read-ahead. Default: 0.
	public Options setBlobCompactionReadaheadSize(MemorySize size) {
		try {
			MH_SET_BLOB_COMPACTION_READAHEAD_SIZE.invokeExact(ptr(), size.toBytes());
			return this;
		} catch (Throwable t) {
			throw new RocksDBException("setBlobCompactionReadaheadSize failed", t);
		}
	}

	public MemorySize getBlobCompactionReadaheadSize() {
		try {
			return MemorySize.ofBytes((long) MH_GET_BLOB_COMPACTION_READAHEAD_SIZE.invokeExact(ptr()));
		} catch (Throwable t) {
			throw new RocksDBException("getBlobCompactionReadaheadSize failed", t);
		}
	}

	/// LSM level at which blob file separation begins. Keys in levels below this
	/// threshold are stored inline. Default: 0 (all levels externalize blobs).
	public Options setBlobFileStartingLevel(int level) {
		try {
			MH_SET_BLOB_FILE_STARTING_LEVEL.invokeExact(ptr(), level);
			return this;
		} catch (Throwable t) {
			throw new RocksDBException("setBlobFileStartingLevel failed", t);
		}
	}

	public int getBlobFileStartingLevel() {
		try {
			return (int) MH_GET_BLOB_FILE_STARTING_LEVEL.invokeExact(ptr());
		} catch (Throwable t) {
			throw new RocksDBException("getBlobFileStartingLevel failed", t);
		}
	}

	/// Attaches a dedicated cache for blob values.
	/// Ownership of the cache is shared; the cache must outlive this Options object.
	public Options setBlobCache(Cache cache) {
		try {
			MH_SET_BLOB_CACHE.invokeExact(ptr(), cache.ptr());
			return this;
		} catch (Throwable t) {
			throw new RocksDBException("setBlobCache failed", t);
		}
	}

	/// Controls whether blob values are pre-populated into the blob cache on write.
	/// Default: [PrepopulateBlobCache#DISABLE].
	public Options setPrepopulateBlobCache(PrepopulateBlobCache mode) {
		try {
			MH_SET_PREPOPULATE_BLOB_CACHE.invokeExact(ptr(), mode.value);
			return this;
		} catch (Throwable t) {
			throw new RocksDBException("setPrepopulateBlobCache failed", t);
		}
	}

	public PrepopulateBlobCache getPrepopulateBlobCache() {
		try {
			return PrepopulateBlobCache.fromValue((int) MH_GET_PREPOPULATE_BLOB_CACHE.invokeExact(ptr()));
		} catch (Throwable t) {
			throw new RocksDBException("getPrepopulateBlobCache failed", t);
		}
	}

	// -----------------------------------------------------------------------
	// Logging options
	// -----------------------------------------------------------------------

	/// Sets the logger for this DB. RocksDB holds a shared reference; it is safe
	/// to close [Logger] after this call.
	public Options setInfoLog(Logger logger) {
		try {
			MH_SET_INFO_LOG.invokeExact(ptr(), logger.ptr());
			return this;
		} catch (Throwable t) {
			throw new RocksDBException("setInfoLog failed", t);
		}
	}

	/// Sets the minimum log level. Messages below this level are suppressed.
	public Options setInfoLogLevel(LogLevel level) {
		try {
			MH_SET_INFO_LOG_LEVEL.invokeExact(ptr(), level.value);
			return this;
		} catch (Throwable t) {
			throw new RocksDBException("setInfoLogLevel failed", t);
		}
	}

	public LogLevel getInfoLogLevel() {
		try {
			return LogLevel.fromValue((int) MH_GET_INFO_LOG_LEVEL.invokeExact(ptr()));
		} catch (Throwable t) {
			throw new RocksDBException("getInfoLogLevel failed", t);
		}
	}

	/// Sets the [Env] used for all file-system and threading operations.
	///
	/// The [Env] must remain open for the lifetime of the database.
	/// No ownership transfer: both objects may be closed independently.
	public Options setEnv(Env env) {
		try {
			MH_SET_ENV.invokeExact(ptr(), env.ptr());
			return this;
		} catch (Throwable t) {
			throw new RocksDBException("setEnv failed", t);
		}
	}

	/// Attaches an [SstFileManager] to track SST files and enforce disk-space limits.
	///
	/// No ownership transfer: both objects may be closed independently.
	public Options setSstFileManager(SstFileManager sfm) {
		try {
			MH_SET_SST_FILE_MANAGER.invokeExact(ptr(), sfm.ptr());
			return this;
		} catch (Throwable t) {
			throw new RocksDBException("setSstFileManager failed", t);
		}
	}

	/// Attaches a [RateLimiter] to throttle compaction and flush I/O.
	///
	/// The rate limiter uses shared ownership: this call does not transfer
	/// ownership — both objects may be closed independently.
	public Options setRateLimiter(RateLimiter rateLimiter) {
		try {
			MH_SET_RATELIMITER.invokeExact(ptr(), rateLimiter.ptr());
			return this;
		} catch (Throwable t) {
			throw new RocksDBException("setRateLimiter failed", t);
		}
	}

	@Override
	protected void tryClose(MemorySegment ptr) throws Throwable {
		MH_DESTROY.invokeExact(ptr);
	}
}
