package io.github.dfa1.rocksdbffm;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/**
 * FFM wrapper for rocksdb_options_t.
 *
 * <p>Usage:
 * <pre>{@code
 * try (Options opts = Options.newOptions().setCreateIfMissing(true)) {
 *     RocksDB db = RocksDB.open(opts, path);
 * }
 * }</pre>
 *
 * <p>Note: the Options object must remain open until after RocksDB.open() returns;
 * it can be closed immediately after that call.
 */
public final class Options extends NativeObject {

	private static final MethodHandle MH_CREATE;
	private static final MethodHandle MH_DESTROY;
	private static final MethodHandle MH_SET_CREATE_IF_MISSING;
	private static final MethodHandle MH_GET_CREATE_IF_MISSING;
	private static final MethodHandle MH_SET_BLOCK_BASED_TABLE_FACTORY;
	private static final MethodHandle MH_ENABLE_STATISTICS;
	private static final MethodHandle MH_SET_STATISTICS_LEVEL;
	private static final MethodHandle MH_GET_STATISTICS_LEVEL;
	private static final MethodHandle MH_STATISTICS_GET_STRING;
	private static final MethodHandle MH_STATISTICS_GET_TICKER_COUNT;
	private static final MethodHandle MH_STATISTICS_GET_HISTOGRAM_DATA;
	private static final MethodHandle MH_SET_COMPRESSION;
	private static final MethodHandle MH_GET_COMPRESSION;

	static {
		MH_CREATE = RocksDB.lookup("rocksdb_options_create",
				FunctionDescriptor.of(ValueLayout.ADDRESS));

		MH_DESTROY = RocksDB.lookup("rocksdb_options_destroy",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

		MH_SET_CREATE_IF_MISSING = RocksDB.lookup("rocksdb_options_set_create_if_missing",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_BYTE));

		MH_GET_CREATE_IF_MISSING = RocksDB.lookup("rocksdb_options_get_create_if_missing",
				FunctionDescriptor.of(ValueLayout.JAVA_BYTE, ValueLayout.ADDRESS));

		// void rocksdb_options_set_block_based_table_factory(opts*, block_based_table_options_t*)
		MH_SET_BLOCK_BASED_TABLE_FACTORY = RocksDB.lookup(
				"rocksdb_options_set_block_based_table_factory",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_ENABLE_STATISTICS = RocksDB.lookup("rocksdb_options_enable_statistics",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

		MH_SET_STATISTICS_LEVEL = RocksDB.lookup("rocksdb_options_set_statistics_level",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_INT));

		MH_GET_STATISTICS_LEVEL = RocksDB.lookup("rocksdb_options_get_statistics_level",
				FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));

		MH_STATISTICS_GET_STRING = RocksDB.lookup("rocksdb_options_statistics_get_string",
				FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_STATISTICS_GET_TICKER_COUNT = RocksDB.lookup("rocksdb_options_statistics_get_ticker_count",
				FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.JAVA_INT));

		MH_STATISTICS_GET_HISTOGRAM_DATA = RocksDB.lookup("rocksdb_options_statistics_get_histogram_data",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_INT, ValueLayout.ADDRESS));

		// void rocksdb_options_set_compression(opts*, int)
		MH_SET_COMPRESSION = RocksDB.lookup("rocksdb_options_set_compression",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_INT));

		// int rocksdb_options_get_compression(opts*)
		MH_GET_COMPRESSION = RocksDB.lookup("rocksdb_options_get_compression",
				FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));

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

	/**
	 * If true, the database will be created if it does not already exist.
	 * Default: false (same as RocksDB C++ default).
	 */
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

	/**
	 * Enables statistics gathering for this DB.
	 */
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
			Native.free(strPtr);
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


	/**
	 * Sets the compression algorithm for all levels.
	 * Use {@link RocksDB#getSupportedCompressions()} to check which types are available.
	 *
	 * @return {@code this} for chaining
	 */
	public Options setCompression(CompressionType type) {
		try {
			MH_SET_COMPRESSION.invokeExact(ptr(), type.value);
			return this;
		} catch (Throwable t) {
			throw new RocksDBException("setCompression failed", t);
		}
	}

	/**
	 * Returns the compression algorithm configured for this Options.
	 */
	public CompressionType getCompression() {
		try {
			return CompressionType.fromValue((int) MH_GET_COMPRESSION.invokeExact(ptr()));
		} catch (Throwable t) {
			throw new RocksDBException("getCompression failed", t);
		}
	}

	/**
	 * Configures block-based table format for this DB.
	 * RocksDB copies the config internally; {@code tableConfig} may be closed after this call.
	 */
	public Options setTableFormatConfig(BlockBasedTableOptions tableConfig) {
		try {
			MH_SET_BLOCK_BASED_TABLE_FACTORY.invokeExact(ptr(), tableConfig.ptr());
		} catch (Throwable t) {
			throw new RocksDBException("setTableFormatConfig failed", t);
		}
		return this;
	}

	@Override
	protected void tryClose(MemorySegment ptr) throws Throwable {
		MH_DESTROY.invokeExact(ptr);
	}
}
