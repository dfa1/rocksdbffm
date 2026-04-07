package io.github.dfa1.rocksdbffm;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/**
 * FFM wrapper for rocksdb_statistics_histogram_data_t.
 */
public final class StatisticsHistogramData extends NativeObject {

	// rocksdb_statistics_histogram_data_create(void) -> rocksdb_statistics_histogram_data_t*
	private static final MethodHandle MH_CREATE;
	// rocksdb_statistics_histogram_data_destroy(rocksdb_statistics_histogram_data_t* data) -> void
	private static final MethodHandle MH_DESTROY;
	// rocksdb_statistics_histogram_data_get_median(rocksdb_statistics_histogram_data_t* data) -> double
	private static final MethodHandle MH_GET_MEDIAN;
	// rocksdb_statistics_histogram_data_get_p95(rocksdb_statistics_histogram_data_t* data) -> double
	private static final MethodHandle MH_GET_P95;
	// rocksdb_statistics_histogram_data_get_p99(rocksdb_statistics_histogram_data_t* data) -> double
	private static final MethodHandle MH_GET_P99;
	// rocksdb_statistics_histogram_data_get_average(rocksdb_statistics_histogram_data_t* data) -> double
	private static final MethodHandle MH_GET_AVERAGE;
	// rocksdb_statistics_histogram_data_get_std_dev(rocksdb_statistics_histogram_data_t* data) -> double
	private static final MethodHandle MH_GET_STD_DEV;
	// rocksdb_statistics_histogram_data_get_max(rocksdb_statistics_histogram_data_t* data) -> double
	private static final MethodHandle MH_GET_MAX;
	// rocksdb_statistics_histogram_data_get_count(rocksdb_statistics_histogram_data_t* data) -> uint64_t
	private static final MethodHandle MH_GET_COUNT;
	// rocksdb_statistics_histogram_data_get_sum(rocksdb_statistics_histogram_data_t* data) -> uint64_t
	private static final MethodHandle MH_GET_SUM;
	// rocksdb_statistics_histogram_data_get_min(rocksdb_statistics_histogram_data_t* data) -> double
	private static final MethodHandle MH_GET_MIN;

	static {
		MH_CREATE = RocksDB.lookup("rocksdb_statistics_histogram_data_create",
				FunctionDescriptor.of(ValueLayout.ADDRESS));
		MH_DESTROY = RocksDB.lookup("rocksdb_statistics_histogram_data_destroy",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));
		MH_GET_MEDIAN = RocksDB.lookup("rocksdb_statistics_histogram_data_get_median",
				FunctionDescriptor.of(ValueLayout.JAVA_DOUBLE, ValueLayout.ADDRESS));
		MH_GET_P95 = RocksDB.lookup("rocksdb_statistics_histogram_data_get_p95",
				FunctionDescriptor.of(ValueLayout.JAVA_DOUBLE, ValueLayout.ADDRESS));
		MH_GET_P99 = RocksDB.lookup("rocksdb_statistics_histogram_data_get_p99",
				FunctionDescriptor.of(ValueLayout.JAVA_DOUBLE, ValueLayout.ADDRESS));
		MH_GET_AVERAGE = RocksDB.lookup("rocksdb_statistics_histogram_data_get_average",
				FunctionDescriptor.of(ValueLayout.JAVA_DOUBLE, ValueLayout.ADDRESS));
		MH_GET_STD_DEV = RocksDB.lookup("rocksdb_statistics_histogram_data_get_std_dev",
				FunctionDescriptor.of(ValueLayout.JAVA_DOUBLE, ValueLayout.ADDRESS));
		MH_GET_MAX = RocksDB.lookup("rocksdb_statistics_histogram_data_get_max",
				FunctionDescriptor.of(ValueLayout.JAVA_DOUBLE, ValueLayout.ADDRESS));
		MH_GET_COUNT = RocksDB.lookup("rocksdb_statistics_histogram_data_get_count",
				FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));
		MH_GET_SUM = RocksDB.lookup("rocksdb_statistics_histogram_data_get_sum",
				FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));
		MH_GET_MIN = RocksDB.lookup("rocksdb_statistics_histogram_data_get_min",
				FunctionDescriptor.of(ValueLayout.JAVA_DOUBLE, ValueLayout.ADDRESS));
	}

	private StatisticsHistogramData(MemorySegment ptr) {
		super(ptr);
	}

	public static StatisticsHistogramData newStatisticsHistogramData() {
		try {
			return new StatisticsHistogramData((MemorySegment) MH_CREATE.invokeExact());
		} catch (Throwable t) {
			throw new RocksDBException("histogram data create failed", t);
		}
	}

	public double getMedian() {
		try {
			return (double) MH_GET_MEDIAN.invokeExact(ptr());
		} catch (Throwable t) {
			throw new RocksDBException("getMedian failed", t);
		}
	}

	public double getP95() {
		try {
			return (double) MH_GET_P95.invokeExact(ptr());
		} catch (Throwable t) {
			throw new RocksDBException("getP95 failed", t);
		}
	}

	public double getP99() {
		try {
			return (double) MH_GET_P99.invokeExact(ptr());
		} catch (Throwable t) {
			throw new RocksDBException("getP99 failed", t);
		}
	}

	public double getAverage() {
		try {
			return (double) MH_GET_AVERAGE.invokeExact(ptr());
		} catch (Throwable t) {
			throw new RocksDBException("getAverage failed", t);
		}
	}

	public double getStdDev() {
		try {
			return (double) MH_GET_STD_DEV.invokeExact(ptr());
		} catch (Throwable t) {
			throw new RocksDBException("getStdDev failed", t);
		}
	}

	public double getMax() {
		try {
			return (double) MH_GET_MAX.invokeExact(ptr());
		} catch (Throwable t) {
			throw new RocksDBException("getMax failed", t);
		}
	}

	public long getCount() {
		try {
			return (long) MH_GET_COUNT.invokeExact(ptr());
		} catch (Throwable t) {
			throw new RocksDBException("getCount failed", t);
		}
	}

	public long getSum() {
		try {
			return (long) MH_GET_SUM.invokeExact(ptr());
		} catch (Throwable t) {
			throw new RocksDBException("getSum failed", t);
		}
	}

	public double getMin() {
		try {
			return (double) MH_GET_MIN.invokeExact(ptr());
		} catch (Throwable t) {
			throw new RocksDBException("getMin failed", t);
		}
	}

	@Override
	protected void tryClose(MemorySegment ptr) throws Throwable {
		MH_DESTROY.invokeExact(ptr);
	}

	@Override
	public String toString() {
		return String.format(
				"Histogram[count=%d, sum=%d, min=%.2f, max=%.2f, avg=%.2f, median=%.2f, p95=%.2f, p99=%.2f, stddev=%.2f]",
				getCount(), getSum(), getMin(), getMax(), getAverage(), getMedian(), getP95(), getP99(), getStdDev()
		);
	}
}
