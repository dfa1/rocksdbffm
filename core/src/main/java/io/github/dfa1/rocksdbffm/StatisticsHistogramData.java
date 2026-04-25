package io.github.dfa1.rocksdbffm;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/// FFM wrapper for `rocksdb_statistics_histogram_data_t`.
public final class StatisticsHistogramData extends NativeObject {

	/// `rocksdb_statistics_histogram_data_t* rocksdb_statistics_histogram_data_create(void);`
	private static final MethodHandle MH_CREATE;
	/// `void rocksdb_statistics_histogram_data_destroy(rocksdb_statistics_histogram_data_t* data);`
	private static final MethodHandle MH_DESTROY;
	/// `double rocksdb_statistics_histogram_data_get_median(rocksdb_statistics_histogram_data_t* data);`
	private static final MethodHandle MH_GET_MEDIAN;
	/// `double rocksdb_statistics_histogram_data_get_p95(rocksdb_statistics_histogram_data_t* data);`
	private static final MethodHandle MH_GET_P95;
	/// `double rocksdb_statistics_histogram_data_get_p99(rocksdb_statistics_histogram_data_t* data);`
	private static final MethodHandle MH_GET_P99;
	/// `double rocksdb_statistics_histogram_data_get_average(rocksdb_statistics_histogram_data_t* data);`
	private static final MethodHandle MH_GET_AVERAGE;
	/// `double rocksdb_statistics_histogram_data_get_std_dev(rocksdb_statistics_histogram_data_t* data);`
	private static final MethodHandle MH_GET_STD_DEV;
	/// `double rocksdb_statistics_histogram_data_get_max(rocksdb_statistics_histogram_data_t* data);`
	private static final MethodHandle MH_GET_MAX;
	/// `uint64_t rocksdb_statistics_histogram_data_get_count(rocksdb_statistics_histogram_data_t* data);`
	private static final MethodHandle MH_GET_COUNT;
	/// `uint64_t rocksdb_statistics_histogram_data_get_sum(rocksdb_statistics_histogram_data_t* data);`
	private static final MethodHandle MH_GET_SUM;
	/// `double rocksdb_statistics_histogram_data_get_min(rocksdb_statistics_histogram_data_t* data);`
	private static final MethodHandle MH_GET_MIN;

	static {
		MH_CREATE = NativeLibrary.lookup("rocksdb_statistics_histogram_data_create",
				FunctionDescriptor.of(ValueLayout.ADDRESS));
		MH_DESTROY = NativeLibrary.lookup("rocksdb_statistics_histogram_data_destroy",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));
		MH_GET_MEDIAN = NativeLibrary.lookup("rocksdb_statistics_histogram_data_get_median",
				FunctionDescriptor.of(ValueLayout.JAVA_DOUBLE, ValueLayout.ADDRESS));
		MH_GET_P95 = NativeLibrary.lookup("rocksdb_statistics_histogram_data_get_p95",
				FunctionDescriptor.of(ValueLayout.JAVA_DOUBLE, ValueLayout.ADDRESS));
		MH_GET_P99 = NativeLibrary.lookup("rocksdb_statistics_histogram_data_get_p99",
				FunctionDescriptor.of(ValueLayout.JAVA_DOUBLE, ValueLayout.ADDRESS));
		MH_GET_AVERAGE = NativeLibrary.lookup("rocksdb_statistics_histogram_data_get_average",
				FunctionDescriptor.of(ValueLayout.JAVA_DOUBLE, ValueLayout.ADDRESS));
		MH_GET_STD_DEV = NativeLibrary.lookup("rocksdb_statistics_histogram_data_get_std_dev",
				FunctionDescriptor.of(ValueLayout.JAVA_DOUBLE, ValueLayout.ADDRESS));
		MH_GET_MAX = NativeLibrary.lookup("rocksdb_statistics_histogram_data_get_max",
				FunctionDescriptor.of(ValueLayout.JAVA_DOUBLE, ValueLayout.ADDRESS));
		MH_GET_COUNT = NativeLibrary.lookup("rocksdb_statistics_histogram_data_get_count",
				FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));
		MH_GET_SUM = NativeLibrary.lookup("rocksdb_statistics_histogram_data_get_sum",
				FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));
		MH_GET_MIN = NativeLibrary.lookup("rocksdb_statistics_histogram_data_get_min",
				FunctionDescriptor.of(ValueLayout.JAVA_DOUBLE, ValueLayout.ADDRESS));
	}

	private StatisticsHistogramData(MemorySegment ptr) {
		super(ptr);
	}

	/// Creates a new histogram data container, initially zeroed.
	///
	/// @return a new [StatisticsHistogramData]; caller must close it
	public static StatisticsHistogramData newStatisticsHistogramData() {
		try {
			return new StatisticsHistogramData((MemorySegment) MH_CREATE.invokeExact());
		} catch (Throwable t) {
			throw new RocksDBException("histogram data create failed", t);
		}
	}

	/// Returns the median value of the histogram.
	///
	/// @return median value
	public double getMedian() {
		try {
			return (double) MH_GET_MEDIAN.invokeExact(ptr());
		} catch (Throwable t) {
			throw new RocksDBException("getMedian failed", t);
		}
	}

	/// Returns the 95th-percentile value of the histogram.
	///
	/// @return 95th-percentile value
	public double getP95() {
		try {
			return (double) MH_GET_P95.invokeExact(ptr());
		} catch (Throwable t) {
			throw new RocksDBException("getP95 failed", t);
		}
	}

	/// Returns the 99th-percentile value of the histogram.
	///
	/// @return 99th-percentile value
	public double getP99() {
		try {
			return (double) MH_GET_P99.invokeExact(ptr());
		} catch (Throwable t) {
			throw new RocksDBException("getP99 failed", t);
		}
	}

	/// Returns the average value of the histogram.
	///
	/// @return average value
	public double getAverage() {
		try {
			return (double) MH_GET_AVERAGE.invokeExact(ptr());
		} catch (Throwable t) {
			throw new RocksDBException("getAverage failed", t);
		}
	}

	/// Returns the standard deviation of the histogram.
	///
	/// @return standard deviation
	public double getStdDev() {
		try {
			return (double) MH_GET_STD_DEV.invokeExact(ptr());
		} catch (Throwable t) {
			throw new RocksDBException("getStdDev failed", t);
		}
	}

	/// Returns the maximum value recorded in the histogram.
	///
	/// @return maximum value
	public double getMax() {
		try {
			return (double) MH_GET_MAX.invokeExact(ptr());
		} catch (Throwable t) {
			throw new RocksDBException("getMax failed", t);
		}
	}

	/// Returns the total number of samples in the histogram.
	///
	/// @return sample count
	public long getCount() {
		try {
			return (long) MH_GET_COUNT.invokeExact(ptr());
		} catch (Throwable t) {
			throw new RocksDBException("getCount failed", t);
		}
	}

	/// Returns the sum of all samples in the histogram.
	///
	/// @return sum of all samples
	public long getSum() {
		try {
			return (long) MH_GET_SUM.invokeExact(ptr());
		} catch (Throwable t) {
			throw new RocksDBException("getSum failed", t);
		}
	}

	/// Returns the minimum value recorded in the histogram.
	///
	/// @return minimum value
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
