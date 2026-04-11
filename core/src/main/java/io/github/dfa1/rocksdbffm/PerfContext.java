package io.github.dfa1.rocksdbffm;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/// FFM wrapper for `rocksdb_perfcontext_t`.
///
/// Wraps the **thread-local** RocksDB perf context, which accumulates
/// fine-grained counters and timings for every operation on the calling thread.
///
/// ## Typical usage
///
/// ```
/// PerfContext.setPerfLevel(PerfLevel.ENABLE_COUNT);
///
/// try (PerfContext ctx = PerfContext.newPerfContext()) {
///     db.get("key".getBytes());
///
///     long blockCacheHits = ctx.metric(PerfMetric.BLOCK_CACHE_HIT_COUNT);
///     long blockReads     = ctx.metric(PerfMetric.BLOCK_READ_COUNT);
///     System.out.println(ctx.report(true));
/// }
///
/// PerfContext.setPerfLevel(PerfLevel.DISABLE);
/// ```
///
/// ## Thread-locality
///
/// The perf context is thread-local: [#create()] wraps the context of the
/// calling thread. Metrics accumulated on one thread are invisible to others.
/// Always create, use, and close [PerfContext] on the same thread.
///
/// ## Overhead
///
/// Leave the level at [PerfLevel#DISABLE] (the default) in production.
/// Enable it only around the code under measurement.
public final class PerfContext extends NativeObject {

	/// `void rocksdb_set_perf_level(int);`
	private static final MethodHandle MH_SET_PERF_LEVEL;
	/// `rocksdb_perfcontext_t* rocksdb_perfcontext_create(void);`
	private static final MethodHandle MH_CREATE;
	/// `void rocksdb_perfcontext_reset(rocksdb_perfcontext_t* context);`
	private static final MethodHandle MH_RESET;
	/// `char* rocksdb_perfcontext_report(rocksdb_perfcontext_t* context, unsigned char exclude_zero_counters);`
	private static final MethodHandle MH_REPORT;
	/// `uint64_t rocksdb_perfcontext_metric(rocksdb_perfcontext_t* context, int metric);`
	private static final MethodHandle MH_METRIC;
	/// `void rocksdb_perfcontext_destroy(rocksdb_perfcontext_t* context);`
	private static final MethodHandle MH_DESTROY;

	static {
		MH_SET_PERF_LEVEL = NativeLibrary.lookup("rocksdb_set_perf_level",
				FunctionDescriptor.ofVoid(ValueLayout.JAVA_INT));

		MH_CREATE = NativeLibrary.lookup("rocksdb_perfcontext_create",
				FunctionDescriptor.of(ValueLayout.ADDRESS));

		MH_RESET = NativeLibrary.lookup("rocksdb_perfcontext_reset",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

		MH_REPORT = NativeLibrary.lookup("rocksdb_perfcontext_report",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS,   // context
						ValueLayout.JAVA_BYTE)); // exclude_zero_counters

		MH_METRIC = NativeLibrary.lookup("rocksdb_perfcontext_metric",
				FunctionDescriptor.of(ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS,  // context
						ValueLayout.JAVA_INT)); // metric

		MH_DESTROY = NativeLibrary.lookup("rocksdb_perfcontext_destroy",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));
	}

	private PerfContext(MemorySegment ptr) {
		super(ptr);
	}

	// -----------------------------------------------------------------------
	// Global level control
	// -----------------------------------------------------------------------

	/// Sets the instrumentation level for the calling thread.
	///
	/// Must be called before creating or using a [PerfContext] to have effect.
	/// Always reset to [PerfLevel#DISABLE] when measurement is complete.
	public static void setPerfLevel(PerfLevel level) {
		try {
			MH_SET_PERF_LEVEL.invokeExact(level.value);
		} catch (Throwable t) {
			throw RocksDBException.wrap("setPerfLevel failed", t);
		}
	}

	// -----------------------------------------------------------------------
	// Factory
	// -----------------------------------------------------------------------

	/// Wraps the calling thread's perf context and resets all counters to zero.
	///
	/// Use this as the standard entry point when you want a clean measurement window.
	public static PerfContext newPerfContext() {
		PerfContext ctx = currentPerfContext();
		ctx.reset();
		return ctx;
	}

	/// Wraps the calling thread's perf context without resetting counters.
	///
	/// Use this when you want to observe metrics that have already accumulated,
	/// or when you manage [#reset()] yourself.
	public static PerfContext currentPerfContext() {
		try {
			return new PerfContext((MemorySegment) MH_CREATE.invokeExact());
		} catch (Throwable t) {
			throw RocksDBException.wrap("PerfContext.currentPerfContext failed", t);
		}
	}

	// -----------------------------------------------------------------------
	// Operations
	// -----------------------------------------------------------------------

	/// Resets all counters and timers to zero on the calling thread.
	public void reset() {
		try {
			MH_RESET.invokeExact(ptr());
		} catch (Throwable t) {
			throw RocksDBException.wrap("reset failed", t);
		}
	}

	/// Returns the accumulated value for the given metric since the last [#reset()].
	public long metric(PerfMetric metric) {
		try {
			return (long) MH_METRIC.invokeExact(ptr(), metric.value);
		} catch (Throwable t) {
			throw RocksDBException.wrap("metric failed", t);
		}
	}

	/// Returns a human-readable report of all metrics.
	///
	/// @param excludeZeroCounters if `true`, metrics with a zero value are omitted
	public String report(boolean excludeZeroCounters) {
		MemorySegment strPtr;
		try {
			strPtr = (MemorySegment) MH_REPORT.invokeExact(ptr(),
					excludeZeroCounters ? (byte) 1 : (byte) 0);
		} catch (Throwable t) {
			throw RocksDBException.wrap("report failed", t);
		}
		if (MemorySegment.NULL.equals(strPtr)) {
			return "";
		}
		String result = strPtr.reinterpret(Long.MAX_VALUE).getString(0);
		Native.free(strPtr);
		return result;
	}

	@Override
	protected void tryClose(MemorySegment ptr) throws Throwable {
		MH_DESTROY.invokeExact(ptr);
	}
}
