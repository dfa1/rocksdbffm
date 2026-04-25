package io.github.dfa1.rocksdbffm;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/// FFM wrapper for `rocksdb_ratelimiter_t`.
///
/// Controls the rate of I/O issued by RocksDB (compaction, flush, etc.).
///
/// ```
/// try (var limiter = RateLimiter.create(MemorySize.ofMB(100));
///      var opts = Options.newOptions()
///          .setCreateIfMissing(true)
///          .setRateLimiter(limiter)) {
///     var db = RocksDB.open(opts, path);
/// }
/// ```
///
/// The rate limiter uses shared ownership: passing it to [Options#setRateLimiter]
/// does not transfer ownership — both objects may be closed independently.
public final class RateLimiter extends NativeObject {

	/// Controls which I/O types are rate-limited.
	public enum Mode {
		/// Rate-limit reads only.
		READS_ONLY(0),
		/// Rate-limit writes only (default).
		WRITES_ONLY(1),
		/// Rate-limit all I/O.
		ALL_IO(2);

		final int value;

		Mode(int v) {
			this.value = v;
		}
	}

	/// `rocksdb_ratelimiter_t* rocksdb_ratelimiter_create(int64_t rate_bytes_per_sec, int64_t refill_period_us, int32_t fairness);`
	private static final MethodHandle MH_CREATE;
	/// `rocksdb_ratelimiter_t* rocksdb_ratelimiter_create_auto_tuned(int64_t rate_bytes_per_sec, int64_t refill_period_us, int32_t fairness);`
	private static final MethodHandle MH_CREATE_AUTO_TUNED;
	/// `rocksdb_ratelimiter_t* rocksdb_ratelimiter_create_with_mode(int64_t rate_bytes_per_sec, int64_t refill_period_us, int32_t fairness, int mode, bool auto_tuned);`
	private static final MethodHandle MH_CREATE_WITH_MODE;
	/// `void rocksdb_ratelimiter_destroy(rocksdb_ratelimiter_t*);`
	private static final MethodHandle MH_DESTROY;

	static {
		MH_CREATE = NativeLibrary.lookup("rocksdb_ratelimiter_create",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.JAVA_LONG,
						ValueLayout.JAVA_LONG,
						ValueLayout.JAVA_INT));

		MH_CREATE_AUTO_TUNED = NativeLibrary.lookup("rocksdb_ratelimiter_create_auto_tuned",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.JAVA_LONG,
						ValueLayout.JAVA_LONG,
						ValueLayout.JAVA_INT));

		MH_CREATE_WITH_MODE = NativeLibrary.lookup("rocksdb_ratelimiter_create_with_mode",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.JAVA_LONG,
						ValueLayout.JAVA_LONG,
						ValueLayout.JAVA_INT,
						ValueLayout.JAVA_INT,
						ValueLayout.JAVA_BYTE));

		MH_DESTROY = NativeLibrary.lookup("rocksdb_ratelimiter_destroy",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));
	}

	private RateLimiter(MemorySegment ptr) {
		super(ptr);
	}

	/// Creates a rate limiter with default refill period (100 ms) and fairness (10),
	/// limiting writes only.
	///
	/// @param rateBytesPerSec maximum I/O rate in bytes per second
	/// @return a new [RateLimiter]; caller must close it
	public static RateLimiter create(MemorySize rateBytesPerSec) {
		return create(rateBytesPerSec, 100_000L, 10);
	}

	/// Creates a rate limiter limiting writes only.
	///
	/// @param rateBytesPerSec  maximum I/O rate in bytes per second
	/// @param refillPeriodUs   refill interval in microseconds (default 100,000)
	/// @param fairness         RateLimiter will allow at most 1 / fairness high-priority
	///                         requests to wait when there are low-priority requests in flight (default 10)
	/// @return a new [RateLimiter]; caller must close it
	public static RateLimiter create(MemorySize rateBytesPerSec, long refillPeriodUs, int fairness) {
		try {
			MemorySegment ptr = (MemorySegment) MH_CREATE.invokeExact(
					rateBytesPerSec.toBytes(), refillPeriodUs, fairness);
			return new RateLimiter(ptr);
		} catch (Throwable t) {
			throw new RocksDBException("RateLimiter create failed", t);
		}
	}

	/// Creates an auto-tuned rate limiter that automatically adjusts the rate to
	/// achieve the target rate. Limits writes only.
	///
	/// @param rateBytesPerSec  target I/O rate in bytes per second
	/// @param refillPeriodUs   refill interval in microseconds (default 100,000)
	/// @param fairness         see [#create(MemorySize, long, int)] (default 10)
	/// @return a new auto-tuned [RateLimiter]; caller must close it
	public static RateLimiter createAutoTuned(MemorySize rateBytesPerSec, long refillPeriodUs, int fairness) {
		try {
			MemorySegment ptr = (MemorySegment) MH_CREATE_AUTO_TUNED.invokeExact(
					rateBytesPerSec.toBytes(), refillPeriodUs, fairness);
			return new RateLimiter(ptr);
		} catch (Throwable t) {
			throw new RocksDBException("RateLimiter createAutoTuned failed", t);
		}
	}

	/// Creates an auto-tuned rate limiter with default refill period (100 ms) and
	/// fairness (10), limiting writes only.
	///
	/// @param rateBytesPerSec target I/O rate in bytes per second
	/// @return a new auto-tuned [RateLimiter]; caller must close it
	public static RateLimiter createAutoTuned(MemorySize rateBytesPerSec) {
		return createAutoTuned(rateBytesPerSec, 100_000L, 10);
	}

	/// Creates a rate limiter with explicit mode and auto-tuning flag.
	///
	/// @param rateBytesPerSec  maximum I/O rate in bytes per second
	/// @param refillPeriodUs   refill interval in microseconds
	/// @param fairness         see [#create(MemorySize, long, int)]
	/// @param mode             which I/O types to rate-limit
	/// @param autoTuned        whether to enable automatic rate adjustment
	/// @return a new [RateLimiter]; caller must close it
	public static RateLimiter createWithMode(MemorySize rateBytesPerSec, long refillPeriodUs,
			int fairness, Mode mode, boolean autoTuned) {
		try {
			MemorySegment ptr = (MemorySegment) MH_CREATE_WITH_MODE.invokeExact(
					rateBytesPerSec.toBytes(), refillPeriodUs, fairness,
					mode.value, autoTuned ? (byte) 1 : (byte) 0);
			return new RateLimiter(ptr);
		} catch (Throwable t) {
			throw new RocksDBException("RateLimiter createWithMode failed", t);
		}
	}

	@Override
	protected void tryClose(MemorySegment ptr) throws Throwable {
		MH_DESTROY.invokeExact(ptr);
	}
}
