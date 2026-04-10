package io.github.dfa1.rocksdbffm;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/// HyperClock block cache (`rocksdb_cache_t`).
///
/// A lock-free cache with better multi-threaded scalability than LRU. Requires an
/// `estimatedEntryCharge` hint: the expected average size of a cached block. Use
/// [MemorySize#ZERO] to let RocksDB pick a default.
///
/// ```
/// try (HyperClockCache cache = HyperClockCache.newHyperClockCache(
///         MemorySize.ofMB(256), MemorySize.ofKB(8))) {
///     BlockBasedTableOptions tbl = BlockBasedTableOptions.newBlockBasedTableOptions()
///         .setBlockCache(cache);
///     ...
/// }
/// ```
// TODO: not tested
public final class HyperClockCache extends Cache {

	/// `rocksdb_hyper_clock_cache_options_t* rocksdb_hyper_clock_cache_options_create(size_t capacity, size_t estimated_entry_charge);`
	private static final MethodHandle MH_OPTS_CREATE;
	/// `void rocksdb_hyper_clock_cache_options_destroy(rocksdb_hyper_clock_cache_options_t*);`
	private static final MethodHandle MH_OPTS_DESTROY;
	/// `void rocksdb_hyper_clock_cache_options_set_num_shard_bits(rocksdb_hyper_clock_cache_options_t*, int);`
	private static final MethodHandle MH_OPTS_SET_NUM_SHARD_BITS;
	/// `rocksdb_cache_t* rocksdb_cache_create_hyper_clock(size_t capacity, size_t estimated_entry_charge);`
	private static final MethodHandle MH_CREATE;
	/// `rocksdb_cache_t* rocksdb_cache_create_hyper_clock_opts(const rocksdb_hyper_clock_cache_options_t*);`
	private static final MethodHandle MH_CREATE_OPTS;

	static {
		MH_OPTS_CREATE = NativeLibrary.lookup("rocksdb_hyper_clock_cache_options_create",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.JAVA_LONG,
						ValueLayout.JAVA_LONG));

		MH_OPTS_DESTROY = NativeLibrary.lookup("rocksdb_hyper_clock_cache_options_destroy",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

		MH_OPTS_SET_NUM_SHARD_BITS = NativeLibrary.lookup(
				"rocksdb_hyper_clock_cache_options_set_num_shard_bits",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_INT));

		MH_CREATE = NativeLibrary.lookup("rocksdb_cache_create_hyper_clock",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.JAVA_LONG,
						ValueLayout.JAVA_LONG));

		MH_CREATE_OPTS = NativeLibrary.lookup("rocksdb_cache_create_hyper_clock_opts",
				FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS));
	}

	private HyperClockCache(MemorySegment ptr) {
		super(ptr);
	}

	/// Creates a HyperClockCache.
	///
	/// @param capacity              total cache capacity
	/// @param estimatedEntryCharge  expected average size of a cached block; use
	///                              [MemorySize#ZERO] to let RocksDB choose
	public static HyperClockCache newHyperClockCache(
			MemorySize capacity, MemorySize estimatedEntryCharge) {
		try {
			return new HyperClockCache((MemorySegment) MH_CREATE.invokeExact(
					capacity.toBytes(), estimatedEntryCharge.toBytes()));
		} catch (Throwable t) {
			throw new RocksDBException("HyperClockCache create failed", t);
		}
	}

	/// Creates a HyperClockCache with an explicit shard count.
	///
	/// @param capacity              total cache capacity
	/// @param estimatedEntryCharge  expected average size of a cached block; use
	///                              [MemorySize#ZERO] to let RocksDB choose
	/// @param numShardBits          number of shard bits (`shards = 1 << numShardBits`);
	///                              pass `-1` to let RocksDB choose automatically
	public static HyperClockCache newHyperClockCache(
			MemorySize capacity, MemorySize estimatedEntryCharge, int numShardBits) {
		MemorySegment opts = null;
		try {
			opts = (MemorySegment) MH_OPTS_CREATE.invokeExact(
					capacity.toBytes(), estimatedEntryCharge.toBytes());
			MH_OPTS_SET_NUM_SHARD_BITS.invokeExact(opts, numShardBits);
			return new HyperClockCache((MemorySegment) MH_CREATE_OPTS.invokeExact(opts));
		} catch (Throwable t) {
			throw new RocksDBException("HyperClockCache create failed", t);
		} finally {
			if (opts != null) {
				try {
					MH_OPTS_DESTROY.invokeExact(opts);
				} catch (Throwable ignored) {
				}
			}
		}
	}
}
