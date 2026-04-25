package io.github.dfa1.rocksdbffm;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/// Base class for RocksDB block cache implementations (LRU, HyperClock).
///
/// Wraps a `rocksdb_cache_t*`. Pass to
/// [BlockBasedTableOptions#setBlockCache(Cache)] to share a single cache
/// across multiple column families or DB instances.
public abstract class Cache extends NativeObject {

	/// `void rocksdb_cache_destroy(rocksdb_cache_t* cache);`
	private static final MethodHandle MH_DESTROY;
	/// `void rocksdb_cache_set_capacity(rocksdb_cache_t* cache, size_t capacity);`
	private static final MethodHandle MH_SET_CAPACITY;
	/// `size_t rocksdb_cache_get_capacity(const rocksdb_cache_t* cache);`
	private static final MethodHandle MH_GET_CAPACITY;
	/// `size_t rocksdb_cache_get_usage(const rocksdb_cache_t* cache);`
	private static final MethodHandle MH_GET_USAGE;
	/// `size_t rocksdb_cache_get_pinned_usage(const rocksdb_cache_t* cache);`
	private static final MethodHandle MH_GET_PINNED_USAGE;

	static {
		MH_DESTROY = NativeLibrary.lookup("rocksdb_cache_destroy",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

		MH_SET_CAPACITY = NativeLibrary.lookup("rocksdb_cache_set_capacity",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));

		MH_GET_CAPACITY = NativeLibrary.lookup("rocksdb_cache_get_capacity",
				FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));

		MH_GET_USAGE = NativeLibrary.lookup("rocksdb_cache_get_usage",
				FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));

		MH_GET_PINNED_USAGE = NativeLibrary.lookup("rocksdb_cache_get_pinned_usage",
				FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));
	}

	/// Constructs a cache wrapping the given native pointer.
	///
	/// @param ptr native `rocksdb_cache_t*`
	protected Cache(MemorySegment ptr) {
		super(ptr);
	}

	/// Dynamically resizes the cache. Excess entries are evicted as needed.
	///
	/// @param capacity new cache capacity
	public void setCapacity(MemorySize capacity) {
		try {
			MH_SET_CAPACITY.invokeExact(ptr(), capacity.toBytes());
		} catch (Throwable t) {
			throw new RocksDBException("setCapacity failed", t);
		}
	}

	/// Returns the configured capacity of the cache.
	///
	/// @return cache capacity
	public MemorySize getCapacity() {
		try {
			return MemorySize.ofBytes((long) MH_GET_CAPACITY.invokeExact(ptr()));
		} catch (Throwable t) {
			throw new RocksDBException("getCapacity failed", t);
		}
	}

	/// Returns the current memory usage of the cache.
	///
	/// @return current usage
	public MemorySize getUsage() {
		try {
			return MemorySize.ofBytes((long) MH_GET_USAGE.invokeExact(ptr()));
		} catch (Throwable t) {
			throw new RocksDBException("getUsage failed", t);
		}
	}

	/// Returns the amount of memory currently pinned (not eligible for eviction).
	///
	/// @return pinned memory usage
	public MemorySize getPinnedUsage() {
		try {
			return MemorySize.ofBytes((long) MH_GET_PINNED_USAGE.invokeExact(ptr()));
		} catch (Throwable t) {
			throw new RocksDBException("getPinnedUsage failed", t);
		}
	}

	@Override
	protected void tryClose(MemorySegment ptr) throws Throwable {
		MH_DESTROY.invokeExact(ptr);
	}
}
