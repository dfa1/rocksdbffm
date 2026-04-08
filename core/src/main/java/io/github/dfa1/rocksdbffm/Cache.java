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

	// rocksdb_cache_destroy(rocksdb_cache_t* cache) -> void
	private static final MethodHandle MH_DESTROY;
	// rocksdb_cache_set_capacity(rocksdb_cache_t* cache, size_t capacity) -> void
	private static final MethodHandle MH_SET_CAPACITY;
	// rocksdb_cache_get_capacity(const rocksdb_cache_t* cache) -> size_t
	private static final MethodHandle MH_GET_CAPACITY;
	// rocksdb_cache_get_usage(const rocksdb_cache_t* cache) -> size_t
	private static final MethodHandle MH_GET_USAGE;
	// rocksdb_cache_get_pinned_usage(const rocksdb_cache_t* cache) -> size_t
	private static final MethodHandle MH_GET_PINNED_USAGE;

	static {
		MH_DESTROY = RocksDB.lookup("rocksdb_cache_destroy",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

		MH_SET_CAPACITY = RocksDB.lookup("rocksdb_cache_set_capacity",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));

		MH_GET_CAPACITY = RocksDB.lookup("rocksdb_cache_get_capacity",
				FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));

		MH_GET_USAGE = RocksDB.lookup("rocksdb_cache_get_usage",
				FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));

		MH_GET_PINNED_USAGE = RocksDB.lookup("rocksdb_cache_get_pinned_usage",
				FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));
	}

	protected Cache(MemorySegment ptr) {
		super(ptr);
	}

	/// Dynamically resizes the cache. Excess entries are evicted as needed.
	public void setCapacity(MemorySize capacity) {
		try {
			MH_SET_CAPACITY.invokeExact(ptr(), capacity.toBytes());
		} catch (Throwable t) {
			throw new RocksDBException("setCapacity failed", t);
		}
	}

	public MemorySize getCapacity() {
		try {
			return MemorySize.ofBytes((long) MH_GET_CAPACITY.invokeExact(ptr()));
		} catch (Throwable t) {
			throw new RocksDBException("getCapacity failed", t);
		}
	}

	public MemorySize getUsage() {
		try {
			return MemorySize.ofBytes((long) MH_GET_USAGE.invokeExact(ptr()));
		} catch (Throwable t) {
			throw new RocksDBException("getUsage failed", t);
		}
	}

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
