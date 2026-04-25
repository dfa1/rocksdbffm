package io.github.dfa1.rocksdbffm;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/// LRU block cache (`rocksdb_cache_t`).
/// ```
/// try (LRUCache cache = LRUCache.newLRUCache(MemorySize.ofMB(64))) {
///     BlockBasedTableOptions tbl = BlockBasedTableOptions.newBlockBasedTableOptions()
///         .setBlockCache(cache);
///     ...
/// }
/// ```
public final class LRUCache extends Cache {

	/// `rocksdb_cache_t* rocksdb_cache_create_lru(size_t capacity);`
	private static final MethodHandle MH_CREATE;

	static {
		MH_CREATE = NativeLibrary.lookup("rocksdb_cache_create_lru",
				FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));
	}

	private LRUCache(MemorySegment ptr) {
		super(ptr);
	}

	/// Creates an LRU block cache with the given capacity.
	///
	/// @param capacity total cache capacity
	/// @return a new [LRUCache]; caller must close it
	public static LRUCache newLRUCache(MemorySize capacity) {
		try {
			return new LRUCache((MemorySegment) MH_CREATE.invokeExact(capacity.toBytes()));
		} catch (Throwable t) {
			throw new RocksDBException("LRUCache create failed", t);
		}
	}
}
