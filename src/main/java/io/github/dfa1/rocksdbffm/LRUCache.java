package io.github.dfa1.rocksdbffm;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;

/**
 * FFM wrapper for rocksdb_cache_t (LRU block cache).
 *
 * <p>Pass to {@link BlockBasedTableConfig#setBlockCache(LRUCache)} to share a single
 * cache across multiple column families or DB instances.
 *
 * <pre>{@code
 * try (LRUCache cache = new LRUCache(64 * 1024 * 1024)) { // 64 MB
 *     BlockBasedTableConfig tbl = new BlockBasedTableConfig()
 *         .setBlockCache(cache);
 *     ...
 * }
 * }</pre>
 */
public final class LRUCache implements AutoCloseable {

    private static final MethodHandle MH_CREATE;
    private static final MethodHandle MH_DESTROY;
    private static final MethodHandle MH_SET_CAPACITY;
    private static final MethodHandle MH_GET_CAPACITY;
    private static final MethodHandle MH_GET_USAGE;
    private static final MethodHandle MH_GET_PINNED_USAGE;

    static {
        // rocksdb_cache_t* rocksdb_cache_create_lru(size_t capacity)
        MH_CREATE = RocksDB.lookup("rocksdb_cache_create_lru",
            FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));

        MH_DESTROY = RocksDB.lookup("rocksdb_cache_destroy",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

        // void rocksdb_cache_set_capacity(cache*, size_t)
        MH_SET_CAPACITY = RocksDB.lookup("rocksdb_cache_set_capacity",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));

        // size_t rocksdb_cache_get_capacity(cache*)
        MH_GET_CAPACITY = RocksDB.lookup("rocksdb_cache_get_capacity",
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));

        // size_t rocksdb_cache_get_usage(cache*)
        MH_GET_USAGE = RocksDB.lookup("rocksdb_cache_get_usage",
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));

        // size_t rocksdb_cache_get_pinned_usage(cache*)
        MH_GET_PINNED_USAGE = RocksDB.lookup("rocksdb_cache_get_pinned_usage",
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));
    }

    /** Package-private: accessed by BlockBasedTableConfig. */
    final MemorySegment ptr;

    /**
     * Creates an LRU block cache with the given capacity in bytes.
     */
    public LRUCache(long capacityBytes) {
        try {
            this.ptr = (MemorySegment) MH_CREATE.invokeExact(capacityBytes);
        } catch (Throwable t) {
            throw new RocksDBException("LRUCache create failed", t);
        }
    }

    /** Dynamically resizes the cache. Excess entries are evicted as needed. */
    public void setCapacity(long capacityBytes) {
        try {
            MH_SET_CAPACITY.invokeExact(ptr, capacityBytes);
        } catch (Throwable t) {
            throw new RocksDBException("setCapacity failed", t);
        }
    }

    /** Returns the configured capacity in bytes. */
    public long getCapacity() {
        try {
            return (long) MH_GET_CAPACITY.invokeExact(ptr);
        } catch (Throwable t) {
            throw new RocksDBException("getCapacity failed", t);
        }
    }

    /** Returns the current memory usage in bytes. */
    public long getUsage() {
        try {
            return (long) MH_GET_USAGE.invokeExact(ptr);
        } catch (Throwable t) {
            throw new RocksDBException("getUsage failed", t);
        }
    }

    /** Returns the current pinned memory usage in bytes. */
    public long getPinnedUsage() {
        try {
            return (long) MH_GET_PINNED_USAGE.invokeExact(ptr);
        } catch (Throwable t) {
            throw new RocksDBException("getPinnedUsage failed", t);
        }
    }

    @Override
    public void close() {
        try {
            MH_DESTROY.invokeExact(ptr);
        } catch (Throwable t) {
            throw new RocksDBException("LRUCache destroy failed", t);
        }
    }
}
