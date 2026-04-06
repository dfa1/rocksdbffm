package io.github.dfa1.rocksdbffm;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;

/**
 * FFM wrapper for rocksdb_block_based_table_options_t.
 *
 * <p>Configure and pass to {@link Options#setTableFormatConfig(BlockBasedTableConfig)}.
 * {@code BlockBasedTableConfig} may be closed once the options have been applied —
 * RocksDB internally copies everything it needs.
 *
 * <pre>{@code
 * try (LRUCache cache = new LRUCache(64 << 20);
 *      BlockBasedTableConfig tbl = new BlockBasedTableConfig()
 *          .setBlockSize(16 * 1024)
 *          .setFilterPolicy(FilterPolicy.newBloom(10))
 *          .setBlockCache(cache)
 *          .setCacheIndexAndFilterBlocks(true);
 *      Options opts = new Options()
 *          .setCreateIfMissing(true)
 *          .setTableFormatConfig(tbl)) {
 *     ...
 * }
 * }</pre>
 *
 * <h2>Filter policy ownership</h2>
 * Calling {@link #setFilterPolicy(FilterPolicy)} transfers native ownership to this
 * config object. The {@code FilterPolicy} must not be used after that call.
 */
public final class BlockBasedTableConfig implements AutoCloseable {

    // -----------------------------------------------------------------------
    // Index type constants (mirrors rocksdb_block_based_table_index_type_*)
    // -----------------------------------------------------------------------

    public enum IndexType {
        /** Standard binary-search index. */
        BINARY_SEARCH(0),
        /** Hash-based index, requires prefix extractor. */
        HASH_SEARCH(1),
        /** Two-level partitioned index (better for large SSTs). */
        TWO_LEVEL_INDEX_SEARCH(2);

        final int value;
        IndexType(int v) { this.value = v; }
    }

    // -----------------------------------------------------------------------
    // Method handles
    // -----------------------------------------------------------------------

    private static final MethodHandle MH_CREATE;
    private static final MethodHandle MH_DESTROY;
    private static final MethodHandle MH_SET_BLOCK_SIZE;
    private static final MethodHandle MH_SET_FILTER_POLICY;
    private static final MethodHandle MH_SET_NO_BLOCK_CACHE;
    private static final MethodHandle MH_SET_BLOCK_CACHE;
    private static final MethodHandle MH_SET_CACHE_INDEX_AND_FILTER_BLOCKS;
    private static final MethodHandle MH_SET_INDEX_TYPE;
    private static final MethodHandle MH_SET_FORMAT_VERSION;
    private static final MethodHandle MH_SET_WHOLE_KEY_FILTERING;
    private static final MethodHandle MH_SET_PARTITION_FILTERS;

    static {
        MH_CREATE = RocksDB.lookup("rocksdb_block_based_options_create",
            FunctionDescriptor.of(ValueLayout.ADDRESS));

        MH_DESTROY = RocksDB.lookup("rocksdb_block_based_options_destroy",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

        // void rocksdb_block_based_options_set_block_size(opts*, size_t)
        MH_SET_BLOCK_SIZE = RocksDB.lookup("rocksdb_block_based_options_set_block_size",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));

        // void rocksdb_block_based_options_set_filter_policy(opts*, filterpolicy_t*)
        MH_SET_FILTER_POLICY = RocksDB.lookup("rocksdb_block_based_options_set_filter_policy",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS));

        // void rocksdb_block_based_options_set_no_block_cache(opts*, unsigned char)
        MH_SET_NO_BLOCK_CACHE = RocksDB.lookup("rocksdb_block_based_options_set_no_block_cache",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_BYTE));

        // void rocksdb_block_based_options_set_block_cache(opts*, cache_t*)
        MH_SET_BLOCK_CACHE = RocksDB.lookup("rocksdb_block_based_options_set_block_cache",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS));

        // void rocksdb_block_based_options_set_cache_index_and_filter_blocks(opts*, unsigned char)
        MH_SET_CACHE_INDEX_AND_FILTER_BLOCKS = RocksDB.lookup(
            "rocksdb_block_based_options_set_cache_index_and_filter_blocks",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_BYTE));

        // void rocksdb_block_based_options_set_index_type(opts*, int)
        MH_SET_INDEX_TYPE = RocksDB.lookup("rocksdb_block_based_options_set_index_type",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_INT));

        // void rocksdb_block_based_options_set_format_version(opts*, int)
        MH_SET_FORMAT_VERSION = RocksDB.lookup("rocksdb_block_based_options_set_format_version",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_INT));

        // void rocksdb_block_based_options_set_whole_key_filtering(opts*, unsigned char)
        MH_SET_WHOLE_KEY_FILTERING = RocksDB.lookup(
            "rocksdb_block_based_options_set_whole_key_filtering",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_BYTE));

        // void rocksdb_block_based_options_set_partition_filters(opts*, unsigned char)
        MH_SET_PARTITION_FILTERS = RocksDB.lookup(
            "rocksdb_block_based_options_set_partition_filters",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_BYTE));
    }

    /** Package-private: accessed by Options.setTableFormatConfig(). */
    final MemorySegment ptr;

    public BlockBasedTableConfig() {
        try {
            this.ptr = (MemorySegment) MH_CREATE.invokeExact();
        } catch (Throwable t) {
            throw new RocksDBException("BlockBasedTableConfig create failed", t);
        }
    }

    // -----------------------------------------------------------------------
    // Setters
    // -----------------------------------------------------------------------

    /**
     * Size of each data block. Default: 4 KB.
     * Larger blocks improve compression but increase read amplification.
     */
    public BlockBasedTableConfig setBlockSize(MemorySize blockSize) {
        try {
            MH_SET_BLOCK_SIZE.invokeExact(ptr, blockSize.toBytes());
        } catch (Throwable t) {
            throw new RocksDBException("setBlockSize failed", t);
        }
        return this;
    }

    /**
     * Sets the filter policy. Native ownership transfers to RocksDB's internal
     * reference counting. The {@code policy} may still be closed via
     * try-with-resources — {@link FilterPolicy#close()} becomes a no-op after transfer.
     */
    public BlockBasedTableConfig setFilterPolicy(FilterPolicy policy) {
        try {
            MH_SET_FILTER_POLICY.invokeExact(ptr, policy.ptr);
            policy.transferOwnership();
        } catch (Throwable t) {
            throw new RocksDBException("setFilterPolicy failed", t);
        }
        return this;
    }

    /**
     * If true, no block cache is used for this table. Default: false.
     * Use when all data fits in memory or when block cache would be counter-productive.
     */
    public BlockBasedTableConfig setNoBlockCache(boolean noBlockCache) {
        try {
            MH_SET_NO_BLOCK_CACHE.invokeExact(ptr, noBlockCache ? (byte) 1 : (byte) 0);
        } catch (Throwable t) {
            throw new RocksDBException("setNoBlockCache failed", t);
        }
        return this;
    }

    /**
     * Sets a custom block cache. The {@code cache} object remains owned by the caller
     * and can be shared across multiple table configs.
     */
    public BlockBasedTableConfig setBlockCache(LRUCache cache) {
        try {
            MH_SET_BLOCK_CACHE.invokeExact(ptr, cache.ptr);
        } catch (Throwable t) {
            throw new RocksDBException("setBlockCache failed", t);
        }
        return this;
    }

    /**
     * If true, index and filter blocks are stored in the block cache (subject to
     * eviction). Default: false (index/filter are pinned in memory).
     */
    public BlockBasedTableConfig setCacheIndexAndFilterBlocks(boolean value) {
        try {
            MH_SET_CACHE_INDEX_AND_FILTER_BLOCKS.invokeExact(ptr, value ? (byte) 1 : (byte) 0);
        } catch (Throwable t) {
            throw new RocksDBException("setCacheIndexAndFilterBlocks failed", t);
        }
        return this;
    }

    /**
     * Sets the index type. Default: {@link IndexType#BINARY_SEARCH}.
     * Use {@link IndexType#TWO_LEVEL_INDEX_SEARCH} for very large SSTs.
     */
    public BlockBasedTableConfig setIndexType(IndexType indexType) {
        try {
            MH_SET_INDEX_TYPE.invokeExact(ptr, indexType.value);
        } catch (Throwable t) {
            throw new RocksDBException("setIndexType failed", t);
        }
        return this;
    }

    /**
     * Sets the SST format version. Higher versions enable newer features but
     * reduce backward compatibility. Default: 2.
     */
    public BlockBasedTableConfig setFormatVersion(int formatVersion) {
        try {
            MH_SET_FORMAT_VERSION.invokeExact(ptr, formatVersion);
        } catch (Throwable t) {
            throw new RocksDBException("setFormatVersion failed", t);
        }
        return this;
    }

    /**
     * If true, a whole-key Bloom filter is built in addition to any prefix filter.
     * Default: true. Set to false when only a prefix filter is desired.
     */
    public BlockBasedTableConfig setWholeKeyFiltering(boolean value) {
        try {
            MH_SET_WHOLE_KEY_FILTERING.invokeExact(ptr, value ? (byte) 1 : (byte) 0);
        } catch (Throwable t) {
            throw new RocksDBException("setWholeKeyFiltering failed", t);
        }
        return this;
    }

    /**
     * If true, use partitioned Bloom filters (one small filter per index partition).
     * Requires {@link IndexType#TWO_LEVEL_INDEX_SEARCH}.
     */
    public BlockBasedTableConfig setPartitionFilters(boolean value) {
        try {
            MH_SET_PARTITION_FILTERS.invokeExact(ptr, value ? (byte) 1 : (byte) 0);
        } catch (Throwable t) {
            throw new RocksDBException("setPartitionFilters failed", t);
        }
        return this;
    }

    @Override
    public void close() {
        try {
            MH_DESTROY.invokeExact(ptr);
        } catch (Throwable t) {
            throw new RocksDBException("BlockBasedTableConfig destroy failed", t);
        }
    }
}
