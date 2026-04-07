package io.github.dfa1.rocksdbffm;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/**
 * FFM wrapper for rocksdb_block_based_table_options_t.
 *
 * <p>Configure and pass to {@link Options#setTableFormatConfig(BlockBasedTableOptions)}.
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
 *      Options opts = Options.newOptions()
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
public final class BlockBasedTableOptions extends NativeObject {

	// -----------------------------------------------------------------------
	// Index type constants (mirrors rocksdb_block_based_table_index_type_*)
	// -----------------------------------------------------------------------

	public enum IndexType {
		/**
		 * Standard binary-search index.
		 */
		BINARY_SEARCH(0),
		/**
		 * Hash-based index, requires prefix extractor.
		 */
		HASH_SEARCH(1),
		/**
		 * Two-level partitioned index (better for large SSTs).
		 */
		TWO_LEVEL_INDEX_SEARCH(2);

		final int value;

		IndexType(int v) {
			this.value = v;
		}
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

	private BlockBasedTableOptions(MemorySegment ptr) {
		super(ptr);
	}

	public static BlockBasedTableOptions newBlockBasedConfig() {
		try {
			return new BlockBasedTableOptions((MemorySegment) MH_CREATE.invokeExact());
		} catch (Throwable e) {
			throw RocksDBException.wrap("new block based config", e);
		}
	}

	// -----------------------------------------------------------------------
	// Setters
	// -----------------------------------------------------------------------

	/**
	 * Size of each data block. Default: 4 KB.
	 * Larger blocks improve compression but increase read amplification.
	 */
	public BlockBasedTableOptions setBlockSize(MemorySize blockSize) {
		try {
			MH_SET_BLOCK_SIZE.invokeExact(ptr(), blockSize.toBytes());
		} catch (Throwable t) {
			throw new RocksDBException("setBlockSize failed", t);
		}
		return this;
	}

	/**
	 * Sets the filter policy.
	 */
	public BlockBasedTableOptions setFilterPolicy(FilterPolicy policy) {
		try {
			MH_SET_FILTER_POLICY.invokeExact(ptr(), policy.ptr());
		} catch (Throwable t) {
			throw new RocksDBException("setFilterPolicy failed", t);
		}
		// BlockBasedTableConfig will take care of the freeing the policy
		policy.transferOwnership();
		return this;
	}

	/**
	 * If true, no block cache is used for this table. Default: false.
	 * Use when all data fits in memory or when block cache would be counter-productive.
	 */
	public BlockBasedTableOptions setNoBlockCache(boolean noBlockCache) {
		try {
			MH_SET_NO_BLOCK_CACHE.invokeExact(ptr(), noBlockCache ? (byte) 1 : (byte) 0);
		} catch (Throwable t) {
			throw new RocksDBException("setNoBlockCache failed", t);
		}
		return this;
	}

	/**
	 * Sets a custom block cache. The {@code cache} object remains owned by the caller
	 * and can be shared across multiple table configs.
	 */
	public BlockBasedTableOptions setBlockCache(LRUCache cache) {
		try {
			MH_SET_BLOCK_CACHE.invokeExact(ptr(), cache.ptr);
		} catch (Throwable t) {
			throw new RocksDBException("setBlockCache failed", t);
		}
		return this;
	}

	/**
	 * If true, index and filter blocks are stored in the block cache (subject to
	 * eviction). Default: false (index/filter are pinned in memory).
	 */
	public BlockBasedTableOptions setCacheIndexAndFilterBlocks(boolean value) {
		try {
			MH_SET_CACHE_INDEX_AND_FILTER_BLOCKS.invokeExact(ptr(), value ? (byte) 1 : (byte) 0);
		} catch (Throwable t) {
			throw new RocksDBException("setCacheIndexAndFilterBlocks failed", t);
		}
		return this;
	}

	/**
	 * Sets the index type. Default: {@link IndexType#BINARY_SEARCH}.
	 * Use {@link IndexType#TWO_LEVEL_INDEX_SEARCH} for very large SSTs.
	 */
	public BlockBasedTableOptions setIndexType(IndexType indexType) {
		try {
			MH_SET_INDEX_TYPE.invokeExact(ptr(), indexType.value);
		} catch (Throwable t) {
			throw new RocksDBException("setIndexType failed", t);
		}
		return this;
	}

	/**
	 * Sets the SST format version. Higher versions enable newer features but
	 * reduce backward compatibility. Default: 2.
	 */
	public BlockBasedTableOptions setFormatVersion(int formatVersion) {
		try {
			MH_SET_FORMAT_VERSION.invokeExact(ptr(), formatVersion);
		} catch (Throwable t) {
			throw new RocksDBException("setFormatVersion failed", t);
		}
		return this;
	}

	/**
	 * If true, a whole-key Bloom filter is built in addition to any prefix filter.
	 * Default: true. Set to false when only a prefix filter is desired.
	 */
	public BlockBasedTableOptions setWholeKeyFiltering(boolean value) {
		try {
			MH_SET_WHOLE_KEY_FILTERING.invokeExact(ptr(), value ? (byte) 1 : (byte) 0);
		} catch (Throwable t) {
			throw new RocksDBException("setWholeKeyFiltering failed", t);
		}
		return this;
	}

	/**
	 * If true, use partitioned Bloom filters (one small filter per index partition).
	 * Requires {@link IndexType#TWO_LEVEL_INDEX_SEARCH}.
	 */
	public BlockBasedTableOptions setPartitionFilters(boolean value) {
		try {
			MH_SET_PARTITION_FILTERS.invokeExact(ptr(), value ? (byte) 1 : (byte) 0);
		} catch (Throwable t) {
			throw new RocksDBException("setPartitionFilters failed", t);
		}
		return this;
	}

	@Override
	public void tryClose(MemorySegment ptr) throws Throwable {
		MH_DESTROY.invokeExact(ptr);
	}
}
