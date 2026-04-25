package io.github.dfa1.rocksdbffm;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/// FFM wrapper for `rocksdb_block_based_table_options_t`.
///
/// Configure and pass to [Options#setTableFormatConfig(BlockBasedTableOptions)].
/// `BlockBasedTableConfig` may be closed once the options have been applied —
/// RocksDB internally copies everything it needs.
///
/// ```
/// try (LRUCache cache = LRUCache.newLRUCache(MemorySize.ofMB(64));
///      BlockBasedTableConfig tbl = new BlockBasedTableConfig()
///          .setBlockSize(16 * 1024)
///          .setFilterPolicy(FilterPolicy.newBloom(10))
///          .setBlockCache(cache)
///          .setCacheIndexAndFilterBlocks(true);
///      Options opts = Options.newOptions()
///          .setCreateIfMissing(true)
///          .setTableFormatConfig(tbl)) {
///     ...
/// }
/// ```
/// ## Filter policy ownership
///
/// Calling [#setFilterPolicy(FilterPolicy)] transfers native ownership to this
/// config object. The `FilterPolicy` must not be used after that call.
public final class BlockBasedTableOptions extends NativeObject {

	// -----------------------------------------------------------------------
	// Index type constants (mirrors rocksdb_block_based_table_index_type_*)
	// -----------------------------------------------------------------------

	/// Index type used by the block-based table format.
	public enum IndexType {
		/// Standard binary-search index.
		BINARY_SEARCH(0),
		/// Hash-based index, requires prefix extractor.
		HASH_SEARCH(1),
		/// Two-level partitioned index (better for large SSTs).
		TWO_LEVEL_INDEX_SEARCH(2);

		final int value;

		IndexType(int v) {
			this.value = v;
		}
	}

	// -----------------------------------------------------------------------
	// Method handles
	// -----------------------------------------------------------------------

	/// `rocksdb_block_based_table_options_t* rocksdb_block_based_options_create(void);`
	private static final MethodHandle MH_CREATE;
	/// `void rocksdb_block_based_options_destroy(rocksdb_block_based_table_options_t* options);`
	private static final MethodHandle MH_DESTROY;
	/// `void rocksdb_block_based_options_set_block_size(rocksdb_block_based_table_options_t* options, size_t block_size);`
	private static final MethodHandle MH_SET_BLOCK_SIZE;
	/// `void rocksdb_block_based_options_set_filter_policy(rocksdb_block_based_table_options_t* options, rocksdb_filterpolicy_t* filter_policy);`
	private static final MethodHandle MH_SET_FILTER_POLICY;
	/// `void rocksdb_block_based_options_set_no_block_cache(rocksdb_block_based_table_options_t* options, unsigned char no_block_cache);`
	private static final MethodHandle MH_SET_NO_BLOCK_CACHE;
	/// `void rocksdb_block_based_options_set_block_cache(rocksdb_block_based_table_options_t* options, rocksdb_cache_t* block_cache);`
	private static final MethodHandle MH_SET_BLOCK_CACHE;
	/// `void rocksdb_block_based_options_set_cache_index_and_filter_blocks(rocksdb_block_based_table_options_t*, unsigned char);`
	private static final MethodHandle MH_SET_CACHE_INDEX_AND_FILTER_BLOCKS;
	/// `void rocksdb_block_based_options_set_index_type(rocksdb_block_based_table_options_t*, int);`
	private static final MethodHandle MH_SET_INDEX_TYPE;
	/// `void rocksdb_block_based_options_set_format_version(rocksdb_block_based_table_options_t*, int);`
	private static final MethodHandle MH_SET_FORMAT_VERSION;
	/// `void rocksdb_block_based_options_set_whole_key_filtering(rocksdb_block_based_table_options_t*, unsigned char);`
	private static final MethodHandle MH_SET_WHOLE_KEY_FILTERING;
	/// `void rocksdb_block_based_options_set_partition_filters(rocksdb_block_based_table_options_t* options, unsigned char partition_filters);`
	private static final MethodHandle MH_SET_PARTITION_FILTERS;

	static {
		MH_CREATE = NativeLibrary.lookup("rocksdb_block_based_options_create",
				FunctionDescriptor.of(ValueLayout.ADDRESS));

		MH_DESTROY = NativeLibrary.lookup("rocksdb_block_based_options_destroy",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

		MH_SET_BLOCK_SIZE = NativeLibrary.lookup("rocksdb_block_based_options_set_block_size",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));

		MH_SET_FILTER_POLICY = NativeLibrary.lookup("rocksdb_block_based_options_set_filter_policy",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_SET_NO_BLOCK_CACHE = NativeLibrary.lookup("rocksdb_block_based_options_set_no_block_cache",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_BYTE));

		MH_SET_BLOCK_CACHE = NativeLibrary.lookup("rocksdb_block_based_options_set_block_cache",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_SET_CACHE_INDEX_AND_FILTER_BLOCKS = NativeLibrary.lookup(
				"rocksdb_block_based_options_set_cache_index_and_filter_blocks",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_BYTE));

		MH_SET_INDEX_TYPE = NativeLibrary.lookup("rocksdb_block_based_options_set_index_type",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_INT));

		MH_SET_FORMAT_VERSION = NativeLibrary.lookup("rocksdb_block_based_options_set_format_version",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_INT));

		MH_SET_WHOLE_KEY_FILTERING = NativeLibrary.lookup(
				"rocksdb_block_based_options_set_whole_key_filtering",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_BYTE));

		MH_SET_PARTITION_FILTERS = NativeLibrary.lookup(
				"rocksdb_block_based_options_set_partition_filters",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_BYTE));
	}

	private BlockBasedTableOptions(MemorySegment ptr) {
		super(ptr);
	}

	/// Creates a new [BlockBasedTableOptions] with RocksDB defaults.
	///
	/// @return a new instance; caller must close it
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

	/// Size of each data block. Default: 4 KB.
	/// Larger blocks improve compression but increase read amplification.
	///
	/// @param blockSize desired block size
	/// @return `this` for chaining
	public BlockBasedTableOptions setBlockSize(MemorySize blockSize) {
		try {
			MH_SET_BLOCK_SIZE.invokeExact(ptr(), blockSize.toBytes());
		} catch (Throwable t) {
			throw new RocksDBException("setBlockSize failed", t);
		}
		return this;
	}

	/// Sets the filter policy. Transfers ownership of `policy` to this config; do not close it afterwards.
	///
	/// @param policy filter policy to use; ownership is transferred
	/// @return `this` for chaining
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

	/// If true, no block cache is used for this table. Default: false.
	/// Use when all data fits in memory or when block cache would be counter-productive.
	///
	/// @param noBlockCache `true` to disable block cache for this table
	/// @return `this` for chaining
	public BlockBasedTableOptions setNoBlockCache(boolean noBlockCache) {
		try {
			MH_SET_NO_BLOCK_CACHE.invokeExact(ptr(), noBlockCache ? (byte) 1 : (byte) 0);
		} catch (Throwable t) {
			throw new RocksDBException("setNoBlockCache failed", t);
		}
		return this;
	}

	/// Sets a custom block cache. The `cache` object remains owned by the caller
	/// and can be shared across multiple table configs.
	///
	/// @param cache block cache to use; caller retains ownership
	/// @return `this` for chaining
	public BlockBasedTableOptions setBlockCache(Cache cache) {
		try {
			MH_SET_BLOCK_CACHE.invokeExact(ptr(), cache.ptr());
		} catch (Throwable t) {
			throw new RocksDBException("setBlockCache failed", t);
		}
		return this;
	}

	/// If true, index and filter blocks are stored in the block cache (subject to
	/// eviction). Default: false (index/filter are pinned in memory).
	///
	/// @param value `true` to store index/filter blocks in the block cache
	/// @return `this` for chaining
	public BlockBasedTableOptions setCacheIndexAndFilterBlocks(boolean value) {
		try {
			MH_SET_CACHE_INDEX_AND_FILTER_BLOCKS.invokeExact(ptr(), value ? (byte) 1 : (byte) 0);
		} catch (Throwable t) {
			throw new RocksDBException("setCacheIndexAndFilterBlocks failed", t);
		}
		return this;
	}

	/// Sets the index type. Default: [IndexType#BINARY_SEARCH].
	/// Use [IndexType#TWO_LEVEL_INDEX_SEARCH] for very large SSTs.
	///
	/// @param indexType index type to use
	/// @return `this` for chaining
	public BlockBasedTableOptions setIndexType(IndexType indexType) {
		try {
			MH_SET_INDEX_TYPE.invokeExact(ptr(), indexType.value);
		} catch (Throwable t) {
			throw new RocksDBException("setIndexType failed", t);
		}
		return this;
	}

	/// Sets the SST format version. Higher versions enable newer features but
	/// reduce backward compatibility. Default: 2.
	///
	/// @param formatVersion SST format version to use
	/// @return `this` for chaining
	public BlockBasedTableOptions setFormatVersion(int formatVersion) {
		try {
			MH_SET_FORMAT_VERSION.invokeExact(ptr(), formatVersion);
		} catch (Throwable t) {
			throw new RocksDBException("setFormatVersion failed", t);
		}
		return this;
	}

	/// If true, a whole-key Bloom filter is built in addition to any prefix filter.
	/// Default: true. Set to false when only a prefix filter is desired.
	///
	/// @param value `true` to enable whole-key filtering
	/// @return `this` for chaining
	public BlockBasedTableOptions setWholeKeyFiltering(boolean value) {
		try {
			MH_SET_WHOLE_KEY_FILTERING.invokeExact(ptr(), value ? (byte) 1 : (byte) 0);
		} catch (Throwable t) {
			throw new RocksDBException("setWholeKeyFiltering failed", t);
		}
		return this;
	}

	/// If true, use partitioned Bloom filters (one small filter per index partition).
	/// Requires [IndexType#TWO_LEVEL_INDEX_SEARCH].
	///
	/// @param value `true` to enable partitioned filters
	/// @return `this` for chaining
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
