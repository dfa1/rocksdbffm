package io.github.dfa1.rocksdbffm;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/// FFM wrapper for `rocksdb_filterpolicy_t`.
///
/// Use inside a try-with-resources block. If the policy is passed to
/// [BlockBasedTableOptions#setFilterPolicy(FilterPolicy)], ownership
/// transfers to RocksDB's internal reference counting and [#close()]
/// becomes a no-op — it is safe (and recommended) to still call it via
/// try-with-resources.
///
/// ```
/// try (var filter = FilterPolicy.newBloom(10);
///      var tbl = new BlockBasedTableConfig().setFilterPolicy(filter);
///      var opts = Options.newOptions().setCreateIfMissing(true).setTableFormatConfig(tbl)) {
///     // filter.close() called automatically — no-op because ownership transferred
/// }
/// ```
public final class FilterPolicy extends NativeObject {

	/// `rocksdb_filterpolicy_t* rocksdb_filterpolicy_create_bloom(double bits_per_key);`
	private static final MethodHandle MH_CREATE_BLOOM;
	/// `rocksdb_filterpolicy_t* rocksdb_filterpolicy_create_ribbon(double bloom_equivalent_bits_per_key);`
	private static final MethodHandle MH_CREATE_RIBBON;
	/// `void rocksdb_filterpolicy_destroy(rocksdb_filterpolicy_t*);`
	private static final MethodHandle MH_DESTROY;

	static {
		MH_CREATE_BLOOM = NativeLibrary.lookup("rocksdb_filterpolicy_create_bloom",
				FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.JAVA_DOUBLE));

		MH_CREATE_RIBBON = NativeLibrary.lookup("rocksdb_filterpolicy_create_ribbon",
				FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.JAVA_DOUBLE));

		MH_DESTROY = NativeLibrary.lookup("rocksdb_filterpolicy_destroy",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));
	}

	private FilterPolicy(MemorySegment ptr) {
		super(ptr);
	}

	/// Creates a Bloom filter with the given number of bits per key.
	/// Typical value: `10` (≈1% false-positive rate).
	///
	/// @param bitsPerKey number of bits per key (higher = lower false-positive rate)
	/// @return a new [FilterPolicy]; caller must close it (or transfer ownership via [BlockBasedTableOptions#setFilterPolicy])
	public static FilterPolicy newBloom(double bitsPerKey) {
		try {
			return new FilterPolicy((MemorySegment) MH_CREATE_BLOOM.invokeExact(bitsPerKey));
		} catch (Throwable t) {
			throw new RocksDBException("FilterPolicy.newBloom failed", t);
		}
	}

	/// Creates a Ribbon filter (successor to Bloom, better space efficiency at
	/// similar query cost). Uses `bloomEquivalentBitsPerKey` to set the
	/// target false-positive rate equivalent to a Bloom filter at that setting.
	///
	/// @param bloomEquivalentBitsPerKey bits-per-key equivalent to a Bloom filter at that false-positive rate
	/// @return a new [FilterPolicy]; caller must close it (or transfer ownership via [BlockBasedTableOptions#setFilterPolicy])
	public static FilterPolicy newRibbon(double bloomEquivalentBitsPerKey) {
		try {
			return new FilterPolicy((MemorySegment) MH_CREATE_RIBBON.invokeExact(bloomEquivalentBitsPerKey));
		} catch (Throwable t) {
			throw new RocksDBException("FilterPolicy.newRibbon failed", t);
		}
	}

	@Override
	public void tryClose(MemorySegment ptr) throws Throwable {
		MH_DESTROY.invokeExact(ptr);
	}
}
