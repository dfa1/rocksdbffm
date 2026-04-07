package io.github.dfa1.rocksdbffm;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/**
 * FFM wrapper for rocksdb_filterpolicy_t.
 *
 * <p>Use inside a try-with-resources block. If the policy is passed to
 * {@link BlockBasedTableConfig#setFilterPolicy(FilterPolicy)}, ownership
 * transfers to RocksDB's internal reference counting and {@link #close()}
 * becomes a no-op — it is safe (and recommended) to still call it via
 * try-with-resources.
 *
 * <pre>{@code
 * try (var filter = FilterPolicy.newBloom(10);
 *      var tbl = new BlockBasedTableConfig().setFilterPolicy(filter);
 *      var opts = new Options().setCreateIfMissing(true).setTableFormatConfig(tbl)) {
 *     // filter.close() called automatically — no-op because ownership transferred
 * }
 * }</pre>
 */
public final class FilterPolicy implements AutoCloseable {

	private static final MethodHandle MH_CREATE_BLOOM;
	private static final MethodHandle MH_CREATE_RIBBON;
	private static final MethodHandle MH_DESTROY;

	static {
		// rocksdb_filterpolicy_t* rocksdb_filterpolicy_create_bloom(double bits_per_key)
		MH_CREATE_BLOOM = RocksDB.lookup("rocksdb_filterpolicy_create_bloom",
				FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.JAVA_DOUBLE));

		// rocksdb_filterpolicy_t* rocksdb_filterpolicy_create_ribbon(double bloom_equivalent_bits_per_key)
		MH_CREATE_RIBBON = RocksDB.lookup("rocksdb_filterpolicy_create_ribbon",
				FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.JAVA_DOUBLE));

		MH_DESTROY = RocksDB.lookup("rocksdb_filterpolicy_destroy",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));
	}

	/**
	 * Package-private: read by BlockBasedTableConfig.
	 */
	final MemorySegment ptr;

	/**
	 * True once ownership has been transferred to a BlockBasedTableConfig.
	 * After transfer, close() is a no-op — RocksDB manages the lifetime.
	 */
	private boolean transferred;

	private FilterPolicy(MemorySegment ptr) {
		this.ptr = ptr;
	}

	/**
	 * Creates a Bloom filter with the given number of bits per key.
	 * Typical value: {@code 10} (≈1% false-positive rate).
	 */
	public static FilterPolicy newBloom(double bitsPerKey) {
		try {
			return new FilterPolicy((MemorySegment) MH_CREATE_BLOOM.invokeExact(bitsPerKey));
		} catch (Throwable t) {
			throw new RocksDBException("FilterPolicy.newBloom failed", t);
		}
	}

	/**
	 * Creates a Ribbon filter (successor to Bloom, better space efficiency at
	 * similar query cost). Uses {@code bloomEquivalentBitsPerKey} to set the
	 * target false-positive rate equivalent to a Bloom filter at that setting.
	 */
	public static FilterPolicy newRibbon(double bloomEquivalentBitsPerKey) {
		try {
			return new FilterPolicy((MemorySegment) MH_CREATE_RIBBON.invokeExact(bloomEquivalentBitsPerKey));
		} catch (Throwable t) {
			throw new RocksDBException("FilterPolicy.newRibbon failed", t);
		}
	}

	/**
	 * Called by {@link BlockBasedTableConfig#setFilterPolicy(FilterPolicy)} to
	 * indicate that native ownership has transferred. Subsequent {@link #close()}
	 * calls are no-ops.
	 */
	void transferOwnership() {
		this.transferred = true;
	}

	/**
	 * Destroys the native filter policy unless ownership has been transferred
	 * to a {@link BlockBasedTableConfig}, in which case this is a no-op.
	 */
	@Override
	public void close() {
		if (transferred) return;
		Native.closeQuietly(() -> MH_DESTROY.invokeExact(ptr));
	}
}
