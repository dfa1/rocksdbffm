package io.github.dfa1.rocksdbffm;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;

/**
 * FFM wrapper for rocksdb_filterpolicy_t.
 *
 * <p>Pass to {@link BlockBasedTableConfig#setFilterPolicy(FilterPolicy)}.
 * Ownership transfers to the {@code BlockBasedTableConfig} on that call —
 * the filter policy is then managed by RocksDB's internal reference counting
 * and must not be closed by the caller.
 *
 * <pre>{@code
 * BlockBasedTableConfig tbl = new BlockBasedTableConfig()
 *     .setFilterPolicy(FilterPolicy.newBloom(10));
 * // do NOT close the FilterPolicy — BlockBasedTableConfig owns it now
 * }</pre>
 */
public final class FilterPolicy {

    private static final MethodHandle MH_CREATE_BLOOM;
    private static final MethodHandle MH_CREATE_RIBBON;
    static final MethodHandle MH_DESTROY;

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

    /** Package-private: read by BlockBasedTableConfig. */
    final MemorySegment ptr;

    private FilterPolicy(MemorySegment ptr) {
        this.ptr = ptr;
    }

    /**
     * Creates a Bloom filter with the given number of bits per key.
     * Typical value: {@code 10} (≈1% false-positive rate).
     * Ownership transfers to the {@link BlockBasedTableConfig} it is passed to.
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
     * Ownership transfers to the {@link BlockBasedTableConfig} it is passed to.
     */
    public static FilterPolicy newRibbon(double bloomEquivalentBitsPerKey) {
        try {
            return new FilterPolicy((MemorySegment) MH_CREATE_RIBBON.invokeExact(bloomEquivalentBitsPerKey));
        } catch (Throwable t) {
            throw new RocksDBException("FilterPolicy.newRibbon failed", t);
        }
    }
}
