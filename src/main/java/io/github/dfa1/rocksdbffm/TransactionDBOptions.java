package io.github.dfa1.rocksdbffm;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;

/**
 * FFM wrapper for rocksdb_transactiondb_options_t.
 */
public final class TransactionDBOptions implements AutoCloseable {

    private static final MethodHandle MH_CREATE;
    private static final MethodHandle MH_DESTROY;
    private static final MethodHandle MH_SET_MAX_NUM_LOCKS;
    private static final MethodHandle MH_SET_NUM_STRIPES;

    static {
        MH_CREATE = RocksDB.lookup("rocksdb_transactiondb_options_create",
            FunctionDescriptor.of(ValueLayout.ADDRESS));

        MH_DESTROY = RocksDB.lookup("rocksdb_transactiondb_options_destroy",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

        // void rocksdb_transactiondb_options_set_max_num_locks(opt*, int64_t)
        MH_SET_MAX_NUM_LOCKS = RocksDB.lookup("rocksdb_transactiondb_options_set_max_num_locks",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));

        // void rocksdb_transactiondb_options_set_num_stripes(opt*, size_t)
        MH_SET_NUM_STRIPES = RocksDB.lookup("rocksdb_transactiondb_options_set_num_stripes",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));
    }

    /** Package-private: accessed by TransactionDB.open(). */
    final MemorySegment ptr;

    public TransactionDBOptions() {
        try {
            this.ptr = (MemorySegment) MH_CREATE.invokeExact();
        } catch (Throwable t) {
            throw new RocksDBException("transactiondb options create failed", t);
        }
    }

    /** Maximum number of locks held simultaneously. Default: -1 (unlimited). */
    public TransactionDBOptions setMaxNumLocks(long maxNumLocks) {
        try {
            MH_SET_MAX_NUM_LOCKS.invokeExact(ptr, maxNumLocks);
        } catch (Throwable t) {
            throw new RocksDBException("setMaxNumLocks failed", t);
        }
        return this;
    }

    /** Number of sub-lock-tables. Increasing reduces lock contention. Default: 16. */
    public TransactionDBOptions setNumStripes(long numStripes) {
        try {
            MH_SET_NUM_STRIPES.invokeExact(ptr, numStripes);
        } catch (Throwable t) {
            throw new RocksDBException("setNumStripes failed", t);
        }
        return this;
    }

    @Override
    public void close() {
        try {
            MH_DESTROY.invokeExact(ptr);
        } catch (Throwable t) {
            throw new RocksDBException("transactiondb options destroy failed", t);
        }
    }
}
