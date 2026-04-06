package io.github.dfa1.rocksdbffm;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;

/**
 * FFM wrapper for {@code rocksdb_optimistictransaction_options_t}.
 *
 * <p>Unlike pessimistic {@link TransactionOptions}, there is no deadlock detection
 * or lock timeout — conflicts are detected at {@link Transaction#commit()} time.
 */
public final class OptimisticTransactionOptions implements AutoCloseable {

    private static final MethodHandle MH_CREATE;
    private static final MethodHandle MH_DESTROY;
    private static final MethodHandle MH_SET_SET_SNAPSHOT;

    static {
        MH_CREATE = RocksDB.lookup("rocksdb_optimistictransaction_options_create",
            FunctionDescriptor.of(ValueLayout.ADDRESS));

        MH_DESTROY = RocksDB.lookup("rocksdb_optimistictransaction_options_destroy",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

        // void rocksdb_optimistictransaction_options_set_set_snapshot(opt*, unsigned char v)
        MH_SET_SET_SNAPSHOT = RocksDB.lookup(
            "rocksdb_optimistictransaction_options_set_set_snapshot",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_BYTE));
    }

    /** Package-private: accessed by {@link OptimisticTransactionDB#beginTransaction}. */
    final MemorySegment ptr;

    public OptimisticTransactionOptions() {
        try {
            this.ptr = (MemorySegment) MH_CREATE.invokeExact();
        } catch (Throwable t) {
            throw new RocksDBException("optimistic transaction options create failed", t);
        }
    }

    /**
     * If {@code true}, a snapshot is taken at the start of the transaction.
     * The transaction will then check whether any keys it reads or writes
     * have been modified since that snapshot when {@link Transaction#commit()} is called.
     * Default: {@code false}.
     */
    public OptimisticTransactionOptions setSetSnapshot(boolean value) {
        try {
            MH_SET_SET_SNAPSHOT.invokeExact(ptr, value ? (byte) 1 : (byte) 0);
        } catch (Throwable t) {
            throw new RocksDBException("setSetSnapshot failed", t);
        }
        return this;
    }

    @Override
    public void close() {
        try {
            MH_DESTROY.invokeExact(ptr);
        } catch (Throwable t) {
            throw new RocksDBException("optimistic transaction options destroy failed", t);
        }
    }
}
