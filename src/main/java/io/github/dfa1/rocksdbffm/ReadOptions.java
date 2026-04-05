package io.github.dfa1.rocksdbffm;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;

/**
 * FFM wrapper for rocksdb_readoptions_t.
 */
public final class ReadOptions implements AutoCloseable {

    static final MethodHandle MH_CREATE;
    static final MethodHandle MH_DESTROY;

    static {
        MH_CREATE = RocksDB.lookup("rocksdb_readoptions_create",
            FunctionDescriptor.of(ValueLayout.ADDRESS));

        MH_DESTROY = RocksDB.lookup("rocksdb_readoptions_destroy",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));
    }

    /** Package-private: accessed by Transaction.get(). */
    final MemorySegment ptr;

    public ReadOptions() {
        try {
            this.ptr = (MemorySegment) MH_CREATE.invokeExact();
        } catch (Throwable t) {
            throw new RocksDBException("readoptions create failed", t);
        }
    }

    @Override
    public void close() {
        try {
            MH_DESTROY.invokeExact(ptr);
        } catch (Throwable t) {
            throw new RocksDBException("readoptions destroy failed", t);
        }
    }
}
