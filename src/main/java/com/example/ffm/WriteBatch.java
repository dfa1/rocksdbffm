package com.example.ffm;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;

/**
 * FFM wrapper for rocksdb_writebatch_t.
 * Accumulates put/delete operations and commits them atomically via RocksDB.write().
 *
 * Note: rocksdb_writebatch_put/delete have no errptr — they are infallible at
 * the C level. Only rocksdb_write (on the DB) can fail.
 */
public final class WriteBatch implements AutoCloseable {

    private static final MethodHandle MH_CREATE;
    private static final MethodHandle MH_DESTROY;
    private static final MethodHandle MH_PUT;
    private static final MethodHandle MH_DELETE;
    private static final MethodHandle MH_CLEAR;
    private static final MethodHandle MH_COUNT;

    static {
        MH_CREATE = RocksDB.lookup("rocksdb_writebatch_create",
            FunctionDescriptor.of(ValueLayout.ADDRESS));

        MH_DESTROY = RocksDB.lookup("rocksdb_writebatch_destroy",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

        // void rocksdb_writebatch_put(batch*, key*, klen, val*, vlen)
        MH_PUT = RocksDB.lookup("rocksdb_writebatch_put",
            FunctionDescriptor.ofVoid(
                ValueLayout.ADDRESS,
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));

        // void rocksdb_writebatch_delete(batch*, key*, klen)
        MH_DELETE = RocksDB.lookup("rocksdb_writebatch_delete",
            FunctionDescriptor.ofVoid(
                ValueLayout.ADDRESS,
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));

        MH_CLEAR = RocksDB.lookup("rocksdb_writebatch_clear",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

        MH_COUNT = RocksDB.lookup("rocksdb_writebatch_count",
            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));
    }

    /** Package-private: accessed by RocksDB.write(WriteBatch). */
    final MemorySegment ptr;

    private WriteBatch(MemorySegment ptr) {
        this.ptr = ptr;
    }

    public static WriteBatch create() {
        try {
            return new WriteBatch((MemorySegment) MH_CREATE.invokeExact());
        } catch (Throwable t) {
            throw new RocksDBException("writebatch create failed", t);
        }
    }

    public void put(byte[] key, byte[] value) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment k = toNative(arena, key);
            MemorySegment v = toNative(arena, value);
            MH_PUT.invokeExact(ptr, k, (long) key.length, v, (long) value.length);
        } catch (Throwable t) {
            throw new RocksDBException("writebatch put failed", t);
        }
    }

    public void delete(byte[] key) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment k = toNative(arena, key);
            MH_DELETE.invokeExact(ptr, k, (long) key.length);
        } catch (Throwable t) {
            throw new RocksDBException("writebatch delete failed", t);
        }
    }

    public void clear() {
        try {
            MH_CLEAR.invokeExact(ptr);
        } catch (Throwable t) {
            throw new RocksDBException("writebatch clear failed", t);
        }
    }

    public int count() {
        try {
            return (int) MH_COUNT.invokeExact(ptr);
        } catch (Throwable t) {
            throw new RocksDBException("writebatch count failed", t);
        }
    }

    @Override
    public void close() {
        try {
            MH_DESTROY.invokeExact(ptr);
        } catch (Throwable t) {
            throw new RocksDBException("writebatch destroy failed", t);
        }
    }

    private static MemorySegment toNative(Arena arena, byte[] bytes) {
        MemorySegment seg = arena.allocate(bytes.length);
        MemorySegment.copy(bytes, 0, seg, ValueLayout.JAVA_BYTE, 0, bytes.length);
        return seg;
    }
}
