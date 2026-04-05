package io.github.dfa1.rocksdbffm;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.ByteBuffer;

/**
 * FFM wrapper for rocksdb_iterator_t.
 *
 * <p>Obtain via {@link RocksDB#newIterator()} or {@link RocksDB#newIterator(ReadOptions)}.
 * Always close after use.
 *
 * <pre>{@code
 * try (RocksIterator it = db.newIterator()) {
 *     for (it.seekToFirst(); it.isValid(); it.next()) {
 *         byte[] key   = it.key();
 *         byte[] value = it.value();
 *     }
 *     it.checkError();
 * }
 * }</pre>
 */
public final class RocksIterator implements AutoCloseable {

    // rocksdb_iterator_t* rocksdb_create_iterator(rocksdb_t*, rocksdb_readoptions_t*)
    static final MethodHandle MH_CREATE;

    private static final MethodHandle MH_DESTROY;
    private static final MethodHandle MH_VALID;
    private static final MethodHandle MH_SEEK_TO_FIRST;
    private static final MethodHandle MH_SEEK_TO_LAST;
    private static final MethodHandle MH_SEEK;
    private static final MethodHandle MH_SEEK_FOR_PREV;
    private static final MethodHandle MH_NEXT;
    private static final MethodHandle MH_PREV;
    // const char* rocksdb_iter_key(iter*, size_t* klen)
    private static final MethodHandle MH_KEY;
    // const char* rocksdb_iter_value(iter*, size_t* vlen)
    private static final MethodHandle MH_VALUE;
    // void rocksdb_iter_get_error(iter*, char** errptr)
    private static final MethodHandle MH_GET_ERROR;
    // void rocksdb_iter_refresh(iter*, char** errptr)
    private static final MethodHandle MH_REFRESH;

    static {
        MH_CREATE = RocksDB.lookup("rocksdb_create_iterator",
            FunctionDescriptor.of(ValueLayout.ADDRESS,
                ValueLayout.ADDRESS, ValueLayout.ADDRESS));

        MH_DESTROY = RocksDB.lookup("rocksdb_iter_destroy",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

        MH_VALID = RocksDB.lookup("rocksdb_iter_valid",
            FunctionDescriptor.of(ValueLayout.JAVA_BYTE, ValueLayout.ADDRESS));

        MH_SEEK_TO_FIRST = RocksDB.lookup("rocksdb_iter_seek_to_first",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

        MH_SEEK_TO_LAST = RocksDB.lookup("rocksdb_iter_seek_to_last",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

        // void rocksdb_iter_seek(iter*, key*, size_t klen)
        MH_SEEK = RocksDB.lookup("rocksdb_iter_seek",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS,
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));

        // void rocksdb_iter_seek_for_prev(iter*, key*, size_t klen)
        MH_SEEK_FOR_PREV = RocksDB.lookup("rocksdb_iter_seek_for_prev",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS,
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));

        MH_NEXT = RocksDB.lookup("rocksdb_iter_next",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

        MH_PREV = RocksDB.lookup("rocksdb_iter_prev",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

        MH_KEY = RocksDB.lookup("rocksdb_iter_key",
            FunctionDescriptor.of(ValueLayout.ADDRESS,
                ValueLayout.ADDRESS, ValueLayout.ADDRESS));

        MH_VALUE = RocksDB.lookup("rocksdb_iter_value",
            FunctionDescriptor.of(ValueLayout.ADDRESS,
                ValueLayout.ADDRESS, ValueLayout.ADDRESS));

        MH_GET_ERROR = RocksDB.lookup("rocksdb_iter_get_error",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS));

        MH_REFRESH = RocksDB.lookup("rocksdb_iter_refresh",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS));
    }

    private final MemorySegment ptr;

    /** Package-private: created by RocksDB. */
    RocksIterator(MemorySegment ptr) {
        this.ptr = ptr;
    }

    // -----------------------------------------------------------------------
    // Positioning
    // -----------------------------------------------------------------------

    /** Positions the iterator at the first key in the database. */
    public void seekToFirst() {
        try {
            MH_SEEK_TO_FIRST.invokeExact(ptr);
        } catch (Throwable t) {
            throw new RocksDBException("seekToFirst failed", t);
        }
    }

    /** Positions the iterator at the last key in the database. */
    public void seekToLast() {
        try {
            MH_SEEK_TO_LAST.invokeExact(ptr);
        } catch (Throwable t) {
            throw new RocksDBException("seekToLast failed", t);
        }
    }

    /** Positions the iterator at the first key &gt;= {@code target}. Slow path: copies key. */
    public void seek(byte[] target) {
        try (Arena arena = Arena.ofConfined()) {
            MH_SEEK.invokeExact(ptr, toNative(arena, target), (long) target.length);
        } catch (Throwable t) {
            throw new RocksDBException("seek failed", t);
        }
    }

    /** Positions the iterator at the first key &gt;= {@code target}. Zero-copy for direct buffers. */
    public void seek(ByteBuffer target) {
        try {
            MH_SEEK.invokeExact(ptr, MemorySegment.ofBuffer(target), (long) target.remaining());
        } catch (Throwable t) {
            throw new RocksDBException("seek failed", t);
        }
    }

    /** Positions the iterator at the first key &gt;= {@code target}. Zero-copy. */
    public void seek(MemorySegment target) {
        try {
            MH_SEEK.invokeExact(ptr, target, target.byteSize());
        } catch (Throwable t) {
            throw new RocksDBException("seek failed", t);
        }
    }

    /** Positions the iterator at the last key &lt;= {@code target}. Slow path: copies key. */
    public void seekForPrev(byte[] target) {
        try (Arena arena = Arena.ofConfined()) {
            MH_SEEK_FOR_PREV.invokeExact(ptr, toNative(arena, target), (long) target.length);
        } catch (Throwable t) {
            throw new RocksDBException("seekForPrev failed", t);
        }
    }

    /** Positions the iterator at the last key &lt;= {@code target}. Zero-copy for direct buffers. */
    public void seekForPrev(ByteBuffer target) {
        try {
            MH_SEEK_FOR_PREV.invokeExact(ptr, MemorySegment.ofBuffer(target), (long) target.remaining());
        } catch (Throwable t) {
            throw new RocksDBException("seekForPrev failed", t);
        }
    }

    /** Positions the iterator at the last key &lt;= {@code target}. Zero-copy. */
    public void seekForPrev(MemorySegment target) {
        try {
            MH_SEEK_FOR_PREV.invokeExact(ptr, target, target.byteSize());
        } catch (Throwable t) {
            throw new RocksDBException("seekForPrev failed", t);
        }
    }

    /** Moves to the next key. Only call when {@link #isValid()} is true. */
    public void next() {
        try {
            MH_NEXT.invokeExact(ptr);
        } catch (Throwable t) {
            throw new RocksDBException("next failed", t);
        }
    }

    /** Moves to the previous key. Only call when {@link #isValid()} is true. */
    public void prev() {
        try {
            MH_PREV.invokeExact(ptr);
        } catch (Throwable t) {
            throw new RocksDBException("prev failed", t);
        }
    }

    // -----------------------------------------------------------------------
    // State
    // -----------------------------------------------------------------------

    /** Returns true if the iterator is positioned at a valid key. */
    public boolean isValid() {
        try {
            return ((byte) MH_VALID.invokeExact(ptr)) != 0;
        } catch (Throwable t) {
            throw new RocksDBException("isValid failed", t);
        }
    }

    /**
     * Checks for any I/O error encountered during iteration.
     * Always call after an iteration loop to detect background errors.
     * Throws {@link RocksDBException} if an error occurred.
     */
    public void checkError() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment err = Native.errHolder(arena);
            MH_GET_ERROR.invokeExact(ptr, err);
            Native.checkError(err);
        } catch (Throwable t) {
            throw (t instanceof RocksDBException r) ? r : new RocksDBException("getError failed", t);
        }
    }

    /**
     * Refreshes the iterator to reflect the latest DB state after mutations.
     * Repositions to the same key if it still exists.
     */
    public void refresh() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment err = Native.errHolder(arena);
            MH_REFRESH.invokeExact(ptr, err);
            Native.checkError(err);
        } catch (Throwable t) {
            throw (t instanceof RocksDBException r) ? r : new RocksDBException("refresh failed", t);
        }
    }

    // -----------------------------------------------------------------------
    // Key/Value access — MemorySegment (zero-copy, valid until next navigation call)
    // -----------------------------------------------------------------------

    /**
     * Returns a zero-copy view of the current key.
     * The returned segment is only valid until the next positioning call.
     * Only call when {@link #isValid()} is true.
     */
    public MemorySegment keySegment() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment lenSeg = arena.allocate(ValueLayout.JAVA_LONG);
            MemorySegment data = (MemorySegment) MH_KEY.invokeExact(ptr, lenSeg);
            return data.reinterpret(lenSeg.get(ValueLayout.JAVA_LONG, 0));
        } catch (Throwable t) {
            throw new RocksDBException("key failed", t);
        }
    }

    /**
     * Returns a zero-copy view of the current value.
     * The returned segment is only valid until the next positioning call.
     * Only call when {@link #isValid()} is true.
     */
    public MemorySegment valueSegment() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment lenSeg = arena.allocate(ValueLayout.JAVA_LONG);
            MemorySegment data = (MemorySegment) MH_VALUE.invokeExact(ptr, lenSeg);
            return data.reinterpret(lenSeg.get(ValueLayout.JAVA_LONG, 0));
        } catch (Throwable t) {
            throw new RocksDBException("value failed", t);
        }
    }

    // -----------------------------------------------------------------------
    // Key/Value access — ByteBuffer (single copy into caller's buffer)
    // -----------------------------------------------------------------------

    /**
     * Copies the current key into {@code dst}. Returns the actual key length.
     * Only call when {@link #isValid()} is true.
     */
    public int key(ByteBuffer dst) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment lenSeg = arena.allocate(ValueLayout.JAVA_LONG);
            MemorySegment data = (MemorySegment) MH_KEY.invokeExact(ptr, lenSeg);
            long len = lenSeg.get(ValueLayout.JAVA_LONG, 0);
            int toCopy = (int) Math.min(len, dst.remaining());
            MemorySegment.ofBuffer(dst).copyFrom(data.reinterpret(toCopy));
            dst.position(dst.position() + toCopy);
            return (int) len;
        } catch (Throwable t) {
            throw new RocksDBException("key failed", t);
        }
    }

    /**
     * Copies the current value into {@code dst}. Returns the actual value length.
     * Only call when {@link #isValid()} is true.
     */
    public int value(ByteBuffer dst) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment lenSeg = arena.allocate(ValueLayout.JAVA_LONG);
            MemorySegment data = (MemorySegment) MH_VALUE.invokeExact(ptr, lenSeg);
            long len = lenSeg.get(ValueLayout.JAVA_LONG, 0);
            int toCopy = (int) Math.min(len, dst.remaining());
            MemorySegment.ofBuffer(dst).copyFrom(data.reinterpret(toCopy));
            dst.position(dst.position() + toCopy);
            return (int) len;
        } catch (Throwable t) {
            throw new RocksDBException("value failed", t);
        }
    }

    // -----------------------------------------------------------------------
    // Key/Value access — byte[] (convenience, always copies)
    // -----------------------------------------------------------------------

    /**
     * Returns a copy of the current key as a byte array.
     * Slower than the MemorySegment or ByteBuffer variants due to heap allocation.
     * Only call when {@link #isValid()} is true.
     */
    public byte[] key() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment lenSeg = arena.allocate(ValueLayout.JAVA_LONG);
            MemorySegment data = (MemorySegment) MH_KEY.invokeExact(ptr, lenSeg);
            return data.reinterpret(lenSeg.get(ValueLayout.JAVA_LONG, 0)).toArray(ValueLayout.JAVA_BYTE);
        } catch (Throwable t) {
            throw new RocksDBException("key failed", t);
        }
    }

    /**
     * Returns a copy of the current value as a byte array.
     * Slower than the MemorySegment or ByteBuffer variants due to heap allocation.
     * Only call when {@link #isValid()} is true.
     */
    public byte[] value() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment lenSeg = arena.allocate(ValueLayout.JAVA_LONG);
            MemorySegment data = (MemorySegment) MH_VALUE.invokeExact(ptr, lenSeg);
            return data.reinterpret(lenSeg.get(ValueLayout.JAVA_LONG, 0)).toArray(ValueLayout.JAVA_BYTE);
        } catch (Throwable t) {
            throw new RocksDBException("value failed", t);
        }
    }

    // -----------------------------------------------------------------------
    // AutoCloseable
    // -----------------------------------------------------------------------

    @Override
    public void close() {
        try {
            MH_DESTROY.invokeExact(ptr);
        } catch (Throwable t) {
            throw new RocksDBException("iterator destroy failed", t);
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static MemorySegment toNative(Arena arena, byte[] bytes) {
        MemorySegment seg = arena.allocate(bytes.length);
        MemorySegment.copy(bytes, 0, seg, ValueLayout.JAVA_BYTE, 0, bytes.length);
        return seg;
    }
}
