package io.github.dfa1.rocksdbffm;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.nio.ByteBuffer;

/**
 * FFM-based wrapper around the native RocksDB C library.
 * Binds directly to librocksdb.dylib without JNI.
 *
 * <h2>Performance design</h2>
 * <ul>
 *   <li><b>Thread-local auxiliary segments</b> — {@code errHolder} and {@code valLenHolder}
 *       are pre-allocated once per thread via {@code Arena.ofAuto()}, eliminating the
 *       per-call Arena create/destroy overhead.</li>
 *   <li><b>PinnableSlice reads</b> — {@code rocksdb_get_pinned} pins data directly from
 *       the block cache, avoiding the intermediate {@code std::string} copy that
 *       {@code rocksdb_get} performs. The pinned pointer is read and the pin released
 *       immediately after the copy.</li>
 *   <li><b>Zero-copy ByteBuffer puts</b> — {@code MemorySegment.ofBuffer(directBuffer)}
 *       wraps the buffer's native memory directly; no heap→native copy occurs.</li>
 *   <li><b>Zero-copy ByteBuffer gets</b> — combining PinnableSlice + direct output
 *       ByteBuffer gives a single copy from block cache to the caller's buffer,
 *       with no intermediate allocations.</li>
 * </ul>
 */
public final class RocksDB implements AutoCloseable {

    // -----------------------------------------------------------------------
    // Static: library + method handles (initialized once at class load)
    // -----------------------------------------------------------------------

    private static final Linker LINKER = Linker.nativeLinker();
    private static final SymbolLookup LIB;

    private static final MethodHandle MH_OPTIONS_CREATE;
    private static final MethodHandle MH_OPTIONS_SET_CREATE_IF_MISSING;
    private static final MethodHandle MH_OPTIONS_DESTROY;
    private static final MethodHandle MH_OPEN;
    private static final MethodHandle MH_CLOSE;
    private static final MethodHandle MH_WRITEOPTIONS_CREATE;
    private static final MethodHandle MH_WRITEOPTIONS_DESTROY;
    private static final MethodHandle MH_READOPTIONS_CREATE;
    private static final MethodHandle MH_READOPTIONS_DESTROY;
    private static final MethodHandle MH_PUT;
    private static final MethodHandle MH_GET;
    private static final MethodHandle MH_GET_PINNED;
    private static final MethodHandle MH_PINNABLESLICE_DESTROY;
    private static final MethodHandle MH_PINNABLESLICE_VALUE;
    private static final MethodHandle MH_DELETE;
    private static final MethodHandle MH_WRITE;
    private static final MethodHandle MH_FREE;

    static {
        String libPath = System.getProperty(
            "rocksdb.lib.path",
            "/opt/homebrew/Cellar/rocksdb/10.10.1/lib/librocksdb.dylib"
        );
        LIB = SymbolLookup.libraryLookup(libPath, Arena.global());

        MH_OPTIONS_CREATE = lookup("rocksdb_options_create",
            FunctionDescriptor.of(ValueLayout.ADDRESS));

        MH_OPTIONS_SET_CREATE_IF_MISSING = lookup("rocksdb_options_set_create_if_missing",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_BYTE));

        MH_OPTIONS_DESTROY = lookup("rocksdb_options_destroy",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

        // rocksdb_t* rocksdb_open(const rocksdb_options_t*, const char* name, char** errptr)
        MH_OPEN = lookup("rocksdb_open",
            FunctionDescriptor.of(ValueLayout.ADDRESS,
                ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));

        MH_CLOSE = lookup("rocksdb_close",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

        MH_WRITEOPTIONS_CREATE = lookup("rocksdb_writeoptions_create",
            FunctionDescriptor.of(ValueLayout.ADDRESS));

        MH_WRITEOPTIONS_DESTROY = lookup("rocksdb_writeoptions_destroy",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

        MH_READOPTIONS_CREATE = lookup("rocksdb_readoptions_create",
            FunctionDescriptor.of(ValueLayout.ADDRESS));

        MH_READOPTIONS_DESTROY = lookup("rocksdb_readoptions_destroy",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

        // void rocksdb_put(db*, wo*, key*, klen, val*, vlen, errptr**)
        MH_PUT = lookup("rocksdb_put",
            FunctionDescriptor.ofVoid(
                ValueLayout.ADDRESS, ValueLayout.ADDRESS,
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS));

        // char* rocksdb_get(db*, ro*, key*, klen, size_t* vallen, errptr**)
        MH_GET = lookup("rocksdb_get",
            FunctionDescriptor.of(ValueLayout.ADDRESS,
                ValueLayout.ADDRESS, ValueLayout.ADDRESS,
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS, ValueLayout.ADDRESS));

        // rocksdb_pinnableslice_t* rocksdb_get_pinned(db*, ro*, key*, klen, errptr**)
        MH_GET_PINNED = lookup("rocksdb_get_pinned",
            FunctionDescriptor.of(ValueLayout.ADDRESS,
                ValueLayout.ADDRESS, ValueLayout.ADDRESS,
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS));

        // void rocksdb_pinnableslice_destroy(rocksdb_pinnableslice_t*)
        MH_PINNABLESLICE_DESTROY = lookup("rocksdb_pinnableslice_destroy",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

        // const char* rocksdb_pinnableslice_value(const rocksdb_pinnableslice_t*, size_t* vlen)
        MH_PINNABLESLICE_VALUE = lookup("rocksdb_pinnableslice_value",
            FunctionDescriptor.of(ValueLayout.ADDRESS,
                ValueLayout.ADDRESS, ValueLayout.ADDRESS));

        // void rocksdb_delete(db*, wo*, key*, klen, errptr**)
        MH_DELETE = lookup("rocksdb_delete",
            FunctionDescriptor.ofVoid(
                ValueLayout.ADDRESS, ValueLayout.ADDRESS,
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS));

        // void rocksdb_write(db*, wo*, batch*, errptr**)
        MH_WRITE = lookup("rocksdb_write",
            FunctionDescriptor.ofVoid(
                ValueLayout.ADDRESS, ValueLayout.ADDRESS,
                ValueLayout.ADDRESS, ValueLayout.ADDRESS));

        MH_FREE = lookup("rocksdb_free",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));
    }

    // -----------------------------------------------------------------------
    // Thread-local pre-allocated auxiliary segments
    // Avoids per-call Arena create/destroy for the errHolder and valLenHolder.
    // Arena.ofAuto() is GC-managed; each thread gets its own segment that
    // lives as long as the thread-local reference is reachable.
    // -----------------------------------------------------------------------

    private static final ThreadLocal<MemorySegment> ERR_HOLDER = ThreadLocal.withInitial(
        () -> Arena.ofAuto().allocate(ValueLayout.ADDRESS));

    private static final ThreadLocal<MemorySegment> VAL_LEN_HOLDER = ThreadLocal.withInitial(
        () -> Arena.ofAuto().allocate(ValueLayout.JAVA_LONG));

    // -----------------------------------------------------------------------
    // Instance state
    // -----------------------------------------------------------------------

    private final MemorySegment dbPtr;
    private final MemorySegment writeOptions;
    private final MemorySegment readOptions;

    private RocksDB(MemorySegment dbPtr, MemorySegment writeOptions, MemorySegment readOptions) {
        this.dbPtr = dbPtr;
        this.writeOptions = writeOptions;
        this.readOptions = readOptions;
    }

    // -----------------------------------------------------------------------
    // Factory
    // -----------------------------------------------------------------------

    public static RocksDB open(String path) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment opts = (MemorySegment) MH_OPTIONS_CREATE.invokeExact();
            MH_OPTIONS_SET_CREATE_IF_MISSING.invokeExact(opts, (byte) 1);

            MemorySegment pathSeg = arena.allocateFrom(path);
            MemorySegment errHolder = ERR_HOLDER.get();
            errHolder.set(ValueLayout.ADDRESS, 0, MemorySegment.NULL);

            MemorySegment dbPtr = (MemorySegment) MH_OPEN.invokeExact(opts, pathSeg, errHolder);
            MH_OPTIONS_DESTROY.invokeExact(opts);
            checkError(errHolder);

            MemorySegment writeOptions = (MemorySegment) MH_WRITEOPTIONS_CREATE.invokeExact();
            MemorySegment readOptions = (MemorySegment) MH_READOPTIONS_CREATE.invokeExact();
            return new RocksDB(dbPtr, writeOptions, readOptions);
        } catch (RocksDBException e) {
            throw e;
        } catch (Throwable t) {
            throw new RocksDBException("Failed to open database: " + path, t);
        }
    }

    // -----------------------------------------------------------------------
    // Public API — byte[] variants
    // -----------------------------------------------------------------------

    public void put(byte[] key, byte[] value) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment keyNative = toNative(arena, key);
            MemorySegment valNative = toNative(arena, value);
            MemorySegment errHolder = ERR_HOLDER.get();
            errHolder.set(ValueLayout.ADDRESS, 0, MemorySegment.NULL);

            MH_PUT.invokeExact(dbPtr, writeOptions,
                keyNative, (long) key.length,
                valNative, (long) value.length,
                errHolder);
            checkError(errHolder);
        } catch (RocksDBException e) {
            throw e;
        } catch (Throwable t) {
            throw new RocksDBException("put failed", t);
        }
    }

    /**
     * Get via PinnableSlice: pins data directly from the block cache,
     * avoiding the intermediate std::string copy that rocksdb_get performs.
     */
    public byte[] get(byte[] key) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment keyNative = toNative(arena, key);
            MemorySegment errHolder = ERR_HOLDER.get();
            errHolder.set(ValueLayout.ADDRESS, 0, MemorySegment.NULL);

            MemorySegment pin = (MemorySegment) MH_GET_PINNED.invokeExact(
                dbPtr, readOptions, keyNative, (long) key.length, errHolder);
            checkError(errHolder);

            if (MemorySegment.NULL.equals(pin)) return null;

            MemorySegment valLenSeg = VAL_LEN_HOLDER.get();
            MemorySegment valPtr = (MemorySegment) MH_PINNABLESLICE_VALUE.invokeExact(pin, valLenSeg);
            long valLen = valLenSeg.get(ValueLayout.JAVA_LONG, 0);
            byte[] result = valPtr.reinterpret(valLen).toArray(ValueLayout.JAVA_BYTE);
            MH_PINNABLESLICE_DESTROY.invokeExact(pin);
            return result;
        } catch (RocksDBException e) {
            throw e;
        } catch (Throwable t) {
            throw new RocksDBException("get failed", t);
        }
    }

    public void delete(byte[] key) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment keyNative = toNative(arena, key);
            MemorySegment errHolder = ERR_HOLDER.get();
            errHolder.set(ValueLayout.ADDRESS, 0, MemorySegment.NULL);

            MH_DELETE.invokeExact(dbPtr, writeOptions,
                keyNative, (long) key.length,
                errHolder);
            checkError(errHolder);
        } catch (RocksDBException e) {
            throw e;
        } catch (Throwable t) {
            throw new RocksDBException("delete failed", t);
        }
    }

    // -----------------------------------------------------------------------
    // Public API — direct ByteBuffer variants (highest performance)
    // -----------------------------------------------------------------------

    /**
     * Zero-copy put: MemorySegment.ofBuffer() wraps the direct buffer's native
     * memory without any heap→native copy.
     */
    public void put(ByteBuffer key, ByteBuffer value) {
        MemorySegment keyNative = MemorySegment.ofBuffer(key);
        MemorySegment valNative = MemorySegment.ofBuffer(value);
        MemorySegment errHolder = ERR_HOLDER.get();
        errHolder.set(ValueLayout.ADDRESS, 0, MemorySegment.NULL);
        try {
            MH_PUT.invokeExact(dbPtr, writeOptions,
                keyNative, (long) key.remaining(),
                valNative, (long) value.remaining(),
                errHolder);
            checkError(errHolder);
        } catch (RocksDBException e) {
            throw e;
        } catch (Throwable t) {
            throw new RocksDBException("put failed", t);
        }
    }

    /**
     * Single-copy get via PinnableSlice + direct output ByteBuffer.
     * Pins data from the block cache and copies once into the caller's buffer.
     * No Arena allocation occurs on the hot path.
     * Returns the actual value length, or -1 if not found.
     */
    public int get(ByteBuffer key, ByteBuffer value) {
        MemorySegment keyNative = MemorySegment.ofBuffer(key);
        MemorySegment errHolder = ERR_HOLDER.get();
        errHolder.set(ValueLayout.ADDRESS, 0, MemorySegment.NULL);
        try {
            MemorySegment pin = (MemorySegment) MH_GET_PINNED.invokeExact(
                dbPtr, readOptions, keyNative, (long) key.remaining(), errHolder);
            checkError(errHolder);

            if (MemorySegment.NULL.equals(pin)) return -1;

            MemorySegment valLenSeg = VAL_LEN_HOLDER.get();
            MemorySegment valPtr = (MemorySegment) MH_PINNABLESLICE_VALUE.invokeExact(pin, valLenSeg);
            long valLen = valLenSeg.get(ValueLayout.JAVA_LONG, 0);
            int toCopy = (int) Math.min(valLen, value.remaining());
            MemorySegment.ofBuffer(value).copyFrom(valPtr.reinterpret(toCopy));
            value.position(value.position() + toCopy);
            MH_PINNABLESLICE_DESTROY.invokeExact(pin);
            return (int) valLen;
        } catch (RocksDBException e) {
            throw e;
        } catch (Throwable t) {
            throw new RocksDBException("get failed", t);
        }
    }

    // -----------------------------------------------------------------------
    // Batch write
    // -----------------------------------------------------------------------

    public void write(WriteBatch batch) {
        MemorySegment errHolder = ERR_HOLDER.get();
        errHolder.set(ValueLayout.ADDRESS, 0, MemorySegment.NULL);
        try {
            MH_WRITE.invokeExact(dbPtr, writeOptions, batch.ptr, errHolder);
            checkError(errHolder);
        } catch (RocksDBException e) {
            throw e;
        } catch (Throwable t) {
            throw new RocksDBException("write batch failed", t);
        }
    }

    // -----------------------------------------------------------------------
    // AutoCloseable
    // -----------------------------------------------------------------------

    @Override
    public void close() {
        try {
            MH_WRITEOPTIONS_DESTROY.invokeExact(writeOptions);
            MH_READOPTIONS_DESTROY.invokeExact(readOptions);
            MH_CLOSE.invokeExact(dbPtr);
        } catch (Throwable t) {
            throw new RocksDBException("close failed", t);
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    static MethodHandle lookup(String name, FunctionDescriptor fd) {
        return LINKER.downcallHandle(
            LIB.find(name).orElseThrow(() ->
                new UnsatisfiedLinkError("Symbol not found: " + name)),
            fd);
    }

    /** Copy byte[] into native memory without null-termination. */
    private static MemorySegment toNative(Arena arena, byte[] bytes) {
        MemorySegment seg = arena.allocate(bytes.length);
        MemorySegment.copy(bytes, 0, seg, ValueLayout.JAVA_BYTE, 0, bytes.length);
        return seg;
    }

    /** Check errHolder after a C call; throws RocksDBException if an error was set. */
    private static void checkError(MemorySegment errHolder) {
        MemorySegment errPtr = errHolder.get(ValueLayout.ADDRESS, 0);
        if (!MemorySegment.NULL.equals(errPtr)) {
            String msg = errPtr.reinterpret(Long.MAX_VALUE).getString(0);
            try {
                MH_FREE.invokeExact(errPtr);
            } catch (Throwable ignored) {
            }
            throw new RocksDBException(msg);
        }
    }
}
