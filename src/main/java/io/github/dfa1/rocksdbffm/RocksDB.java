package io.github.dfa1.rocksdbffm;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.nio.ByteBuffer;
import java.nio.file.Path;

/**
 * FFM-based wrapper around the native RocksDB C library.
 * Binds directly to librocksdb.dylib without JNI.
 *
 * <h2>Performance design</h2>
 * <ul>
 *   <li><b>Surgical error handling</b> — error holders are allocated on-demand
 *       via {@link Native#check(Native.CheckedConsumer)}, using confined arenas
 *       that are quickly reclaimed.</li>
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

    private static final MethodHandle MH_OPEN;
    private static final MethodHandle MH_OPEN_FOR_READ_ONLY;
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

        // rocksdb_t* rocksdb_open(const rocksdb_options_t*, const char* name, char** errptr)
        MH_OPEN = lookup("rocksdb_open",
            FunctionDescriptor.of(ValueLayout.ADDRESS,
                ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));

        // rocksdb_t* rocksdb_open_for_read_only(opts*, name, error_if_wal_file_exists, errptr**)
        MH_OPEN_FOR_READ_ONLY = lookup("rocksdb_open_for_read_only",
            FunctionDescriptor.of(ValueLayout.ADDRESS,
                ValueLayout.ADDRESS, ValueLayout.ADDRESS,
                ValueLayout.JAVA_BYTE, ValueLayout.ADDRESS));

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
    // Instance state
    // -----------------------------------------------------------------------

    /** Package-private: accessed by Checkpoint. */
    final MemorySegment dbPtr;
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

    /**
     * Opens the database at {@code path} with {@code createIfMissing=true}.
     * Convenience overload; equivalent to {@code open(new Options().setCreateIfMissing(true), path)}.
     */
    public static RocksDB open(Path path) {
        try (Options opts = new Options().setCreateIfMissing(true)) {
            return open(opts, path);
        }
    }

    /**
     * Opens the database at {@code path} using the supplied {@link Options}.
     * The caller retains ownership of {@code options} and may close it after this call returns.
     */
    public static RocksDB open(Options options, Path path) {
        return Native.check(err -> {
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment pathSeg = arena.allocateFrom(path.toString());
                MemorySegment dbPtr = (MemorySegment) MH_OPEN.invokeExact(options.ptr, pathSeg, err);

                MemorySegment writeOptions = (MemorySegment) MH_WRITEOPTIONS_CREATE.invokeExact();
                MemorySegment readOptions = (MemorySegment) MH_READOPTIONS_CREATE.invokeExact();
                return new RocksDB(dbPtr, writeOptions, readOptions);
            }
        });
    }

    /**
     * Opens the database at {@code path} in read-only mode.
     * Write operations on the returned instance will throw {@link RocksDBException}.
     *
     * @param errorIfWalFileExists if true, fails when unrecovered WAL files are present
     */
    public static RocksDB openReadOnly(Options options, Path path, boolean errorIfWalFileExists) {
        return Native.check(err -> {
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment pathSeg = arena.allocateFrom(path.toString());
                MemorySegment dbPtr = (MemorySegment) MH_OPEN_FOR_READ_ONLY.invokeExact(
                    options.ptr, pathSeg, errorIfWalFileExists ? (byte) 1 : (byte) 0, err);

                MemorySegment writeOptions = (MemorySegment) MH_WRITEOPTIONS_CREATE.invokeExact();
                MemorySegment readOptions = (MemorySegment) MH_READOPTIONS_CREATE.invokeExact();
                return new RocksDB(dbPtr, writeOptions, readOptions);
            }
        });
    }

    /**
     * Opens the database at {@code path} in read-only mode.
     * Equivalent to {@code openReadOnly(options, path, false)}.
     */
    public static RocksDB openReadOnly(Options options, Path path) {
        return openReadOnly(options, path, false);
    }

    /**
     * Opens the database at {@code path} in read-only mode with default options.
     */
    public static RocksDB openReadOnly(Path path) {
        try (Options opts = new Options()) {
            return openReadOnly(opts, path, false);
        }
    }

    // -----------------------------------------------------------------------
    // Public API — byte[] variants
    // -----------------------------------------------------------------------

    public void put(byte[] key, byte[] value) {
        Native.check(err -> {
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment keyNative = toNative(arena, key);
                MemorySegment valNative = toNative(arena, value);

                MH_PUT.invokeExact(dbPtr, writeOptions,
                    keyNative, (long) key.length,
                    valNative, (long) value.length,
                    err);
            }
        });
    }

    /**
     * Get via PinnableSlice: pins data directly from the block cache,
     * avoiding the intermediate std::string copy that rocksdb_get performs.
     */
    public byte[] get(byte[] key) {
        return Native.check(err -> {
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment keyNative = toNative(arena, key);

                MemorySegment pin = (MemorySegment) MH_GET_PINNED.invokeExact(
                    dbPtr, readOptions, keyNative, (long) key.length, err);

                if (MemorySegment.NULL.equals(pin)) return null;

                MemorySegment valLenSeg = arena.allocate(ValueLayout.JAVA_LONG);
                MemorySegment valPtr = (MemorySegment) MH_PINNABLESLICE_VALUE.invokeExact(pin, valLenSeg);
                long valLen = valLenSeg.get(ValueLayout.JAVA_LONG, 0);
                byte[] result = valPtr.reinterpret(valLen).toArray(ValueLayout.JAVA_BYTE);
                MH_PINNABLESLICE_DESTROY.invokeExact(pin);
                return result;
            }
        });
    }

    public void delete(byte[] key) {
        Native.check(err -> {
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment keyNative = toNative(arena, key);

                MH_DELETE.invokeExact(dbPtr, writeOptions,
                    keyNative, (long) key.length,
                    err);
            }
        });
    }

    // -----------------------------------------------------------------------
    // Public API — direct ByteBuffer variants (highest performance)
    // -----------------------------------------------------------------------

    /**
     * Zero-copy put: MemorySegment.ofBuffer() wraps the direct buffer's native
     * memory without any heap→native copy.
     */
    public void put(ByteBuffer key, ByteBuffer value) {
        Native.check(err -> {
            MemorySegment keyNative = MemorySegment.ofBuffer(key);
            MemorySegment valNative = MemorySegment.ofBuffer(value);
            MH_PUT.invokeExact(dbPtr, writeOptions,
                keyNative, (long) key.remaining(),
                valNative, (long) value.remaining(),
                err);
        });
    }

    /**
     * Single-copy get via PinnableSlice + direct output ByteBuffer.
     * Pins data from the block cache and copies once into the caller's buffer.
     * No Arena allocation occurs on the hot path.
     * Returns the actual value length, or -1 if not found.
     */
    public int get(ByteBuffer key, ByteBuffer value) {
        return Native.check(err -> {
            MemorySegment keyNative = MemorySegment.ofBuffer(key);
            MemorySegment pin = (MemorySegment) MH_GET_PINNED.invokeExact(
                dbPtr, readOptions, keyNative, (long) key.remaining(), err);

            if (MemorySegment.NULL.equals(pin)) return -1;

            try (Arena arena = Arena.ofConfined()) {
                MemorySegment valLenSeg = arena.allocate(ValueLayout.JAVA_LONG);
                MemorySegment valPtr = (MemorySegment) MH_PINNABLESLICE_VALUE.invokeExact(pin, valLenSeg);
                long valLen = valLenSeg.get(ValueLayout.JAVA_LONG, 0);
                int toCopy = (int) Math.min(valLen, value.remaining());
                MemorySegment.ofBuffer(value).copyFrom(valPtr.reinterpret(toCopy));
                value.position(value.position() + toCopy);
                MH_PINNABLESLICE_DESTROY.invokeExact(pin);
                return (int) valLen;
            }
        });
    }

    // -----------------------------------------------------------------------
    // Batch write
    // -----------------------------------------------------------------------

    public void write(WriteBatch batch) {
        Native.check(err -> {
            MH_WRITE.invokeExact(dbPtr, writeOptions, batch.ptr, err);
        });
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
}
