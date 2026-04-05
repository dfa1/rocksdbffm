package io.github.dfa1.rocksdbffm;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.ByteBuffer;
import java.nio.file.Path;

/**
 * Main entry point for RocksDB operations.
 *
 * <p>All native calls that can fail are wrapped in the standardized Arena pattern:
 * <pre>{@code
 * try (Arena arena = Arena.ofConfined()) {
 *     MemorySegment err = Native.errHolder(arena);
 *     MH_CALL.invokeExact(..., err);
 *     Native.checkError(err);
 * }
 * }</pre>
 */
public final class RocksDB implements AutoCloseable {

    private static final Linker LINKER = Linker.nativeLinker();
    private static final SymbolLookup LIB;

    private static final MethodHandle MH_OPEN;
    private static final MethodHandle MH_OPEN_FOR_READ_ONLY;
    private static final MethodHandle MH_CLOSE;
    private static final MethodHandle MH_PUT;
    private static final MethodHandle MH_GET_PINNED;
    private static final MethodHandle MH_PINNABLESLICE_VALUE;
    private static final MethodHandle MH_PINNABLESLICE_DESTROY;
    private static final MethodHandle MH_DELETE;
    private static final MethodHandle MH_WRITE;
    private static final MethodHandle MH_WRITEOPTIONS_CREATE;
    private static final MethodHandle MH_WRITEOPTIONS_DESTROY;
    private static final MethodHandle MH_READOPTIONS_CREATE;
    private static final MethodHandle MH_READOPTIONS_DESTROY;
    private static final MethodHandle MH_CREATE_SNAPSHOT;

    static {
        String libPath = System.getProperty(
            "rocksdb.lib.path",
            "/opt/homebrew/Cellar/rocksdb/10.10.1/lib/librocksdb.dylib"
        );
        LIB = SymbolLookup.libraryLookup(libPath, Arena.ofAuto());

        MH_OPEN = lookup("rocksdb_open",
            FunctionDescriptor.of(ValueLayout.ADDRESS,
                ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));

        MH_OPEN_FOR_READ_ONLY = lookup("rocksdb_open_for_read_only",
            FunctionDescriptor.of(ValueLayout.ADDRESS,
                ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_BYTE, ValueLayout.ADDRESS));

        MH_CLOSE = lookup("rocksdb_close",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

        MH_PUT = lookup("rocksdb_put",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS,
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));

        MH_GET_PINNED = lookup("rocksdb_get_pinned",
            FunctionDescriptor.of(ValueLayout.ADDRESS,
                ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));

        MH_PINNABLESLICE_VALUE = lookup("rocksdb_pinnableslice_value",
            FunctionDescriptor.of(ValueLayout.ADDRESS,
                ValueLayout.ADDRESS, ValueLayout.ADDRESS));

        MH_PINNABLESLICE_DESTROY = lookup("rocksdb_pinnableslice_destroy",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

        MH_DELETE = lookup("rocksdb_delete",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS,
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));

        MH_WRITE = lookup("rocksdb_write",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS,
                ValueLayout.ADDRESS, ValueLayout.ADDRESS));

        MH_WRITEOPTIONS_CREATE = lookup("rocksdb_writeoptions_create",
            FunctionDescriptor.of(ValueLayout.ADDRESS));

        MH_WRITEOPTIONS_DESTROY = lookup("rocksdb_writeoptions_destroy",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

        MH_READOPTIONS_CREATE = lookup("rocksdb_readoptions_create",
            FunctionDescriptor.of(ValueLayout.ADDRESS));

        MH_READOPTIONS_DESTROY = lookup("rocksdb_readoptions_destroy",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

        // const rocksdb_snapshot_t* rocksdb_create_snapshot(db*)
        MH_CREATE_SNAPSHOT = lookup("rocksdb_create_snapshot",
            FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS));
    }

    private final MemorySegment dbPtr;
    private final MemorySegment writeOptions;
    private final MemorySegment readOptions;

    private RocksDB(MemorySegment dbPtr, MemorySegment writeOptions, MemorySegment readOptions) {
        this.dbPtr = dbPtr;
        this.writeOptions = writeOptions;
        this.readOptions = readOptions;
    }

    /** Returns the underlying native handle. For internal and TransactionDB use. */
    MemorySegment ptr() {
        return dbPtr;
    }

    /**
     * Opens a database at the specified path.
     * Use {@link Options#setCreateIfMissing(boolean)} to control behavior if
     * the path does not exist.
     */
    public static RocksDB open(Options options, Path path) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment err = Native.errHolder(arena);
            MemorySegment pathSeg = arena.allocateFrom(path.toString());
            MemorySegment dbPtr = (MemorySegment) MH_OPEN.invokeExact(
                options.ptr, pathSeg, err);

            Native.checkError(err);

            MemorySegment writeOptions = (MemorySegment) MH_WRITEOPTIONS_CREATE.invokeExact();
            MemorySegment readOptions = (MemorySegment) MH_READOPTIONS_CREATE.invokeExact();
            return new RocksDB(dbPtr, writeOptions, readOptions);
        } catch (Throwable t) {
            throw (t instanceof RocksDBException r) ? r : new RocksDBException("open failed", t);
        }
    }

    /** Equivalent to {@code open(options, path)} with default options. */
    public static RocksDB open(Path path) {
        try (Options opts = new Options().setCreateIfMissing(true)) {
            return open(opts, path);
        }
    }

    /**
     * Opens the database at {@code path} in read-only mode.
     * Write operations on the returned instance will throw {@link RocksDBException}.
     *
     * @param errorIfWalFileExists if true, fails when unrecovered WAL files are present
     */
    public static RocksDB openReadOnly(Options options, Path path, boolean errorIfWalFileExists) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment err = Native.errHolder(arena);
            MemorySegment pathSeg = arena.allocateFrom(path.toString());
            MemorySegment dbPtr = (MemorySegment) MH_OPEN_FOR_READ_ONLY.invokeExact(
                options.ptr, pathSeg, errorIfWalFileExists ? (byte) 1 : (byte) 0, err);

            Native.checkError(err);

            MemorySegment writeOptions = (MemorySegment) MH_WRITEOPTIONS_CREATE.invokeExact();
            MemorySegment readOptions = (MemorySegment) MH_READOPTIONS_CREATE.invokeExact();
            return new RocksDB(dbPtr, writeOptions, readOptions);
        } catch (Throwable t) {
            throw (t instanceof RocksDBException r) ? r : new RocksDBException("openReadOnly failed", t);
        }
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
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment err = Native.errHolder(arena);
            MemorySegment keyNative = Native.toNative(arena,key);
            MemorySegment valNative = Native.toNative(arena,value);

            MH_PUT.invokeExact(dbPtr, writeOptions,
                keyNative, (long) key.length,
                valNative, (long) value.length,
                err);

            Native.checkError(err);
        } catch (Throwable t) {
            throw (t instanceof RocksDBException r) ? r : new RocksDBException("put failed", t);
        }
    }

    /**
     * Get via PinnableSlice: pins data directly from the block cache,
     * avoiding the intermediate std::string copy that rocksdb_get performs.
     */
    public byte[] get(byte[] key) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment err = Native.errHolder(arena);
            MemorySegment keyNative = Native.toNative(arena,key);

            MemorySegment pin = (MemorySegment) MH_GET_PINNED.invokeExact(
                dbPtr, readOptions, keyNative, (long) key.length, err);

            Native.checkError(err);

            if (MemorySegment.NULL.equals(pin)) return null;

            MemorySegment valLenSeg = arena.allocate(ValueLayout.JAVA_LONG);
            MemorySegment valPtr = (MemorySegment) MH_PINNABLESLICE_VALUE.invokeExact(pin, valLenSeg);
            long valLen = valLenSeg.get(ValueLayout.JAVA_LONG, 0);
            byte[] result = valPtr.reinterpret(valLen).toArray(ValueLayout.JAVA_BYTE);
            MH_PINNABLESLICE_DESTROY.invokeExact(pin);
            return result;
        } catch (Throwable t) {
            throw (t instanceof RocksDBException r) ? r : new RocksDBException("get failed", t);
        }
    }

    public void delete(byte[] key) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment err = Native.errHolder(arena);
            MemorySegment keyNative = Native.toNative(arena,key);

            MH_DELETE.invokeExact(dbPtr, writeOptions,
                keyNative, (long) key.length,
                err);

            Native.checkError(err);
        } catch (Throwable t) {
            throw (t instanceof RocksDBException r) ? r : new RocksDBException("delete failed", t);
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
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment err = Native.errHolder(arena);
            MemorySegment keyNative = MemorySegment.ofBuffer(key);
            MemorySegment valNative = MemorySegment.ofBuffer(value);
            MH_PUT.invokeExact(dbPtr, writeOptions,
                keyNative, (long) key.remaining(),
                valNative, (long) value.remaining(),
                err);
            Native.checkError(err);
        } catch (Throwable t) {
            throw (t instanceof RocksDBException r) ? r : new RocksDBException("put failed", t);
        }
    }

    /**
     * Single-copy get via PinnableSlice + direct output ByteBuffer.
     * Pins data from the block cache and copies once into the caller's buffer.
     * No Arena allocation occurs on the hot path.
     * Returns the actual value length, or -1 if not found.
     */
    public int get(ByteBuffer key, ByteBuffer value) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment err = Native.errHolder(arena);
            MemorySegment keyNative = MemorySegment.ofBuffer(key);
            MemorySegment pin = (MemorySegment) MH_GET_PINNED.invokeExact(
                dbPtr, readOptions, keyNative, (long) key.remaining(), err);

            Native.checkError(err);

            if (MemorySegment.NULL.equals(pin)) return -1;

            MemorySegment valLenSeg = arena.allocate(ValueLayout.JAVA_LONG);
            MemorySegment valPtr = (MemorySegment) MH_PINNABLESLICE_VALUE.invokeExact(pin, valLenSeg);
            long valLen = valLenSeg.get(ValueLayout.JAVA_LONG, 0);
            int toCopy = (int) Math.min(valLen, value.remaining());
            MemorySegment.ofBuffer(value).copyFrom(valPtr.reinterpret(toCopy));
            value.position(value.position() + toCopy);
            MH_PINNABLESLICE_DESTROY.invokeExact(pin);
            return (int) valLen;
        } catch (Throwable t) {
            throw (t instanceof RocksDBException r) ? r : new RocksDBException("get failed", t);
        }
    }

    // -----------------------------------------------------------------------
    // Snapshot
    // -----------------------------------------------------------------------

    /**
     * Creates a snapshot of the current DB state.
     * The returned snapshot must be closed after use to release native resources.
     */
    public Snapshot getSnapshot() {
        try {
            MemorySegment snapPtr = (MemorySegment) MH_CREATE_SNAPSHOT.invokeExact(dbPtr);
            return new Snapshot(dbPtr, snapPtr);
        } catch (Throwable t) {
            throw (t instanceof RocksDBException r) ? r : new RocksDBException("getSnapshot failed", t);
        }
    }

    /**
     * Get with explicit ReadOptions, e.g. for snapshot-pinned reads.
     * Uses PinnableSlice to avoid intermediate copies.
     */
    public byte[] get(ReadOptions readOptions, byte[] key) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment err = Native.errHolder(arena);
            MemorySegment keyNative = Native.toNative(arena, key);

            MemorySegment pin = (MemorySegment) MH_GET_PINNED.invokeExact(
                dbPtr, readOptions.ptr, keyNative, (long) key.length, err);

            Native.checkError(err);

            if (MemorySegment.NULL.equals(pin)) return null;

            MemorySegment valLenSeg = arena.allocate(ValueLayout.JAVA_LONG);
            MemorySegment valPtr = (MemorySegment) MH_PINNABLESLICE_VALUE.invokeExact(pin, valLenSeg);
            long valLen = valLenSeg.get(ValueLayout.JAVA_LONG, 0);
            byte[] result = valPtr.reinterpret(valLen).toArray(ValueLayout.JAVA_BYTE);
            MH_PINNABLESLICE_DESTROY.invokeExact(pin);
            return result;
        } catch (Throwable t) {
            throw (t instanceof RocksDBException r) ? r : new RocksDBException("get failed", t);
        }
    }

    // -----------------------------------------------------------------------
    // Iterator
    // -----------------------------------------------------------------------

    /** Returns a new iterator using the database's default read options. */
    public RocksIterator newIterator() {
        return RocksIterator.create(dbPtr, readOptions);
    }

    /** Returns a new iterator using the supplied {@code readOptions}. */
    public RocksIterator newIterator(ReadOptions readOptions) {
        return RocksIterator.create(dbPtr, readOptions.ptr);
    }

    // -----------------------------------------------------------------------
    // Batch write
    // -----------------------------------------------------------------------

    public void write(WriteBatch batch) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment err = Native.errHolder(arena);
            MH_WRITE.invokeExact(dbPtr, writeOptions, batch.ptr, err);
            Native.checkError(err);
        } catch (Throwable t) {
            throw (t instanceof RocksDBException r) ? r : new RocksDBException("write failed", t);
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

}
