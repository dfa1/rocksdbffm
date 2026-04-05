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
import java.util.Optional;
import java.util.OptionalLong;

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
    private static final MethodHandle MH_MERGE;
    private static final MethodHandle MH_DELETE_RANGE_CF;
    private static final MethodHandle MH_GET_DEFAULT_CF;
    private static final MethodHandle MH_WRITE;
    private static final MethodHandle MH_WRITEOPTIONS_CREATE;
    private static final MethodHandle MH_WRITEOPTIONS_DESTROY;
    private static final MethodHandle MH_READOPTIONS_CREATE;
    private static final MethodHandle MH_READOPTIONS_DESTROY;
    private static final MethodHandle MH_CREATE_SNAPSHOT;
    private static final MethodHandle MH_FLUSH;
    private static final MethodHandle MH_FLUSH_WAL;
    private static final MethodHandle MH_KEY_MAY_EXIST;
    private static final MethodHandle MH_PROPERTY_VALUE;
    private static final MethodHandle MH_PROPERTY_INT;
    private static final MethodHandle MH_FREE;

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

        // void rocksdb_merge(db*, wo*, key*, klen, val*, vlen, errptr**)
        MH_MERGE = lookup("rocksdb_merge",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS,
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));

        // void rocksdb_delete_range_cf(db*, wo*, cf*, start*, slen, end*, elen, errptr**)
        MH_DELETE_RANGE_CF = lookup("rocksdb_delete_range_cf",
            FunctionDescriptor.ofVoid(
                ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS,
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS));

        // rocksdb_column_family_handle_t* rocksdb_get_default_column_family_handle(db*)
        MH_GET_DEFAULT_CF = lookup("rocksdb_get_default_column_family_handle",
            FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS));

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

        // void rocksdb_flush(db*, flushopts*, errptr**)
        MH_FLUSH = lookup("rocksdb_flush",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));

        // void rocksdb_flush_wal(db*, sync, errptr**)
        MH_FLUSH_WAL = lookup("rocksdb_flush_wal",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_BYTE, ValueLayout.ADDRESS));

        // unsigned char rocksdb_key_may_exist(db*, ro*, key*, klen,
        //     char** value, size_t* val_len, timestamp*, ts_len, unsigned char* value_found)
        // value/val_len/timestamp/value_found are all passed as NULL — pure Bloom filter check.
        MH_KEY_MAY_EXIST = lookup("rocksdb_key_may_exist",
            FunctionDescriptor.of(ValueLayout.JAVA_BYTE,
                ValueLayout.ADDRESS, ValueLayout.ADDRESS,  // db*, ro*
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, // key*, klen
                ValueLayout.ADDRESS, ValueLayout.ADDRESS,  // value**, val_len*
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, // timestamp*, ts_len
                ValueLayout.ADDRESS));                     // value_found*

        // char* rocksdb_property_value(db*, propname) — returns NULL if not supported; caller frees
        MH_PROPERTY_VALUE = lookup("rocksdb_property_value",
            FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));

        // int rocksdb_property_int(db*, propname, uint64_t* out_val) — 0=ok, -1=unsupported
        MH_PROPERTY_INT = lookup("rocksdb_property_int",
            FunctionDescriptor.of(ValueLayout.JAVA_INT,
                ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));

        MH_FREE = lookup("rocksdb_free", FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));
    }

    private final MemorySegment dbPtr;
    private final MemorySegment writeOptions;
    private final MemorySegment readOptions;

    private     RocksDB(MemorySegment dbPtr, MemorySegment writeOptions, MemorySegment readOptions) {
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
            throw RocksDBException.wrap("open failed", t);
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
            throw RocksDBException.wrap("openReadOnly failed", t);
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
            throw RocksDBException.wrap("put failed", t);
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
            throw RocksDBException.wrap("get failed", t);
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
            throw RocksDBException.wrap("delete failed", t);
        }
    }

    // -----------------------------------------------------------------------
    // Public API — merge
    // -----------------------------------------------------------------------

    /** Applies a merge operand to {@code key}. Slow path: copies key/value. */
    public void merge(byte[] key, byte[] value) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment err = Native.errHolder(arena);
            MH_MERGE.invokeExact(dbPtr, writeOptions,
                Native.toNative(arena, key),   (long) key.length,
                Native.toNative(arena, value), (long) value.length,
                err);
            Native.checkError(err);
        } catch (Throwable t) {
            throw RocksDBException.wrap("merge failed", t);
        }
    }

    /** Applies a merge operand to {@code key}. Zero-copy for direct {@link java.nio.ByteBuffer}s. */
    public void merge(ByteBuffer key, ByteBuffer value) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment err = Native.errHolder(arena);
            MH_MERGE.invokeExact(dbPtr, writeOptions,
                MemorySegment.ofBuffer(key),   (long) key.remaining(),
                MemorySegment.ofBuffer(value), (long) value.remaining(),
                err);
            Native.checkError(err);
        } catch (Throwable t) {
            throw RocksDBException.wrap("merge failed", t);
        }
    }

    /** Applies a merge operand to {@code key}. Zero-copy native-first path. */
    public void merge(MemorySegment key, MemorySegment value) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment err = Native.errHolder(arena);
            MH_MERGE.invokeExact(dbPtr, writeOptions,
                key,   key.byteSize(),
                value, value.byteSize(),
                err);
            Native.checkError(err);
        } catch (Throwable t) {
            throw RocksDBException.wrap("merge failed", t);
        }
    }

    // -----------------------------------------------------------------------
    // Public API — deleteRange (range tombstone)
    // -----------------------------------------------------------------------

    /**
     * Deletes all keys in the half-open range [{@code startKey}, {@code endKey}).
     * Uses the default column family via {@code rocksdb_delete_range_cf}.
     * Slow path: copies keys into native memory.
     */
    public void deleteRange(byte[] startKey, byte[] endKey) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment err = Native.errHolder(arena);
            MemorySegment cf = (MemorySegment) MH_GET_DEFAULT_CF.invokeExact(dbPtr);
            MH_DELETE_RANGE_CF.invokeExact(dbPtr, writeOptions, cf,
                Native.toNative(arena, startKey), (long) startKey.length,
                Native.toNative(arena, endKey),   (long) endKey.length,
                err);
            Native.checkError(err);
        } catch (Throwable t) {
            throw RocksDBException.wrap("deleteRange failed", t);
        }
    }

    /**
     * Deletes all keys in the half-open range [{@code startKey}, {@code endKey}).
     * Zero-copy for direct {@link java.nio.ByteBuffer}s.
     */
    public void deleteRange(ByteBuffer startKey, ByteBuffer endKey) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment err = Native.errHolder(arena);
            MemorySegment cf = (MemorySegment) MH_GET_DEFAULT_CF.invokeExact(dbPtr);
            MH_DELETE_RANGE_CF.invokeExact(dbPtr, writeOptions, cf,
                MemorySegment.ofBuffer(startKey), (long) startKey.remaining(),
                MemorySegment.ofBuffer(endKey),   (long) endKey.remaining(),
                err);
            Native.checkError(err);
        } catch (Throwable t) {
            throw RocksDBException.wrap("deleteRange failed", t);
        }
    }

    /**
     * Deletes all keys in the half-open range [{@code startKey}, {@code endKey}).
     * Zero-copy native-first path.
     */
    public void deleteRange(MemorySegment startKey, MemorySegment endKey) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment err = Native.errHolder(arena);
            MemorySegment cf = (MemorySegment) MH_GET_DEFAULT_CF.invokeExact(dbPtr);
            MH_DELETE_RANGE_CF.invokeExact(dbPtr, writeOptions, cf,
                startKey, startKey.byteSize(),
                endKey,   endKey.byteSize(),
                err);
            Native.checkError(err);
        } catch (Throwable t) {
            throw RocksDBException.wrap("deleteRange failed", t);
        }
    }

    // -----------------------------------------------------------------------
    // Public API — keyMayExist (Bloom filter check)
    // -----------------------------------------------------------------------

    /**
     * Returns {@code false} if the key definitely does not exist in the database,
     * allowing callers to skip an expensive {@link #get} call entirely.
     * A return value of {@code true} means the key <em>may</em> exist and a full
     * read is needed to confirm.
     *
     * <p>Slow path: copies the key into native memory.
     */
    public boolean keyMayExist(byte[] key) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment k = Native.toNative(arena, key);
            return keyMayExistNative(readOptions, k, key.length);
        } catch (Throwable t) {
            throw RocksDBException.wrap("keyMayExist failed", t);
        }
    }

    /**
     * {@link #keyMayExist(byte[])} with explicit {@link ReadOptions}, e.g. for
     * snapshot-pinned checks. Slow path: copies the key into native memory.
     */
    public boolean keyMayExist(ReadOptions readOptions, byte[] key) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment k = Native.toNative(arena, key);
            return keyMayExistNative(readOptions.ptr, k, key.length);
        } catch (Throwable t) {
            throw RocksDBException.wrap("keyMayExist failed", t);
        }
    }

    /**
     * {@link #keyMayExist(byte[])} for direct {@link java.nio.ByteBuffer}s. Zero-copy.
     */
    public boolean keyMayExist(ByteBuffer key) {
        try {
            return keyMayExistNative(readOptions, MemorySegment.ofBuffer(key), key.remaining());
        } catch (Throwable t) {
            throw RocksDBException.wrap("keyMayExist failed", t);
        }
    }

    /**
     * {@link #keyMayExist(byte[])} for {@link java.lang.foreign.MemorySegment}s. Zero-copy.
     */
    public boolean keyMayExist(MemorySegment key) {
        try {
            return keyMayExistNative(readOptions, key, (int) key.byteSize());
        } catch (Throwable t) {
            throw RocksDBException.wrap("keyMayExist failed", t);
        }
    }

    private boolean keyMayExistNative(MemorySegment roPtr, MemorySegment key, long keyLen)
            throws Throwable {
        return ((byte) MH_KEY_MAY_EXIST.invokeExact(
            dbPtr, roPtr,
            key, keyLen,
            MemorySegment.NULL, MemorySegment.NULL,  // no value output
            MemorySegment.NULL, 0L,                  // no timestamp
            MemorySegment.NULL                       // no value_found output
        )) != 0;
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
            throw RocksDBException.wrap("put failed", t);
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
            throw RocksDBException.wrap("get failed", t);
        }
    }

    // -----------------------------------------------------------------------
    // Flush
    // -----------------------------------------------------------------------

    /**
     * Flushes all memtable data to SST files on disk.
     * Blocks until the flush completes when {@link FlushOptions#isWait()} is {@code true}.
     */
    public void flush(FlushOptions flushOptions) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment err = Native.errHolder(arena);
            MH_FLUSH.invokeExact(dbPtr, flushOptions.ptr, err);
            Native.checkError(err);
        } catch (Throwable t) {
            throw RocksDBException.wrap("flush failed", t);
        }
    }

    /**
     * Flushes the WAL (write-ahead log) to disk.
     *
     * @param sync if {@code true}, performs an {@code fsync} after writing; otherwise
     *             only guarantees the data has been written to the OS buffer
     */
    public void flushWal(boolean sync) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment err = Native.errHolder(arena);
            MH_FLUSH_WAL.invokeExact(dbPtr, sync ? (byte) 1 : (byte) 0, err);
            Native.checkError(err);
        } catch (Throwable t) {
            throw RocksDBException.wrap("flushWal failed", t);
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
            throw RocksDBException.wrap("getSnapshot failed", t);
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
            throw RocksDBException.wrap("get failed", t);
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
    // DB Properties
    // -----------------------------------------------------------------------

    /**
     * Returns the value of a DB property as a string, or {@link Optional#empty()}
     * if the property is not supported by this DB instance.
     * The returned string is copied from native memory; no manual freeing is needed.
     */
    public Optional<String> getProperty(Property property) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment propSeg = arena.allocateFrom(property.propertyName());
            MemorySegment result = (MemorySegment) MH_PROPERTY_VALUE.invokeExact(dbPtr, propSeg);
            if (MemorySegment.NULL.equals(result)) return Optional.empty();
            String value = result.reinterpret(Long.MAX_VALUE).getString(0);
            MH_FREE.invokeExact(result);
            return Optional.of(value);
        } catch (Throwable t) {
            throw RocksDBException.wrap("getProperty failed", t);
        }
    }

    /**
     * Returns the value of a numeric DB property, or {@link OptionalLong#empty()}
     * if the property is not supported or is not numeric.
     */
    public OptionalLong getLongProperty(Property property) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment propSeg = arena.allocateFrom(property.propertyName());
            MemorySegment out = arena.allocate(ValueLayout.JAVA_LONG);
            int rc = (int) MH_PROPERTY_INT.invokeExact(dbPtr, propSeg, out);
            if (rc != 0) return OptionalLong.empty();
            return OptionalLong.of(out.get(ValueLayout.JAVA_LONG, 0));
        } catch (Throwable t) {
            throw RocksDBException.wrap("getLongProperty failed", t);
        }
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
            throw RocksDBException.wrap("write failed", t);
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
