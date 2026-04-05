package com.example.ffm;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.nio.ByteBuffer;

/**
 * FFM-based wrapper around the native RocksDB C library.
 * Binds directly to librocksdb.dylib without JNI.
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
    private static final MethodHandle MH_DELETE;
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

        // void rocksdb_delete(db*, wo*, key*, klen, errptr**)
        MH_DELETE = lookup("rocksdb_delete",
            FunctionDescriptor.ofVoid(
                ValueLayout.ADDRESS, ValueLayout.ADDRESS,
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS));

        MH_FREE = lookup("rocksdb_free",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));
    }

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
            MemorySegment errHolder = arena.allocate(ValueLayout.ADDRESS);
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
    // Public API
    // -----------------------------------------------------------------------

    public void put(byte[] key, byte[] value) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment keyNative = toNative(arena, key);
            MemorySegment valNative = toNative(arena, value);
            MemorySegment errHolder = arena.allocate(ValueLayout.ADDRESS);
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

    public byte[] get(byte[] key) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment keyNative = toNative(arena, key);
            MemorySegment valLenHolder = arena.allocate(ValueLayout.JAVA_LONG);
            MemorySegment errHolder = arena.allocate(ValueLayout.ADDRESS);
            errHolder.set(ValueLayout.ADDRESS, 0, MemorySegment.NULL);

            MemorySegment result = (MemorySegment) MH_GET.invokeExact(
                dbPtr, readOptions,
                keyNative, (long) key.length,
                valLenHolder, errHolder);
            checkError(errHolder);

            if (MemorySegment.NULL.equals(result)) {
                return null;
            }

            long valLen = valLenHolder.get(ValueLayout.JAVA_LONG, 0);
            byte[] value = result.reinterpret(valLen).toArray(ValueLayout.JAVA_BYTE);
            MH_FREE.invokeExact(result);
            return value;
        } catch (RocksDBException e) {
            throw e;
        } catch (Throwable t) {
            throw new RocksDBException("get failed", t);
        }
    }

    /** Put using direct ByteBuffers — zero-copy path from position to limit. */
    public void put(ByteBuffer key, ByteBuffer value) {
        MemorySegment keyNative = MemorySegment.ofBuffer(key);
        MemorySegment valNative = MemorySegment.ofBuffer(value);
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment errHolder = arena.allocate(ValueLayout.ADDRESS);
            errHolder.set(ValueLayout.ADDRESS, 0, MemorySegment.NULL);
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
     * Get using direct ByteBuffers — copies result into value buffer.
     * Returns the actual value length, or -1 if not found.
     */
    public int get(ByteBuffer key, ByteBuffer value) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment keyNative = MemorySegment.ofBuffer(key);
            MemorySegment valLenHolder = arena.allocate(ValueLayout.JAVA_LONG);
            MemorySegment errHolder = arena.allocate(ValueLayout.ADDRESS);
            errHolder.set(ValueLayout.ADDRESS, 0, MemorySegment.NULL);

            MemorySegment result = (MemorySegment) MH_GET.invokeExact(
                dbPtr, readOptions,
                keyNative, (long) key.remaining(),
                valLenHolder, errHolder);
            checkError(errHolder);

            if (MemorySegment.NULL.equals(result)) return -1;

            long valLen = valLenHolder.get(ValueLayout.JAVA_LONG, 0);
            int toCopy = (int) Math.min(valLen, value.remaining());
            MemorySegment.ofBuffer(value).copyFrom(result.reinterpret(toCopy));
            value.position(value.position() + toCopy);
            MH_FREE.invokeExact(result);
            return (int) valLen;
        } catch (RocksDBException e) {
            throw e;
        } catch (Throwable t) {
            throw new RocksDBException("get failed", t);
        }
    }

    public void delete(byte[] key) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment keyNative = toNative(arena, key);
            MemorySegment errHolder = arena.allocate(ValueLayout.ADDRESS);
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

    private static MethodHandle lookup(String name, FunctionDescriptor fd) {
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
