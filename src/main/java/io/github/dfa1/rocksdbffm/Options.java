package io.github.dfa1.rocksdbffm;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;

/**
 * FFM wrapper for rocksdb_options_t.
 *
 * <p>Usage:
 * <pre>{@code
 * try (Options opts = new Options().setCreateIfMissing(true)) {
 *     RocksDB db = RocksDB.open(opts, path);
 * }
 * }</pre>
 *
 * <p>Note: the Options object must remain open until after RocksDB.open() returns;
 * it can be closed immediately after that call.
 */
public final class Options implements AutoCloseable {

    static final MethodHandle MH_CREATE;
    static final MethodHandle MH_DESTROY;
    static final MethodHandle MH_SET_CREATE_IF_MISSING;
    static final MethodHandle MH_GET_CREATE_IF_MISSING;
    private static final MethodHandle MH_SET_BLOCK_BASED_TABLE_FACTORY;

    static {
        MH_CREATE = RocksDB.lookup("rocksdb_options_create",
            FunctionDescriptor.of(ValueLayout.ADDRESS));

        MH_DESTROY = RocksDB.lookup("rocksdb_options_destroy",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

        MH_SET_CREATE_IF_MISSING = RocksDB.lookup("rocksdb_options_set_create_if_missing",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_BYTE));

        MH_GET_CREATE_IF_MISSING = RocksDB.lookup("rocksdb_options_get_create_if_missing",
            FunctionDescriptor.of(ValueLayout.JAVA_BYTE, ValueLayout.ADDRESS));

        // void rocksdb_options_set_block_based_table_factory(opts*, block_based_table_options_t*)
        MH_SET_BLOCK_BASED_TABLE_FACTORY = RocksDB.lookup(
            "rocksdb_options_set_block_based_table_factory",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS));
    }

    /** Package-private: accessed by RocksDB.open(). */
    final MemorySegment ptr;

    public Options() {
        try {
            this.ptr = (MemorySegment) MH_CREATE.invokeExact();
        } catch (Throwable t) {
            throw new RocksDBException("options create failed", t);
        }
    }

    /**
     * If true, the database will be created if it does not already exist.
     * Default: false (same as RocksDB C++ default).
     */
    public Options setCreateIfMissing(boolean value) {
        try {
            MH_SET_CREATE_IF_MISSING.invokeExact(ptr, value ? (byte) 1 : (byte) 0);
        } catch (Throwable t) {
            throw new RocksDBException("setCreateIfMissing failed", t);
        }
        return this;
    }

    public boolean getCreateIfMissing() {
        try {
            return ((byte) MH_GET_CREATE_IF_MISSING.invokeExact(ptr)) != 0;
        } catch (Throwable t) {
            throw new RocksDBException("getCreateIfMissing failed", t);
        }
    }

    /**
     * Configures block-based table format for this DB.
     * RocksDB copies the config internally; {@code tableConfig} may be closed after this call.
     */
    public Options setTableFormatConfig(BlockBasedTableConfig tableConfig) {
        try {
            MH_SET_BLOCK_BASED_TABLE_FACTORY.invokeExact(ptr, tableConfig.ptr);
        } catch (Throwable t) {
            throw new RocksDBException("setTableFormatConfig failed", t);
        }
        return this;
    }

    @Override
    public void close() {
        try {
            MH_DESTROY.invokeExact(ptr);
        } catch (Throwable t) {
            throw new RocksDBException("options destroy failed", t);
        }
    }
}
