package io.github.dfa1.rocksdbffm;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.nio.file.Path;

/**
 * FFM wrapper for rocksdb_checkpoint_t.
 *
 * <p>A checkpoint is a consistent, point-in-time snapshot of the database
 * written to a new directory. The resulting directory is a valid RocksDB
 * database that can be opened read-only without any recovery.
 *
 * <pre>{@code
 * try (var db = RocksDB.open(dir);
 *      var cp = Checkpoint.create(db)) {
 *
 *     cp.exportTo(checkpointDir1);
 *     db.put("k".getBytes(), "v2".getBytes());
 *     cp.exportTo(checkpointDir2);  // second checkpoint, later state
 * }
 *
 * // Each checkpoint directory is an independent read-only database
 * try (var snap = RocksDB.openReadOnly(checkpointDir1)) {
 *     snap.get("k".getBytes()); // returns value at checkpoint time
 * }
 * }</pre>
 */
public final class Checkpoint implements AutoCloseable {

    private static final MethodHandle MH_CREATE;
    private static final MethodHandle MH_EXPORT;
    private static final MethodHandle MH_DESTROY;

    static {
        // rocksdb_checkpoint_t* rocksdb_checkpoint_object_create(rocksdb_t* db, char** errptr)
        MH_CREATE = RocksDB.lookup("rocksdb_checkpoint_object_create",
            FunctionDescriptor.of(ValueLayout.ADDRESS,
                ValueLayout.ADDRESS,   // db
                ValueLayout.ADDRESS)); // errptr

        // void rocksdb_checkpoint_create(rocksdb_checkpoint_t*, const char* dir,
        //                                uint64_t log_size_for_flush, char** errptr)
        MH_EXPORT = RocksDB.lookup("rocksdb_checkpoint_create",
            FunctionDescriptor.ofVoid(
                ValueLayout.ADDRESS,   // checkpoint
                ValueLayout.ADDRESS,   // checkpoint_dir
                ValueLayout.JAVA_LONG, // log_size_for_flush
                ValueLayout.ADDRESS)); // errptr

        // void rocksdb_checkpoint_object_destroy(rocksdb_checkpoint_t*)
        MH_DESTROY = RocksDB.lookup("rocksdb_checkpoint_object_destroy",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));
    }

    private static final ThreadLocal<MemorySegment> ERR_HOLDER = ThreadLocal.withInitial(
        () -> Arena.ofAuto().allocate(ValueLayout.ADDRESS));

    private final MemorySegment ptr;

    private Checkpoint(MemorySegment ptr) {
        this.ptr = ptr;
    }

    /**
     * Creates a checkpoint object bound to {@code db}.
     * The checkpoint object may be reused to export multiple snapshots.
     * Close it when done — this does not affect the database or any exported checkpoints.
     */
    public static Checkpoint create(RocksDB db) {
        var errHolder = ERR_HOLDER.get();
        errHolder.set(ValueLayout.ADDRESS, 0, MemorySegment.NULL);
        try {
            var ptr = (MemorySegment) MH_CREATE.invokeExact(db.dbPtr, errHolder);
            RocksDB.checkError(errHolder);
            return new Checkpoint(ptr);
        } catch (RocksDBException e) {
            throw e;
        } catch (Throwable t) {
            throw new RocksDBException("Checkpoint.create failed", t);
        }
    }

    /**
     * Exports a consistent snapshot of the database to {@code checkpointDir}.
     * The directory must not exist yet — RocksDB creates it.
     *
     * <p>The exported directory is a fully valid RocksDB database. Open it with
     * {@link RocksDB#openReadOnly(Path)} for read-only access.
     *
     * @param checkpointDir target directory (must not exist)
     * @param logSizeForFlush if the WAL is larger than this threshold (in bytes),
     *                        it is flushed to SST files before the checkpoint is taken.
     *                        Pass {@code 0} to always flush; pass {@code Long.MAX_VALUE}
     *                        to never flush (use WAL as-is).
     */
    public void exportTo(Path checkpointDir, long logSizeForFlush) {
        var errHolder = ERR_HOLDER.get();
        errHolder.set(ValueLayout.ADDRESS, 0, MemorySegment.NULL);
        try (var arena = Arena.ofConfined()) {
            var dirSeg = arena.allocateFrom(checkpointDir.toString());
            MH_EXPORT.invokeExact(ptr, dirSeg, logSizeForFlush, errHolder);
            RocksDB.checkError(errHolder);
        } catch (RocksDBException e) {
            throw e;
        } catch (Throwable t) {
            throw new RocksDBException("Checkpoint.exportTo failed", t);
        }
    }

    /**
     * Exports a consistent snapshot to {@code checkpointDir}, flushing the WAL
     * first (equivalent to {@code exportTo(dir, 0)}).
     */
    public void exportTo(Path checkpointDir) {
        exportTo(checkpointDir, 0L);
    }

    @Override
    public void close() {
        try {
            MH_DESTROY.invokeExact(ptr);
        } catch (Throwable t) {
            throw new RocksDBException("Checkpoint destroy failed", t);
        }
    }
}
