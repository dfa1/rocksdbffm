package io.github.dfa1.rocksdbffm;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.file.Path;

/// FFM wrapper for `rocksdb_checkpoint_t`.
///
/// A checkpoint is a consistent, point-in-time snapshot of the database
/// written to a new directory. The resulting directory is a valid RocksDB
/// database that can be opened read-only without any recovery.
///
/// ```
/// try (var db = RocksDB.open(dir);
///      var cp = Checkpoint.create(db)) {
///     cp.exportTo(checkpointDir1);
///     db.put("k".getBytes(), "v2".getBytes());
///     cp.exportTo(checkpointDir2);  // second checkpoint, later state
/// }
/// // Each checkpoint directory is an independent read-only database
/// try (var snap = RocksDB.openReadOnly(checkpointDir1)) {
///     snap.get("k".getBytes()); // returns value at checkpoint time
/// }
/// ```
public final class Checkpoint extends NativeObject {

	/// `rocksdb_checkpoint_t* rocksdb_checkpoint_object_create(rocksdb_t* db, char** errptr);`
	private static final MethodHandle MH_CREATE;
	/// `void rocksdb_checkpoint_create(rocksdb_checkpoint_t* checkpoint, const char* checkpoint_dir, uint64_t log_size_for_flush, char** errptr);`
	private static final MethodHandle MH_EXPORT;
	/// `void rocksdb_checkpoint_object_destroy(rocksdb_checkpoint_t* checkpoint);`
	private static final MethodHandle MH_DESTROY;

	static {
		MH_CREATE = NativeLibrary.lookup("rocksdb_checkpoint_object_create",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS,   // db
						ValueLayout.ADDRESS)); // errptr

		MH_EXPORT = NativeLibrary.lookup("rocksdb_checkpoint_create",
				FunctionDescriptor.ofVoid(
						ValueLayout.ADDRESS,   // checkpoint
						ValueLayout.ADDRESS,   // checkpoint_dir
						ValueLayout.JAVA_LONG, // log_size_for_flush
						ValueLayout.ADDRESS)); // errptr

		MH_DESTROY = NativeLibrary.lookup("rocksdb_checkpoint_object_destroy",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));
	}

	private Checkpoint(MemorySegment ptr) {
		super(ptr);
	}

	/// Creates a checkpoint object bound to `db`.
	/// The checkpoint object may be reused to export multiple snapshots.
	/// Close it when done — this does not affect the database or any exported checkpoints.
	///
	/// Accepts any [RocksDbHandle]: [ReadWriteDB], [TtlDB], [ReadOnlyDB], or [SecondaryDB].
	public static Checkpoint newCheckpoint(RocksDbHandle db) {
		return create(db.ptr());
	}

	private static Checkpoint create(MemorySegment dbPtr) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = Native.errHolder(arena);
			var ptr = (MemorySegment) MH_CREATE.invokeExact(dbPtr, err);
			Native.checkError(err);
			return new Checkpoint(ptr);
		} catch (Throwable t) {
			throw RocksDBException.wrap("newCheckpoint failed", t);
		}
	}

	/// Exports a consistent snapshot of the database to `checkpointDir`.
	/// The directory must not exist yet — RocksDB creates it.
	///
	/// The exported directory is a fully valid RocksDB database. Open it with
	/// [RocksDB#openReadOnly(Path)] for read-only access.
	///
	/// @param checkpointDir   target directory (must not exist)
	/// @param logSizeForFlush if the WAL is larger than this threshold (in bytes),
	///                        it is flushed to SST files before the checkpoint is taken.
	///                        Pass `0` to always flush; pass `Long.MAX_VALUE`
	///                        to never flush (use WAL as-is).
	// TODO: expose MemorySize for logSizeForFlush
	public void exportTo(Path checkpointDir, long logSizeForFlush) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = Native.errHolder(arena);
			var dirSeg = arena.allocateFrom(checkpointDir.toString());
			MH_EXPORT.invokeExact(ptr(), dirSeg, logSizeForFlush, err);
			Native.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("Native call failed", t);
		}
	}

	/// Exports a consistent snapshot to `checkpointDir`, flushing the WAL
	/// first (equivalent to `exportTo(dir, 0)`).
	public void exportTo(Path checkpointDir) {
		exportTo(checkpointDir, 0L);
	}

	@Override
	public void tryClose(MemorySegment ptr) throws Throwable {
		MH_DESTROY.invokeExact(ptr);
	}
}
