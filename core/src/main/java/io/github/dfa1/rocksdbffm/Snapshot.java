package io.github.dfa1.rocksdbffm;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/// FFM wrapper for `rocksdb_snapshot_t`.
///
/// A snapshot provides a consistent, point-in-time view of the database.
/// Reads performed with a snapshot set on [ReadOptions] see only data
/// that was committed before the snapshot was taken.
///
/// Obtain via [RocksDB#getSnapshot()], [TransactionDB#getSnapshot()],
/// or [Transaction#getSnapshot()]. Always close after use to release the
/// underlying native snapshot.
///
/// ```
/// try (Snapshot snap = db.getSnapshot();
///      ReadOptions ro = ReadOptions.newReadOptions().setSnapshot(snap)) {
///     byte[] v1 = db.get(ro, key);
///     db.put(key, newValue);
///     byte[] v2 = db.get(ro, key); // still returns v1 — consistent read
/// }
/// ```
public final class Snapshot extends NativeObject {

	/// `uint64_t rocksdb_snapshot_get_sequence_number(const rocksdb_snapshot_t* snapshot);`
	private static final MethodHandle MH_SEQUENCE_NUMBER;
	/// `void rocksdb_release_snapshot(rocksdb_t* db, const rocksdb_snapshot_t* snapshot);`
	private static final MethodHandle MH_RELEASE;
	// rocksdb_free(ptr*) — for Transaction snapshots

	static {
		MH_SEQUENCE_NUMBER = NativeLibrary.lookup("rocksdb_snapshot_get_sequence_number",
				FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));

		MH_RELEASE = NativeLibrary.lookup("rocksdb_release_snapshot",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		// TODO: rocksdb_free is declared a lot of times
	}

	/// DB pointer used to release the snapshot; NULL signals that `rocksdb_free`
	/// should be used instead (transaction snapshot ownership model).
	private final MemorySegment dbPtr;

	/// Creates a snapshot owned by a RocksDB or TransactionDB instance.
	Snapshot(MemorySegment dbPtr, MemorySegment ptr) {
		super(ptr);
		this.dbPtr = dbPtr;
	}

	/// Creates a snapshot owned by a Transaction instance.
	/// Released via `rocksdb_free` rather than `rocksdb_release_snapshot`.
	Snapshot(MemorySegment ptr) {
		super(ptr);
		this.dbPtr = MemorySegment.NULL;
	}

	/// Returns the sequence number at which this snapshot was taken.
	/// Useful for ordering and debugging.
	public SequenceNumber sequenceNumber() {
		try {
			return SequenceNumber.of((long) MH_SEQUENCE_NUMBER.invokeExact(ptr()));
		} catch (Throwable t) {
			throw new RocksDBException("snapshot sequenceNumber failed", t);
		}
	}

	@Override
	protected void tryClose(MemorySegment ptr) throws Throwable {
		if (MemorySegment.NULL.equals(dbPtr)) {
			Native.free(ptr);
		} else {
			MH_RELEASE.invokeExact(dbPtr, ptr);
		}
	}

}
