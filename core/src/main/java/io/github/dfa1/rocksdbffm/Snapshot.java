package io.github.dfa1.rocksdbffm;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/**
 * FFM wrapper for rocksdb_snapshot_t.
 *
 * <p>A snapshot provides a consistent, point-in-time view of the database.
 * Reads performed with a snapshot set on {@link ReadOptions} see only data
 * that was committed before the snapshot was taken.
 *
 * <p>Obtain via {@link RocksDB#getSnapshot()}, {@link TransactionDB#getSnapshot()},
 * or {@link Transaction#getSnapshot()}. Always close after use to release the
 * underlying native snapshot.
 *
 * <pre>{@code
 * try (Snapshot snap = db.getSnapshot();
 *      ReadOptions ro = new ReadOptions().setSnapshot(snap)) {
 *     byte[] v1 = db.get(ro, key);
 *     db.put(key, newValue);
 *     byte[] v2 = db.get(ro, key); // still returns v1 — consistent read
 * }
 * }</pre>
 */
public final class Snapshot extends NativeObject {

	// rocksdb_snapshot_get_sequence_number(snap*) -> uint64_t
	private static final MethodHandle MH_SEQUENCE_NUMBER;
	// rocksdb_release_snapshot(db*, snap*) — for RocksDB and TransactionDB snapshots
	private static final MethodHandle MH_RELEASE;
	// rocksdb_free(ptr*) — for Transaction snapshots
	private static final MethodHandle MH_FREE;

	static {
		MH_SEQUENCE_NUMBER = RocksDB.lookup("rocksdb_snapshot_get_sequence_number",
				FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));

		MH_RELEASE = RocksDB.lookup("rocksdb_release_snapshot",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_FREE = RocksDB.lookup("rocksdb_free",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));
	}

	/**
	 * Package-private: the raw snapshot pointer, used by ReadOptions.setSnapshot().
	 */
	final MemorySegment ptr;

	/**
	 * DB pointer used to release the snapshot; NULL signals that rocksdb_free
	 * should be used instead (transaction snapshot ownership model).
	 */
	private final MemorySegment dbPtr;

	/**
	 * Creates a snapshot owned by a RocksDB or TransactionDB instance.
	 */
	Snapshot(MemorySegment dbPtr, MemorySegment ptr) {
		super(ptr);
		this.dbPtr = dbPtr;
		this.ptr = ptr;
	}

	/**
	 * Creates a snapshot owned by a Transaction instance.
	 * Released via {@code rocksdb_free} rather than {@code rocksdb_release_snapshot}.
	 */
	Snapshot(MemorySegment ptr) {
		super(ptr);
		this.dbPtr = MemorySegment.NULL;
		this.ptr = ptr;
	}

	/**
	 * Returns the sequence number at which this snapshot was taken.
	 * Useful for ordering and debugging.
	 */
	public SequenceNumber sequenceNumber() {
		try {
			return SequenceNumber.of((long) MH_SEQUENCE_NUMBER.invokeExact(ptr));
		} catch (Throwable t) {
			throw new RocksDBException("snapshot sequenceNumber failed", t);
		}
	}

	@Override
	protected void tryClose(MemorySegment ptr) throws Throwable {
		if (MemorySegment.NULL.equals(dbPtr)) {
			MH_FREE.invokeExact(ptr);
		} else {
			MH_RELEASE.invokeExact(dbPtr, ptr);
		}
	}

}
