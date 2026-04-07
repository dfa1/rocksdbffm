package io.github.dfa1.rocksdbffm;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/**
 * FFM wrapper for rocksdb_transaction_t.
 *
 * <p>Obtained via {@link TransactionDB#beginTransaction}. Always close after use
 * (either committed or rolled back) to free the native object.
 *
 * <pre>{@code
 * try (Transaction txn = txnDb.beginTransaction(writeOptions)) {
 *     txn.put("key".getBytes(), "value".getBytes());
 *     txn.commit();
 * }
 * }</pre>
 */
public final class Transaction extends NativeObject {

	private static final MethodHandle MH_COMMIT;
	private static final MethodHandle MH_ROLLBACK;
	private static final MethodHandle MH_DESTROY;
	private static final MethodHandle MH_GET_SNAPSHOT;
	private static final MethodHandle MH_SET_SAVEPOINT;
	private static final MethodHandle MH_ROLLBACK_TO_SAVEPOINT;
	private static final MethodHandle MH_PUT;
	private static final MethodHandle MH_DELETE;
	private static final MethodHandle MH_GET_PINNED;
	private static final MethodHandle MH_GET_FOR_UPDATE;
	private static final MethodHandle MH_PINNABLESLICE_VALUE;
	private static final MethodHandle MH_PINNABLESLICE_DESTROY;

	static {
		MH_COMMIT = RocksDB.lookup("rocksdb_transaction_commit",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_ROLLBACK = RocksDB.lookup("rocksdb_transaction_rollback",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_DESTROY = RocksDB.lookup("rocksdb_transaction_destroy",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

		// void rocksdb_transaction_set_savepoint(txn*)
		MH_SET_SAVEPOINT = RocksDB.lookup("rocksdb_transaction_set_savepoint",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

		// void rocksdb_transaction_rollback_to_savepoint(txn*, errptr**)
		MH_ROLLBACK_TO_SAVEPOINT = RocksDB.lookup("rocksdb_transaction_rollback_to_savepoint",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		// void rocksdb_transaction_put(txn*, key*, klen, val*, vlen, errptr**)
		MH_PUT = RocksDB.lookup("rocksdb_transaction_put",
				FunctionDescriptor.ofVoid(
						ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS));

		// void rocksdb_transaction_delete(txn*, key*, klen, errptr**)
		MH_DELETE = RocksDB.lookup("rocksdb_transaction_delete",
				FunctionDescriptor.ofVoid(
						ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS));

		// rocksdb_pinnableslice_t* rocksdb_transaction_get_pinned(txn*, ro*, key*, klen, errptr**)
		MH_GET_PINNED = RocksDB.lookup("rocksdb_transaction_get_pinned",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS));

		// char* rocksdb_transaction_get_for_update(txn*, ro*, key*, klen, size_t* vlen, exclusive, errptr**)
		MH_GET_FOR_UPDATE = RocksDB.lookup("rocksdb_transaction_get_for_update",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS, ValueLayout.JAVA_BYTE,
						ValueLayout.ADDRESS));

		MH_PINNABLESLICE_VALUE = RocksDB.lookup("rocksdb_pinnableslice_value",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_PINNABLESLICE_DESTROY = RocksDB.lookup("rocksdb_pinnableslice_destroy",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));


		// const rocksdb_snapshot_t* rocksdb_transaction_get_snapshot(txn*)
		// Note: must be freed with rocksdb_free, not rocksdb_release_snapshot
		MH_GET_SNAPSHOT = RocksDB.lookup("rocksdb_transaction_get_snapshot",
				FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS));
	}

	/**
	 * Package-private: created by TransactionDB.
	 */
	Transaction(MemorySegment ptr) {
		super(ptr);
	}

	// -----------------------------------------------------------------------
	// Write operations
	// -----------------------------------------------------------------------

	/**
	 * Stages a put inside this transaction. Slow path: allocates native memory for key/value.
	 */
	public void put(byte[] key, byte[] value) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = Native.errHolder(arena);
			MemorySegment k = Native.toNative(arena, key);
			MemorySegment v = Native.toNative(arena, value);
			MH_PUT.invokeExact(ptr(), k, (long) key.length, v, (long) value.length, err);
			Native.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("Native call failed", t);
		}
	}

	/**
	 * Stages a delete inside this transaction. Slow path: allocates native memory for key.
	 */
	public void delete(byte[] key) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = Native.errHolder(arena);
			MemorySegment k = Native.toNative(arena, key);
			MH_DELETE.invokeExact(ptr(), k, (long) key.length, err);
			Native.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("Native call failed", t);
		}
	}

	// -----------------------------------------------------------------------
	// Read operations
	// -----------------------------------------------------------------------

	/**
	 * Reads the value for {@code key} within this transaction, using PinnableSlice
	 * to avoid an intermediate copy. Returns {@code null} if not found.
	 *
	 * <p>Slow path: allocates native memory for the key.
	 */
	public byte[] get(ReadOptions readOptions, byte[] key) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = Native.errHolder(arena);
			MemorySegment k = Native.toNative(arena, key);

			MemorySegment pin = (MemorySegment) MH_GET_PINNED.invokeExact(
					ptr(), readOptions.ptr(), k, (long) key.length, err);

			Native.checkError(err);

			if (MemorySegment.NULL.equals(pin)) {
				return null;
			}

			MemorySegment valLenSeg = arena.allocate(ValueLayout.JAVA_LONG);
			MemorySegment valPtr = (MemorySegment) MH_PINNABLESLICE_VALUE.invokeExact(pin, valLenSeg);
			long valLen = valLenSeg.get(ValueLayout.JAVA_LONG, 0);
			byte[] result = valPtr.reinterpret(valLen).toArray(ValueLayout.JAVA_BYTE);
			MH_PINNABLESLICE_DESTROY.invokeExact(pin);
			return result;
		} catch (Throwable t) {
			throw RocksDBException.wrap("Native call failed", t);
		}
	}

	/**
	 * Reads the value for {@code key} and acquires a pessimistic lock on it for
	 * the duration of this transaction. Returns {@code null} if not found.
	 *
	 * @param exclusive if true, acquires an exclusive (write) lock; otherwise a shared (read) lock
	 */
	public byte[] getForUpdate(ReadOptions readOptions, byte[] key, boolean exclusive) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = Native.errHolder(arena);
			MemorySegment k = Native.toNative(arena, key);
			MemorySegment valLenSeg = arena.allocate(ValueLayout.JAVA_LONG);

			MemorySegment valPtr = (MemorySegment) MH_GET_FOR_UPDATE.invokeExact(
					ptr(), readOptions.ptr(), k, (long) key.length,
					valLenSeg, exclusive ? (byte) 1 : (byte) 0, err);

			Native.checkError(err);

			if (MemorySegment.NULL.equals(valPtr)) {
				return null;
			}

			long valLen = valLenSeg.get(ValueLayout.JAVA_LONG, 0);
			byte[] result = valPtr.reinterpret(valLen).toArray(ValueLayout.JAVA_BYTE);
			Native.free(valPtr);
			return result;
		} catch (Throwable t) {
			throw RocksDBException.wrap("Native call failed", t);
		}
	}

	// -----------------------------------------------------------------------
	// Snapshot
	// -----------------------------------------------------------------------

	/**
	 * Returns the snapshot associated with this transaction, or {@code null} if none
	 * was set via {@link TransactionOptions}.
	 * The returned snapshot must be closed after use (freed via {@code rocksdb_free}).
	 */
	public Snapshot getSnapshot() {
		try {
			MemorySegment snapPtr = (MemorySegment) MH_GET_SNAPSHOT.invokeExact(ptr());
			if (MemorySegment.NULL.equals(snapPtr)) {
				return null;
			}
			return new Snapshot(snapPtr); // released via rocksdb_free
		} catch (Throwable t) {
			throw RocksDBException.wrap("getSnapshot failed", t);
		}
	}

	// -----------------------------------------------------------------------
	// Transaction control
	// -----------------------------------------------------------------------

	/**
	 * Commits all staged operations in this transaction.
	 */
	public void commit() {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = Native.errHolder(arena);
			MH_COMMIT.invokeExact(ptr(), err);
			Native.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("Native call failed", t);
		}
	}

	/**
	 * Rolls back all staged operations in this transaction.
	 */
	public void rollback() {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = Native.errHolder(arena);
			MH_ROLLBACK.invokeExact(ptr(), err);
			Native.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("Native call failed", t);
		}
	}

	/**
	 * Records a savepoint. Rollback can return to this point via {@link #rollbackToSavePoint()}.
	 */
	public void setSavePoint() {
		try {
			MH_SET_SAVEPOINT.invokeExact(ptr());
		} catch (Throwable t) {
			throw new RocksDBException("transaction setSavePoint failed", t);
		}
	}

	/**
	 * Rolls back to the most recent savepoint set by {@link #setSavePoint()}.
	 */
	public void rollbackToSavePoint() {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = Native.errHolder(arena);
			MH_ROLLBACK_TO_SAVEPOINT.invokeExact(ptr(), err);
			Native.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("Native call failed", t);
		}
	}

	/**
	 * Destroys the native transaction object. Does <em>not</em> commit or rollback;
	 * call {@link #commit()} or {@link #rollback()} first.
	 */
	@Override
	protected void tryClose(MemorySegment ptr) throws Throwable {
		MH_DESTROY.invokeExact(ptr);
	}

	// -----------------------------------------------------------------------
	// Helpers
	// -----------------------------------------------------------------------

}
