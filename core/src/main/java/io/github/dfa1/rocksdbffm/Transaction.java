package io.github.dfa1.rocksdbffm;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/// FFM wrapper for `rocksdb_transaction_t`.
///
/// Obtained via [TransactionDB#beginTransaction]. Always close after use
/// (either committed or rolled back) to free the native object.
///
/// ```
/// try (Transaction txn = txnDb.beginTransaction(writeOptions)) {
///     txn.put("key".getBytes(), "value".getBytes());
///     txn.commit();
/// }
/// ```
public final class Transaction extends NativeObject {

	/// `rocksdb_pinnableslice_t* rocksdb_transaction_get_pinned_cf(rocksdb_transaction_t* txn, const rocksdb_readoptions_t* options, rocksdb_column_family_handle_t* column_family, const char* key, size_t klen, char** errptr);`
	private static final MethodHandle MH_GET_PINNED_CF;
	/// `char* rocksdb_transaction_get_for_update_cf(rocksdb_transaction_t* txn, const rocksdb_readoptions_t* options, rocksdb_column_family_handle_t* column_family, const char* key, size_t klen, size_t* vlen, unsigned char exclusive, char** errptr);`
	private static final MethodHandle MH_GET_FOR_UPDATE_CF;
	/// `void rocksdb_transaction_put_cf(rocksdb_transaction_t* txn, rocksdb_column_family_handle_t* column_family, const char* key, size_t klen, const char* val, size_t vlen, char** errptr);`
	private static final MethodHandle MH_PUT_CF;
	/// `void rocksdb_transaction_delete_cf(rocksdb_transaction_t* txn, rocksdb_column_family_handle_t* column_family, const char* key, size_t klen, char** errptr);`
	private static final MethodHandle MH_DELETE_CF;
	/// `rocksdb_iterator_t* rocksdb_transaction_create_iterator_cf(rocksdb_transaction_t* txn, const rocksdb_readoptions_t* options, rocksdb_column_family_handle_t* column_family);`
	private static final MethodHandle MH_CREATE_ITERATOR_CF;
	/// `void rocksdb_transaction_commit(rocksdb_transaction_t* txn, char** errptr);`
	private static final MethodHandle MH_COMMIT;
	/// `void rocksdb_transaction_rollback(rocksdb_transaction_t* txn, char** errptr);`
	private static final MethodHandle MH_ROLLBACK;
	/// `void rocksdb_transaction_destroy(rocksdb_transaction_t* txn);`
	private static final MethodHandle MH_DESTROY;
	/// `const rocksdb_snapshot_t* rocksdb_transaction_get_snapshot(rocksdb_transaction_t* txn);`
	private static final MethodHandle MH_GET_SNAPSHOT;
	/// `void rocksdb_transaction_set_savepoint(rocksdb_transaction_t* txn);`
	private static final MethodHandle MH_SET_SAVEPOINT;
	/// `void rocksdb_transaction_rollback_to_savepoint(rocksdb_transaction_t* txn, char** errptr);`
	private static final MethodHandle MH_ROLLBACK_TO_SAVEPOINT;
	/// `void rocksdb_transaction_put(rocksdb_transaction_t* txn, const char* key, size_t klen, const char* val, size_t vlen, char** errptr);`
	private static final MethodHandle MH_PUT;
	/// `void rocksdb_transaction_delete(rocksdb_transaction_t* txn, const char* key, size_t klen, char** errptr);`
	private static final MethodHandle MH_DELETE;
	/// `rocksdb_pinnableslice_t* rocksdb_transaction_get_pinned(rocksdb_transaction_t* txn, const rocksdb_readoptions_t* options, const char* key, size_t klen, char** errptr);`
	private static final MethodHandle MH_GET_PINNED;
	/// `char* rocksdb_transaction_get_for_update(rocksdb_transaction_t* txn, const rocksdb_readoptions_t* options, const char* key, size_t klen, size_t* vlen, unsigned char exclusive, char** errptr);`
	private static final MethodHandle MH_GET_FOR_UPDATE;
	/// `const char* rocksdb_pinnableslice_value(const rocksdb_pinnableslice_t* t, size_t* vlen);`
	private static final MethodHandle MH_PINNABLESLICE_VALUE;
	/// `void rocksdb_pinnableslice_destroy(rocksdb_pinnableslice_t* v);`
	private static final MethodHandle MH_PINNABLESLICE_DESTROY;

	static {
		MH_COMMIT = NativeLibrary.lookup("rocksdb_transaction_commit",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_ROLLBACK = NativeLibrary.lookup("rocksdb_transaction_rollback",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_DESTROY = NativeLibrary.lookup("rocksdb_transaction_destroy",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

		MH_SET_SAVEPOINT = NativeLibrary.lookup("rocksdb_transaction_set_savepoint",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

		MH_ROLLBACK_TO_SAVEPOINT = NativeLibrary.lookup("rocksdb_transaction_rollback_to_savepoint",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_PUT = NativeLibrary.lookup("rocksdb_transaction_put",
				FunctionDescriptor.ofVoid(
						ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS));

		MH_DELETE = NativeLibrary.lookup("rocksdb_transaction_delete",
				FunctionDescriptor.ofVoid(
						ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS));

		MH_GET_PINNED = NativeLibrary.lookup("rocksdb_transaction_get_pinned",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS));

		MH_GET_FOR_UPDATE = NativeLibrary.lookup("rocksdb_transaction_get_for_update",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS, ValueLayout.JAVA_BYTE,
						ValueLayout.ADDRESS));

		MH_PINNABLESLICE_VALUE = NativeLibrary.lookup("rocksdb_pinnableslice_value",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_PINNABLESLICE_DESTROY = NativeLibrary.lookup("rocksdb_pinnableslice_destroy",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));


		MH_PUT_CF = NativeLibrary.lookup("rocksdb_transaction_put_cf",
				FunctionDescriptor.ofVoid(
						ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS));

		MH_DELETE_CF = NativeLibrary.lookup("rocksdb_transaction_delete_cf",
				FunctionDescriptor.ofVoid(
						ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS));

		MH_GET_PINNED_CF = NativeLibrary.lookup("rocksdb_transaction_get_pinned_cf",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS));

		MH_GET_FOR_UPDATE_CF = NativeLibrary.lookup("rocksdb_transaction_get_for_update_cf",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS, ValueLayout.JAVA_BYTE,
						ValueLayout.ADDRESS));

		MH_CREATE_ITERATOR_CF = NativeLibrary.lookup("rocksdb_transaction_create_iterator_cf",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		// Note: must be freed with rocksdb_free, not rocksdb_release_snapshot
		MH_GET_SNAPSHOT = NativeLibrary.lookup("rocksdb_transaction_get_snapshot",
				FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS));
	}

	/// Package-private: created by TransactionDB.
	Transaction(MemorySegment ptr) {
		super(ptr);
	}

	// -----------------------------------------------------------------------
	// Write operations
	// -----------------------------------------------------------------------

	/// Stages a put inside this transaction. Slow path: allocates native memory for key/value.
	///
	/// @param key   key bytes
	/// @param value value bytes
	public void put(byte[] key, byte[] value) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = RocksDB.errHolder(arena);
			MemorySegment k = RocksDB.toNative(arena, key);
			MemorySegment v = RocksDB.toNative(arena, value);
			MH_PUT.invokeExact(ptr(), k, (long) key.length, v, (long) value.length, err);
			RocksDB.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("Native call failed", t);
		}
	}

	/// Stages a delete inside this transaction. Slow path: allocates native memory for key.
	///
	/// @param key key bytes to delete
	public void delete(byte[] key) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = RocksDB.errHolder(arena);
			MemorySegment k = RocksDB.toNative(arena, key);
			MH_DELETE.invokeExact(ptr(), k, (long) key.length, err);
			RocksDB.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("Native call failed", t);
		}
	}

	// -----------------------------------------------------------------------
	// Read operations
	// -----------------------------------------------------------------------

	/// Reads the value for `key` within this transaction, using PinnableSlice
	/// to avoid an intermediate copy. Returns `null` if not found.
	///
	/// Slow path: allocates native memory for the key.
	///
	/// @param readOptions read options for this read
	/// @param key         key bytes to look up
	/// @return value bytes, or `null` if the key does not exist
	public byte[] get(ReadOptions readOptions, byte[] key) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = RocksDB.errHolder(arena);
			MemorySegment k = RocksDB.toNative(arena, key);

			MemorySegment pin = (MemorySegment) MH_GET_PINNED.invokeExact(
					ptr(), readOptions.ptr(), k, (long) key.length, err);

			RocksDB.checkError(err);

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

	/// Reads the value for `key` and acquires a pessimistic lock on it for
	/// the duration of this transaction. Returns `null` if not found.
	///
	/// @param readOptions read options for this read
	/// @param key         key bytes to look up
	/// @param exclusive   if `true`, acquires an exclusive (write) lock; otherwise a shared (read) lock
	/// @return value bytes, or `null` if the key does not exist
	public byte[] getForUpdate(ReadOptions readOptions, byte[] key, boolean exclusive) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = RocksDB.errHolder(arena);
			MemorySegment k = RocksDB.toNative(arena, key);
			MemorySegment valLenSeg = arena.allocate(ValueLayout.JAVA_LONG);

			MemorySegment valPtr = (MemorySegment) MH_GET_FOR_UPDATE.invokeExact(
					ptr(), readOptions.ptr(), k, (long) key.length,
					valLenSeg, exclusive ? (byte) 1 : (byte) 0, err);

			RocksDB.checkError(err);

			if (MemorySegment.NULL.equals(valPtr)) {
				return null;
			}

			long valLen = valLenSeg.get(ValueLayout.JAVA_LONG, 0);
			byte[] result = valPtr.reinterpret(valLen).toArray(ValueLayout.JAVA_BYTE);
			RocksDB.free(valPtr);
			return result;
		} catch (Throwable t) {
			throw RocksDBException.wrap("Native call failed", t);
		}
	}

	// -----------------------------------------------------------------------
	// Write operations — column family overloads
	// -----------------------------------------------------------------------

	/// Stages a put into `cf` inside this transaction. Slow path: allocates native memory.
	///
	/// @param cf    target column family
	/// @param key   key bytes
	/// @param value value bytes
	public void put(ColumnFamilyHandle cf, byte[] key, byte[] value) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = RocksDB.errHolder(arena);
			MH_PUT_CF.invokeExact(ptr(), cf.ptr(),
					RocksDB.toNative(arena, key), (long) key.length,
					RocksDB.toNative(arena, value), (long) value.length, err);
			RocksDB.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("Native call failed", t);
		}
	}

	/// Stages a delete of `key` from `cf` inside this transaction. Slow path.
	///
	/// @param cf  target column family
	/// @param key key bytes to delete
	public void delete(ColumnFamilyHandle cf, byte[] key) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = RocksDB.errHolder(arena);
			MH_DELETE_CF.invokeExact(ptr(), cf.ptr(),
					RocksDB.toNative(arena, key), (long) key.length, err);
			RocksDB.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("Native call failed", t);
		}
	}

	// -----------------------------------------------------------------------
	// Read operations — column family overloads
	// -----------------------------------------------------------------------

	/// Reads `key` from `cf` within this transaction via PinnableSlice. Returns `null` if not found.
	///
	/// @param cf          column family to read from
	/// @param readOptions read options for this read
	/// @param key         key bytes to look up
	/// @return value bytes, or `null` if the key does not exist
	public byte[] get(ColumnFamilyHandle cf, ReadOptions readOptions, byte[] key) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = RocksDB.errHolder(arena);
			MemorySegment pin = (MemorySegment) MH_GET_PINNED_CF.invokeExact(
					ptr(), readOptions.ptr(), cf.ptr(),
					RocksDB.toNative(arena, key), (long) key.length, err);
			RocksDB.checkError(err);
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

	/// Reads `key` from `cf` and acquires a lock for the duration of this transaction.
	/// Returns `null` if not found.
	///
	/// @param cf          column family to read from
	/// @param readOptions read options for this read
	/// @param key         key bytes to look up
	/// @param exclusive   if `true`, acquires an exclusive (write) lock; otherwise a shared (read) lock
	/// @return value bytes, or `null` if the key does not exist
	public byte[] getForUpdate(ColumnFamilyHandle cf, ReadOptions readOptions, byte[] key, boolean exclusive) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = RocksDB.errHolder(arena);
			MemorySegment k = RocksDB.toNative(arena, key);
			MemorySegment valLenSeg = arena.allocate(ValueLayout.JAVA_LONG);
			MemorySegment valPtr = (MemorySegment) MH_GET_FOR_UPDATE_CF.invokeExact(
					ptr(), readOptions.ptr(), cf.ptr(), k, (long) key.length,
					valLenSeg, exclusive ? (byte) 1 : (byte) 0, err);
			RocksDB.checkError(err);
			if (MemorySegment.NULL.equals(valPtr)) {
				return null;
			}
			long valLen = valLenSeg.get(ValueLayout.JAVA_LONG, 0);
			byte[] result = valPtr.reinterpret(valLen).toArray(ValueLayout.JAVA_BYTE);
			RocksDB.free(valPtr);
			return result;
		} catch (Throwable t) {
			throw RocksDBException.wrap("Native call failed", t);
		}
	}

	/// Returns a new iterator scoped to `cf` within this transaction.
	///
	/// @param cf          column family to iterate over
	/// @param readOptions read options for this iterator
	/// @return a new [RocksIterator]; caller must close it
	public RocksIterator newIterator(ColumnFamilyHandle cf, ReadOptions readOptions) {
		try {
			MemorySegment iterPtr = (MemorySegment) MH_CREATE_ITERATOR_CF.invokeExact(
					ptr(), readOptions.ptr(), cf.ptr());
			return RocksIterator.create(iterPtr);
		} catch (Throwable t) {
			throw RocksDBException.wrap("newIterator failed", t);
		}
	}

	// -----------------------------------------------------------------------
	// Snapshot
	// -----------------------------------------------------------------------

	/// Returns the snapshot associated with this transaction, or `null` if none
	/// was set via [TransactionOptions].
	/// The returned snapshot must be closed after use (freed via `rocksdb_free`).
	///
	/// @return the transaction's snapshot, or `null` if none was set
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

	/// Commits all staged operations in this transaction.
	public void commit() {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = RocksDB.errHolder(arena);
			MH_COMMIT.invokeExact(ptr(), err);
			RocksDB.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("Native call failed", t);
		}
	}

	/// Rolls back all staged operations in this transaction.
	public void rollback() {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = RocksDB.errHolder(arena);
			MH_ROLLBACK.invokeExact(ptr(), err);
			RocksDB.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("Native call failed", t);
		}
	}

	/// Records a savepoint. Rollback can return to this point via [#rollbackToSavePoint()].
	public void setSavePoint() {
		try {
			MH_SET_SAVEPOINT.invokeExact(ptr());
		} catch (Throwable t) {
			throw new RocksDBException("transaction setSavePoint failed", t);
		}
	}

	/// Rolls back to the most recent savepoint set by [#setSavePoint()].
	public void rollbackToSavePoint() {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = RocksDB.errHolder(arena);
			MH_ROLLBACK_TO_SAVEPOINT.invokeExact(ptr(), err);
			RocksDB.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("Native call failed", t);
		}
	}

	/// Destroys the native transaction object. Does _not_ commit or rollback;
	/// call [#commit()] or [#rollback()] first.
	@Override
	protected void tryClose(MemorySegment ptr) throws Throwable {
		MH_DESTROY.invokeExact(ptr);
	}

}
