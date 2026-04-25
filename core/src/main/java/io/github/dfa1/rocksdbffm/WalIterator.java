package io.github.dfa1.rocksdbffm;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/// FFM wrapper for `rocksdb_wal_iterator_t`.
///
/// Iterates over [WriteBatch] entries written to the WAL since a given [SequenceNumber].
/// Useful for change-data-capture, replication, and auditing.
///
/// Always close after use.
///
/// ```
/// SequenceNumber from = db.getLatestSequenceNumber();
/// db.put("k".getBytes(), "v".getBytes());
///
/// try (WalIterator it = db.getUpdatesSince(from)) {
///     for (; it.isValid(); it.next()) {
///         try (WalBatchResult result = it.getBatch()) {
///             // process result.writeBatch()
///         }
///     }
///     it.checkStatus();
/// }
/// ```
public final class WalIterator extends NativeObject {

	/// `void rocksdb_wal_iter_next(rocksdb_wal_iterator_t* iter);`
	private static final MethodHandle MH_NEXT;
	/// `unsigned char rocksdb_wal_iter_valid(const rocksdb_wal_iterator_t*);`
	private static final MethodHandle MH_VALID;
	/// `void rocksdb_wal_iter_status(const rocksdb_wal_iterator_t* iter, char** errptr);`
	private static final MethodHandle MH_STATUS;
	/// `rocksdb_writebatch_t* rocksdb_wal_iter_get_batch(const rocksdb_wal_iterator_t* iter, uint64_t* seq);`
	private static final MethodHandle MH_GET_BATCH;
	/// `void rocksdb_wal_iter_destroy(const rocksdb_wal_iterator_t* iter);`
	private static final MethodHandle MH_DESTROY;

	static {
		MH_NEXT = NativeLibrary.lookup("rocksdb_wal_iter_next",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

		MH_VALID = NativeLibrary.lookup("rocksdb_wal_iter_valid",
				FunctionDescriptor.of(ValueLayout.JAVA_BYTE, ValueLayout.ADDRESS));

		MH_STATUS = NativeLibrary.lookup("rocksdb_wal_iter_status",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_GET_BATCH = NativeLibrary.lookup("rocksdb_wal_iter_get_batch",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_DESTROY = NativeLibrary.lookup("rocksdb_wal_iter_destroy",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));
	}

	private WalIterator(MemorySegment ptr) {
		super(ptr);
	}

	static WalIterator wrap(MemorySegment ptr) {
		return new WalIterator(ptr);
	}

	/// Returns `true` if the iterator is positioned at a valid batch.
	///
	/// @return `true` if the iterator is valid
	public boolean isValid() {
		try {
			return ((byte) MH_VALID.invokeExact(ptr())) != 0;
		} catch (Throwable t) {
			throw RocksDBException.wrap("wal_iter_valid failed", t);
		}
	}

	/// Moves to the next [WriteBatch]. Only call when [#isValid()] is `true`.
	public void next() {
		try {
			MH_NEXT.invokeExact(ptr());
		} catch (Throwable t) {
			throw RocksDBException.wrap("wal_iter_next failed", t);
		}
	}

	/// Checks for any error encountered during iteration.
	/// Always call after the iteration loop to detect I/O errors.
	///
	/// @throws RocksDBException if an error occurred
	public void checkStatus() {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = RocksDB.errHolder(arena);
			MH_STATUS.invokeExact(ptr(), err);
			RocksDB.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("wal_iter_status failed", t);
		}
	}

	/// Returns the current [WriteBatch] and the [SequenceNumber] of its first transaction.
	/// The caller owns the returned [WalBatchResult] and must close it.
	/// Only call when [#isValid()] is `true`.
	///
	/// @return the current batch and its sequence number
	public WalBatchResult getBatch() {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment seqHolder = arena.allocate(ValueLayout.JAVA_LONG);
			MemorySegment batchPtr = (MemorySegment) MH_GET_BATCH.invokeExact(ptr(), seqHolder);
			long seq = seqHolder.get(ValueLayout.JAVA_LONG, 0);
			return new WalBatchResult(SequenceNumber.of(seq), WriteBatch.wrap(batchPtr));
		} catch (Throwable t) {
			throw RocksDBException.wrap("wal_iter_get_batch failed", t);
		}
	}

	@Override
	protected void tryClose(MemorySegment ptr) throws Throwable {
		MH_DESTROY.invokeExact(ptr);
	}
}
