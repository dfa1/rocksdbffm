package io.github.dfa1.rocksdbffm;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.file.Path;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * FFM wrapper for rocksdb_transactiondb_t — a RocksDB database with pessimistic
 * (locking) transaction support.
 *
 * <pre>{@code
 * try (TransactionDBOptions txnDbOpts = new TransactionDBOptions();
 *      Options opts = new Options().setCreateIfMissing(true);
 *      TransactionDB db = TransactionDB.open(opts, txnDbOpts, path)) {
 *
 *     try (WriteOptions wo = new WriteOptions();
 *          Transaction txn = db.beginTransaction(wo)) {
 *         txn.put("key".getBytes(), "value".getBytes());
 *         txn.commit();
 *     }
 * }
 * }</pre>
 */
public final class TransactionDB implements AutoCloseable {

	// -----------------------------------------------------------------------
	// Method handles
	// -----------------------------------------------------------------------

	private static final MethodHandle MH_OPEN;
	private static final MethodHandle MH_CLOSE;
	private static final MethodHandle MH_BEGIN;
	private static final MethodHandle MH_CREATE_SNAPSHOT;
	private static final MethodHandle MH_FLUSH;
	private static final MethodHandle MH_FLUSH_WAL;
	private static final MethodHandle MH_PROPERTY_VALUE;
	private static final MethodHandle MH_PROPERTY_INT;

	// Direct (non-transactional) operations on the TransactionDB
	private static final MethodHandle MH_PUT;
	private static final MethodHandle MH_DELETE;
	private static final MethodHandle MH_GET;   // returns malloc'd char*, caller frees

	private static final MethodHandle MH_FREE;

	static {
		// rocksdb_transactiondb_t* rocksdb_transactiondb_open(opts*, txnDbOpts*, name, errptr**)
		MH_OPEN = RocksDB.lookup("rocksdb_transactiondb_open",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_CLOSE = RocksDB.lookup("rocksdb_transactiondb_close",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

		// rocksdb_transaction_t* rocksdb_transaction_begin(txndb*, wo*, txn_opts*, old_txn)
		MH_BEGIN = RocksDB.lookup("rocksdb_transaction_begin",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		// void rocksdb_transactiondb_put(txndb*, wo*, key*, klen, val*, vlen, errptr**)
		MH_PUT = RocksDB.lookup("rocksdb_transactiondb_put",
				FunctionDescriptor.ofVoid(
						ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS));

		// void rocksdb_transactiondb_delete(txndb*, wo*, key*, klen, errptr**)
		MH_DELETE = RocksDB.lookup("rocksdb_transactiondb_delete",
				FunctionDescriptor.ofVoid(
						ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS));

		// char* rocksdb_transactiondb_get(txndb*, ro*, key*, klen, size_t* vallen, errptr**)
		MH_GET = RocksDB.lookup("rocksdb_transactiondb_get",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_FREE = RocksDB.lookup("rocksdb_free",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

		// const rocksdb_snapshot_t* rocksdb_transactiondb_create_snapshot(txndb*)
		MH_CREATE_SNAPSHOT = RocksDB.lookup("rocksdb_transactiondb_create_snapshot",
				FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		// void rocksdb_transactiondb_flush(txndb*, flushopts*, errptr**)
		MH_FLUSH = RocksDB.lookup("rocksdb_transactiondb_flush",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		// void rocksdb_transactiondb_flush_wal(txndb*, sync, errptr**)
		MH_FLUSH_WAL = RocksDB.lookup("rocksdb_transactiondb_flush_wal",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_BYTE, ValueLayout.ADDRESS));

		// char* rocksdb_transactiondb_property_value(txndb*, propname)
		MH_PROPERTY_VALUE = RocksDB.lookup("rocksdb_transactiondb_property_value",
				FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		// int rocksdb_transactiondb_property_int(txndb*, propname, uint64_t* out_val)
		MH_PROPERTY_INT = RocksDB.lookup("rocksdb_transactiondb_property_int",
				FunctionDescriptor.of(ValueLayout.JAVA_INT,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));
	}

	// -----------------------------------------------------------------------
	// Instance state
	// -----------------------------------------------------------------------

	private final MemorySegment ptr;       // rocksdb_transactiondb_t*
	private final MemorySegment writeOpts; // default write options for direct ops
	private final MemorySegment readOpts;  // default read options for direct ops

	private TransactionDB(MemorySegment ptr, MemorySegment writeOpts, MemorySegment readOpts) {
		this.ptr = ptr;
		this.writeOpts = writeOpts;
		this.readOpts = readOpts;
	}

	// -----------------------------------------------------------------------
	// Factory
	// -----------------------------------------------------------------------

	/**
	 * Opens a TransactionDB at {@code path}.
	 * The caller retains ownership of {@code dbOptions} and {@code txnDbOptions}.
	 */
	public static TransactionDB open(Options dbOptions, TransactionDBOptions txnDbOptions, Path path) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = Native.errHolder(arena);
			MemorySegment pathSeg = arena.allocateFrom(path.toString());

			MemorySegment ptr = (MemorySegment) MH_OPEN.invokeExact(
					dbOptions.ptr, txnDbOptions.ptr, pathSeg, err);

			Native.checkError(err);

			MemorySegment writeOpts = (MemorySegment) WriteOptions.MH_CREATE.invokeExact();
			MemorySegment readOpts = (MemorySegment) ReadOptions.MH_CREATE.invokeExact();
			return new TransactionDB(ptr, writeOpts, readOpts);
		} catch (Throwable t) {
			throw RocksDBException.wrap("Native call failed", t);
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
			MH_FLUSH.invokeExact(ptr, flushOptions.ptr, err);
			Native.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("flush failed", t);
		}
	}

	/**
	 * Flushes the WAL (write-ahead log) to disk.
	 *
	 * @param sync if {@code true}, performs an {@code fsync} after writing
	 */
	public void flushWal(boolean sync) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = Native.errHolder(arena);
			MH_FLUSH_WAL.invokeExact(ptr, sync ? (byte) 1 : (byte) 0, err);
			Native.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("flushWal failed", t);
		}
	}

	// -----------------------------------------------------------------------
	// Snapshot
	// -----------------------------------------------------------------------

	/**
	 * Creates a snapshot of the current TransactionDB state.
	 * The returned snapshot must be closed after use.
	 */
	public Snapshot getSnapshot() {
		try {
			MemorySegment snapPtr = (MemorySegment) MH_CREATE_SNAPSHOT.invokeExact(ptr);
			return new Snapshot(ptr, snapPtr);
		} catch (Throwable t) {
			throw RocksDBException.wrap("getSnapshot failed", t);
		}
	}

	// -----------------------------------------------------------------------
	// Transaction API
	// -----------------------------------------------------------------------

	/**
	 * Begins a new transaction using the supplied write options and default
	 * transaction options.
	 */
	public Transaction beginTransaction(WriteOptions writeOptions) {
		try (TransactionOptions txnOpts = new TransactionOptions()) {
			return beginTransaction(writeOptions, txnOpts);
		}
	}

	/**
	 * Begins a new transaction using the supplied write options and transaction options.
	 */
	public Transaction beginTransaction(WriteOptions writeOptions, TransactionOptions txnOptions) {
		try {
			MemorySegment txnPtr = (MemorySegment) MH_BEGIN.invokeExact(
					ptr, writeOptions.ptr, txnOptions.ptr, MemorySegment.NULL);
			return new Transaction(txnPtr);
		} catch (Throwable t) {
			throw new RocksDBException("beginTransaction failed", t);
		}
	}

	// -----------------------------------------------------------------------
	// Direct (non-transactional) operations
	// -----------------------------------------------------------------------

	/**
	 * Direct put, bypassing any active transaction. Slow path: allocates native memory.
	 */
	public void put(byte[] key, byte[] value) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = Native.errHolder(arena);
			MemorySegment k = Native.toNative(arena, key);
			MemorySegment v = Native.toNative(arena, value);
			MH_PUT.invokeExact(ptr, writeOpts, k, (long) key.length, v, (long) value.length, err);
			Native.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("Native call failed", t);
		}
	}

	/**
	 * Direct get with explicit ReadOptions (e.g. for snapshot-pinned reads). Returns {@code null} if not found.
	 */
	public byte[] get(ReadOptions readOptions, byte[] key) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = Native.errHolder(arena);
			MemorySegment k = Native.toNative(arena, key);
			MemorySegment valLenSeg = arena.allocate(ValueLayout.JAVA_LONG);

			MemorySegment valPtr = (MemorySegment) MH_GET.invokeExact(
					ptr, readOptions.ptr, k, (long) key.length, valLenSeg, err);

			Native.checkError(err);

			if (MemorySegment.NULL.equals(valPtr)) return null;

			long valLen = valLenSeg.get(ValueLayout.JAVA_LONG, 0);
			byte[] result = valPtr.reinterpret(valLen).toArray(ValueLayout.JAVA_BYTE);
			MH_FREE.invokeExact(valPtr);
			return result;
		} catch (Throwable t) {
			throw RocksDBException.wrap("Native call failed", t);
		}
	}

	/**
	 * Direct get, reading committed data only. Returns {@code null} if not found. Slow path.
	 */
	public byte[] get(byte[] key) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = Native.errHolder(arena);
			MemorySegment k = Native.toNative(arena, key);
			MemorySegment valLenSeg = arena.allocate(ValueLayout.JAVA_LONG);

			MemorySegment valPtr = (MemorySegment) MH_GET.invokeExact(
					ptr, readOpts, k, (long) key.length, valLenSeg, err);

			Native.checkError(err);

			if (MemorySegment.NULL.equals(valPtr)) return null;

			long valLen = valLenSeg.get(ValueLayout.JAVA_LONG, 0);
			byte[] result = valPtr.reinterpret(valLen).toArray(ValueLayout.JAVA_BYTE);
			MH_FREE.invokeExact(valPtr);
			return result;
		} catch (Throwable t) {
			throw RocksDBException.wrap("Native call failed", t);
		}
	}

	// -----------------------------------------------------------------------
	// DB Properties
	// -----------------------------------------------------------------------

	/**
	 * @see RocksDB#getProperty(Property)
	 */
	public Optional<String> getProperty(Property property) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment propSeg = arena.allocateFrom(property.propertyName());
			MemorySegment result = (MemorySegment) MH_PROPERTY_VALUE.invokeExact(ptr, propSeg);
			if (MemorySegment.NULL.equals(result)) return Optional.empty();
			String value = result.reinterpret(Long.MAX_VALUE).getString(0);
			MH_FREE.invokeExact(result);
			return Optional.of(value);
		} catch (Throwable t) {
			throw RocksDBException.wrap("getProperty failed", t);
		}
	}

	/**
	 * @see RocksDB#getLongProperty(Property)
	 */
	public OptionalLong getLongProperty(Property property) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment propSeg = arena.allocateFrom(property.propertyName());
			MemorySegment out = arena.allocate(ValueLayout.JAVA_LONG);
			int rc = (int) MH_PROPERTY_INT.invokeExact(ptr, propSeg, out);
			if (rc != 0) return OptionalLong.empty();
			return OptionalLong.of(out.get(ValueLayout.JAVA_LONG, 0));
		} catch (Throwable t) {
			throw RocksDBException.wrap("getLongProperty failed", t);
		}
	}

	/**
	 * Direct delete, bypassing any active transaction. Slow path.
	 */
	public void delete(byte[] key) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = Native.errHolder(arena);
			MemorySegment k = Native.toNative(arena, key);
			MH_DELETE.invokeExact(ptr, writeOpts, k, (long) key.length, err);
			Native.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("Native call failed", t);
		}
	}

	// -----------------------------------------------------------------------
	// AutoCloseable
	// -----------------------------------------------------------------------

	@Override
	public void close() {
		try {
			WriteOptions.MH_DESTROY.invokeExact(writeOpts);
			ReadOptions.MH_DESTROY.invokeExact(readOpts);
			MH_CLOSE.invokeExact(ptr);
		} catch (Throwable t) {
			throw new RocksDBException("TransactionDB close failed", t);
		}
	}

	// -----------------------------------------------------------------------
	// Helpers
	// -----------------------------------------------------------------------

}
