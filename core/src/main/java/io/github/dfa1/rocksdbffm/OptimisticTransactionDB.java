package io.github.dfa1.rocksdbffm;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.file.Path;
import java.util.Optional;
import java.util.OptionalLong;

/// FFM wrapper for `rocksdb_optimistictransactiondb_t` — a RocksDB database with
/// optimistic (lock-free) transaction support.
///
/// Optimistic transactions do _not_ acquire locks on read. Instead, conflicts
/// are detected at [Transaction#commit()] time. If another writer has modified a
/// key that this transaction read or wrote since the transaction began,
/// [Transaction#commit()] throws [RocksDBException] (status "busy").
/// The caller should then abort and retry.
///
/// ```
/// try (Options opts = Options.newOptions().setCreateIfMissing(true);
///      OptimisticTransactionDB db = OptimisticTransactionDB.open(opts, path)) {
///     try (WriteOptions wo = WriteOptions.newWriteOptions();
///          Transaction txn = db.beginTransaction(wo)) {
///         txn.put("key".getBytes(), "value".getBytes());
///         txn.commit(); // throws RocksDBException if conflict detected
///     }
/// }
/// ```
public final class OptimisticTransactionDB extends NativeObject {

	// -----------------------------------------------------------------------
	// Method handles
	// -----------------------------------------------------------------------

	/// `rocksdb_optimistictransactiondb_t* rocksdb_optimistictransactiondb_open(const rocksdb_options_t* options, const char* name, char** errptr);`
	private static final MethodHandle MH_OPEN;
	/// `void rocksdb_optimistictransactiondb_close(rocksdb_optimistictransactiondb_t* otxn_db);`
	private static final MethodHandle MH_CLOSE;
	/// `rocksdb_transaction_t* rocksdb_optimistictransaction_begin(rocksdb_optimistictransactiondb_t* otxn_db, const rocksdb_writeoptions_t* write_options, const rocksdb_optimistictransaction_options_t* otxn_options, rocksdb_transaction_t* old_txn);`
	private static final MethodHandle MH_BEGIN;
	/// `rocksdb_t* rocksdb_optimistictransactiondb_get_base_db(rocksdb_optimistictransactiondb_t* otxn_db);`
	private static final MethodHandle MH_GET_BASE_DB;
	/// `void rocksdb_optimistictransactiondb_close_base_db(rocksdb_t* base_db);`
	private static final MethodHandle MH_CLOSE_BASE_DB;

	// Direct (non-transactional) ops via base rocksdb_t*
	/// `void rocksdb_put(rocksdb_t* db, const rocksdb_writeoptions_t* options, const char* key, size_t keylen, const char* val, size_t vallen, char** errptr);`
	private static final MethodHandle MH_PUT;
	/// `void rocksdb_delete(rocksdb_t* db, const rocksdb_writeoptions_t* options, const char* key, size_t keylen, char** errptr);`
	private static final MethodHandle MH_DELETE;
	/// `char* rocksdb_get(rocksdb_t* db, const rocksdb_readoptions_t* options, const char* key, size_t keylen, size_t* vallen, char** errptr);`
	private static final MethodHandle MH_GET;
	/// `const rocksdb_snapshot_t* rocksdb_create_snapshot(rocksdb_t* db);`
	private static final MethodHandle MH_CREATE_SNAPSHOT;
	/// `void rocksdb_flush(rocksdb_t* db, const rocksdb_flushoptions_t* options, char** errptr);`
	private static final MethodHandle MH_FLUSH;
	/// `void rocksdb_flush_wal(rocksdb_t* db, unsigned char sync, char** errptr);`
	private static final MethodHandle MH_FLUSH_WAL;
	/// `char* rocksdb_property_value(rocksdb_t* db, const char* propname);`
	private static final MethodHandle MH_PROPERTY_VALUE;
	/// `int rocksdb_property_int(rocksdb_t* db, const char* propname, uint64_t* out_val);`
	private static final MethodHandle MH_PROPERTY_INT;

	static {
		MH_OPEN = RocksDB.lookup("rocksdb_optimistictransactiondb_open",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_CLOSE = RocksDB.lookup("rocksdb_optimistictransactiondb_close",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

		MH_BEGIN = RocksDB.lookup("rocksdb_optimistictransaction_begin",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_GET_BASE_DB = RocksDB.lookup("rocksdb_optimistictransactiondb_get_base_db",
				FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_CLOSE_BASE_DB = RocksDB.lookup("rocksdb_optimistictransactiondb_close_base_db",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

		MH_PUT = RocksDB.lookup("rocksdb_put",
				FunctionDescriptor.ofVoid(
						ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS));

		MH_DELETE = RocksDB.lookup("rocksdb_delete",
				FunctionDescriptor.ofVoid(
						ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS));

		MH_GET = RocksDB.lookup("rocksdb_get",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_CREATE_SNAPSHOT = RocksDB.lookup("rocksdb_create_snapshot",
				FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_FLUSH = RocksDB.lookup("rocksdb_flush",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_FLUSH_WAL = RocksDB.lookup("rocksdb_flush_wal",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_BYTE, ValueLayout.ADDRESS));

		MH_PROPERTY_VALUE = RocksDB.lookup("rocksdb_property_value",
				FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_PROPERTY_INT = RocksDB.lookup("rocksdb_property_int",
				FunctionDescriptor.of(ValueLayout.JAVA_INT,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));

	}

	// -----------------------------------------------------------------------
	// Instance state
	// -----------------------------------------------------------------------

	private final MemorySegment baseDb;    // rocksdb_t* — for direct ops
	private final WriteOptions writeOpts; // default write options
	private final ReadOptions readOpts;  // default read options

	private OptimisticTransactionDB(MemorySegment ptr, MemorySegment baseDb,
	                                WriteOptions writeOpts, ReadOptions readOpts) {
		super(ptr);
		this.baseDb = baseDb;
		this.writeOpts = writeOpts;
		this.readOpts = readOpts;
	}

	// -----------------------------------------------------------------------
	// Factory
	// -----------------------------------------------------------------------

	/// Opens an OptimisticTransactionDB at `path`.
	/// The caller retains ownership of `dbOptions`.
	public static OptimisticTransactionDB open(Options dbOptions, Path path) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = Native.errHolder(arena);
			MemorySegment pathSeg = arena.allocateFrom(path.toString());

			MemorySegment ptr = (MemorySegment) MH_OPEN.invokeExact(
					dbOptions.ptr(), pathSeg, err);
			Native.checkError(err);

			MemorySegment baseDb = (MemorySegment) MH_GET_BASE_DB.invokeExact(ptr);
			return new OptimisticTransactionDB(ptr, baseDb, WriteOptions.newWriteOptions(), ReadOptions.newReadOptions());
		} catch (Throwable t) {
			throw RocksDBException.wrap("OptimisticTransactionDB open failed", t);
		}
	}

	// -----------------------------------------------------------------------
	// Transaction API
	// -----------------------------------------------------------------------

	/// Begins a new optimistic transaction using the supplied write options and
	/// default [OptimisticTransactionOptions].
	public Transaction beginTransaction(WriteOptions writeOptions) {
		try (OptimisticTransactionOptions txnOpts = OptimisticTransactionOptions.newOptimisticTransactionOptions()) {
			return beginTransaction(writeOptions, txnOpts);
		}
	}

	/// Begins a new optimistic transaction using the supplied write options and
	/// transaction options.
	public Transaction beginTransaction(WriteOptions writeOptions, OptimisticTransactionOptions txnOptions) {
		try {
			MemorySegment txnPtr = (MemorySegment) MH_BEGIN.invokeExact(
					ptr(), writeOptions.ptr(), txnOptions.ptr(), MemorySegment.NULL);
			return new Transaction(txnPtr);
		} catch (Throwable t) {
			throw new RocksDBException("beginTransaction failed", t);
		}
	}

	// -----------------------------------------------------------------------
	// Direct (non-transactional) operations via base DB
	// -----------------------------------------------------------------------

	/// Direct put, bypassing any active transaction. Slow path: allocates native memory.
	public void put(byte[] key, byte[] value) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = Native.errHolder(arena);
			MemorySegment k = Native.toNative(arena, key);
			MemorySegment v = Native.toNative(arena, value);
			MH_PUT.invokeExact(baseDb, writeOpts.ptr(), k, (long) key.length, v, (long) value.length, err);
			Native.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("put failed", t);
		}
	}

	/// Direct get with explicit ReadOptions (e.g. for snapshot-pinned reads). Returns `null` if not found.
	public byte[] get(ReadOptions readOptions, byte[] key) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = Native.errHolder(arena);
			MemorySegment k = Native.toNative(arena, key);
			MemorySegment valLenSeg = arena.allocate(ValueLayout.JAVA_LONG);

			MemorySegment valPtr = (MemorySegment) MH_GET.invokeExact(
					baseDb, readOptions.ptr(), k, (long) key.length, valLenSeg, err);
			Native.checkError(err);

			if (MemorySegment.NULL.equals(valPtr)) {
				return null;
			}

			long valLen = valLenSeg.get(ValueLayout.JAVA_LONG, 0);
			byte[] result = valPtr.reinterpret(valLen).toArray(ValueLayout.JAVA_BYTE);
			Native.free(valPtr);
			return result;
		} catch (Throwable t) {
			throw RocksDBException.wrap("get failed", t);
		}
	}

	/// Direct get, reading committed data only. Returns `null` if not found. Slow path.
	public byte[] get(byte[] key) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = Native.errHolder(arena);
			MemorySegment k = Native.toNative(arena, key);
			MemorySegment valLenSeg = arena.allocate(ValueLayout.JAVA_LONG);

			MemorySegment valPtr = (MemorySegment) MH_GET.invokeExact(
					baseDb, readOpts.ptr(), k, (long) key.length, valLenSeg, err);
			Native.checkError(err);

			if (MemorySegment.NULL.equals(valPtr)) {
				return null;
			}

			long valLen = valLenSeg.get(ValueLayout.JAVA_LONG, 0);
			byte[] result = valPtr.reinterpret(valLen).toArray(ValueLayout.JAVA_BYTE);
			Native.free(valPtr);
			return result;
		} catch (Throwable t) {
			throw RocksDBException.wrap("get failed", t);
		}
	}

	/// Direct delete, bypassing any active transaction. Slow path.
	public void delete(byte[] key) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = Native.errHolder(arena);
			MemorySegment k = Native.toNative(arena, key);
			MH_DELETE.invokeExact(baseDb, writeOpts.ptr(), k, (long) key.length, err);
			Native.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("delete failed", t);
		}
	}

	// -----------------------------------------------------------------------
	// Snapshot
	// -----------------------------------------------------------------------

	/// Creates a snapshot of the current DB state.
	/// The returned snapshot must be closed after use.
	public Snapshot getSnapshot() {
		try {
			MemorySegment snapPtr = (MemorySegment) MH_CREATE_SNAPSHOT.invokeExact(baseDb);
			return new Snapshot(baseDb, snapPtr);
		} catch (Throwable t) {
			throw RocksDBException.wrap("getSnapshot failed", t);
		}
	}

	// -----------------------------------------------------------------------
	// Flush
	// -----------------------------------------------------------------------

	/// Flushes all memtable data to SST files on disk.
	public void flush(FlushOptions flushOptions) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = Native.errHolder(arena);
			MH_FLUSH.invokeExact(baseDb, flushOptions.ptr(), err);
			Native.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("flush failed", t);
		}
	}

	/// Flushes the WAL (write-ahead log) to disk.
	///
	/// @param sync if `true`, performs an `fsync` after writing
	public void flushWal(boolean sync) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = Native.errHolder(arena);
			MH_FLUSH_WAL.invokeExact(baseDb, sync ? (byte) 1 : (byte) 0, err);
			Native.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("flushWal failed", t);
		}
	}

	// -----------------------------------------------------------------------
	// DB Properties
	// -----------------------------------------------------------------------

	/// @see RocksDB#getProperty(Property)
	public Optional<String> getProperty(Property property) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment propSeg = arena.allocateFrom(property.propertyName());
			MemorySegment result = (MemorySegment) MH_PROPERTY_VALUE.invokeExact(baseDb, propSeg);
			if (MemorySegment.NULL.equals(result)) {
				return Optional.empty();
			}
			String value = result.reinterpret(Long.MAX_VALUE).getString(0);
			Native.free(result);
			return Optional.of(value);
		} catch (Throwable t) {
			throw RocksDBException.wrap("getProperty failed", t);
		}
	}

	/// @see RocksDB#getLongProperty(Property)
	public OptionalLong getLongProperty(Property property) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment propSeg = arena.allocateFrom(property.propertyName());
			MemorySegment out = arena.allocate(ValueLayout.JAVA_LONG);
			int rc = (int) MH_PROPERTY_INT.invokeExact(baseDb, propSeg, out);
			if (rc != 0) {
				return OptionalLong.empty();
			}
			return OptionalLong.of(out.get(ValueLayout.JAVA_LONG, 0));
		} catch (Throwable t) {
			throw RocksDBException.wrap("getLongProperty failed", t);
		}
	}

	// -----------------------------------------------------------------------
	// AutoCloseable
	// -----------------------------------------------------------------------

	@Override
	protected void tryClose(MemorySegment ptr) throws Throwable {
		writeOpts.close();
		readOpts.close();
		MH_CLOSE_BASE_DB.invokeExact(baseDb);
		MH_CLOSE.invokeExact(ptr);
	}
}
