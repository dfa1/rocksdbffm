package io.github.dfa1.rocksdbffm;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.ByteBuffer;
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
	// Method handles unique to OptimisticTransactionDB
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
	}

	// -----------------------------------------------------------------------
	// Instance state
	// -----------------------------------------------------------------------

	private final MemorySegment baseDb;    // rocksdb_t* — for direct ops via shared helpers
	private final WriteOptions writeOpts;
	private final ReadOptions readOpts;

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
	// Put
	// -----------------------------------------------------------------------

	/// Direct put, bypassing any active transaction. Slow path: allocates native memory.
	public void put(byte[] key, byte[] value) {
		RocksDB.putBytes(baseDb, writeOpts.ptr(), key, value);
	}

	/// Zero-copy put: wraps the direct buffers' native memory without heap→native copy.
	public void put(ByteBuffer key, ByteBuffer value) {
		RocksDB.putSegment(baseDb, writeOpts.ptr(),
				MemorySegment.ofBuffer(key), key.remaining(),
				MemorySegment.ofBuffer(value), value.remaining());
	}

	/// Zero-copy put: caller supplies pre-allocated native segments.
	public void put(MemorySegment key, MemorySegment value) {
		RocksDB.putSegment(baseDb, writeOpts.ptr(), key, key.byteSize(), value, value.byteSize());
	}

	// -----------------------------------------------------------------------
	// Get
	// -----------------------------------------------------------------------

	/// Direct get, reading committed data only. Returns `null` if not found.
	/// Uses PinnableSlice to avoid an intermediate copy from the block cache.
	public byte[] get(byte[] key) {
		return RocksDB.getBytes(baseDb, readOpts.ptr(), key);
	}

	/// Direct get with explicit [ReadOptions], e.g. for snapshot-pinned reads. Returns `null` if not found.
	public byte[] get(ReadOptions readOptions, byte[] key) {
		return RocksDB.getBytes(baseDb, readOptions.ptr(), key);
	}

	/// Single-copy get via PinnableSlice + direct output [ByteBuffer].
	/// Returns the actual value length, or -1 if not found.
	public int get(ByteBuffer key, ByteBuffer value) {
		return RocksDB.getIntoBuffer(baseDb, readOpts.ptr(),
				MemorySegment.ofBuffer(key), key.remaining(), value);
	}

	/// Zero-copy get via PinnableSlice into a caller-supplied native segment.
	/// Returns the actual value length.
	public long get(MemorySegment key, MemorySegment value) {
		return RocksDB.getIntoSegment(baseDb, readOpts.ptr(), key, key.byteSize(), value);
	}

	// -----------------------------------------------------------------------
	// Delete
	// -----------------------------------------------------------------------

	/// Direct delete, bypassing any active transaction. Slow path.
	public void delete(byte[] key) {
		RocksDB.deleteBytes(baseDb, writeOpts.ptr(), key);
	}

	/// Zero-copy for direct [ByteBuffer]s.
	public void delete(ByteBuffer key) {
		RocksDB.deleteSegment(baseDb, writeOpts.ptr(), MemorySegment.ofBuffer(key), key.remaining());
	}

	/// Zero-copy native-first path.
	public void delete(MemorySegment key) {
		RocksDB.deleteSegment(baseDb, writeOpts.ptr(), key, key.byteSize());
	}

	// -----------------------------------------------------------------------
	// Iterator
	// -----------------------------------------------------------------------

	/// Returns a new iterator using the database's default read options.
	public RocksIterator newIterator() {
		return RocksIterator.create(baseDb, readOpts.ptr());
	}

	/// Returns a new iterator using the supplied [ReadOptions].
	public RocksIterator newIterator(ReadOptions readOptions) {
		return RocksIterator.create(baseDb, readOptions.ptr());
	}

	// -----------------------------------------------------------------------
	// Snapshot
	// -----------------------------------------------------------------------

	/// Creates a snapshot of the current DB state.
	/// The returned snapshot must be closed after use.
	public Snapshot getSnapshot() {
		return RocksDB.createSnapshot(baseDb);
	}

	// -----------------------------------------------------------------------
	// Flush
	// -----------------------------------------------------------------------

	/// Flushes all memtable data to SST files on disk.
	public void flush(FlushOptions flushOptions) {
		RocksDB.flush(baseDb, flushOptions);
	}

	/// Flushes the WAL (write-ahead log) to disk.
	///
	/// @param sync if `true`, performs an `fsync` after writing
	public void flushWal(boolean sync) {
		RocksDB.flushWal(baseDb, sync);
	}

	// -----------------------------------------------------------------------
	// DB Properties
	// -----------------------------------------------------------------------

	/// Returns the value of a DB property as a string, or [Optional#empty()] if not supported.
	public Optional<String> getProperty(Property property) {
		return RocksDB.getProperty(baseDb, property);
	}

	/// Returns the value of a numeric DB property, or [OptionalLong#empty()] if not supported.
	public OptionalLong getLongProperty(Property property) {
		return RocksDB.getLongProperty(baseDb, property);
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
