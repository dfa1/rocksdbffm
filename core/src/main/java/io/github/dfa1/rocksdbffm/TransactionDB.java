package io.github.dfa1.rocksdbffm;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.util.Optional;
import java.util.OptionalLong;

/// FFM wrapper for `rocksdb_transactiondb_t` — a RocksDB database with pessimistic
/// (locking) transaction support.
/// ```
/// try (TransactionDBOptions txnDbOpts = TransactionDBOptions.newTransactionDBOptions();
///      Options opts = Options.newOptions().setCreateIfMissing(true);
///      TransactionDB db = TransactionDB.open(opts, txnDbOpts, path)) {
///     try (WriteOptions wo = WriteOptions.newWriteOptions();
///          Transaction txn = db.beginTransaction(wo)) {
///         txn.put("key".getBytes(), "value".getBytes());
///         txn.commit();
///     }
/// }
/// ```
public final class TransactionDB extends NativeObject {

	// -----------------------------------------------------------------------
	// Method handles
	// -----------------------------------------------------------------------

	/// `void rocksdb_transactiondb_close(rocksdb_transactiondb_t* txn_db);`
	private static final MethodHandle MH_CLOSE;
	/// `rocksdb_transaction_t* rocksdb_transaction_begin(rocksdb_transactiondb_t* txn_db, const rocksdb_writeoptions_t* write_options, const rocksdb_transaction_options_t* txn_options, rocksdb_transaction_t* old_txn);`
	private static final MethodHandle MH_BEGIN;
	/// `const rocksdb_snapshot_t* rocksdb_transactiondb_create_snapshot(rocksdb_transactiondb_t* txn_db);`
	private static final MethodHandle MH_CREATE_SNAPSHOT;
	/// `void rocksdb_transactiondb_flush(rocksdb_transactiondb_t* txn_db, const rocksdb_flushoptions_t* options, char** errptr);`
	private static final MethodHandle MH_FLUSH;
	/// `void rocksdb_transactiondb_flush_wal(rocksdb_transactiondb_t* txn_db, unsigned char sync, char** errptr);`
	private static final MethodHandle MH_FLUSH_WAL;
	/// `char* rocksdb_transactiondb_property_value(rocksdb_transactiondb_t* db, const char* propname);`
	private static final MethodHandle MH_PROPERTY_VALUE;
	/// `int rocksdb_transactiondb_property_int(rocksdb_transactiondb_t* db, const char* propname, uint64_t* out_val);`
	private static final MethodHandle MH_PROPERTY_INT;

	// Direct (non-transactional) operations on the TransactionDB
	/// `void rocksdb_transactiondb_put(rocksdb_transactiondb_t* txn_db, const rocksdb_writeoptions_t* options, const char* key, size_t klen, const char* val, size_t vlen, char** errptr);`
	private static final MethodHandle MH_PUT;
	/// `void rocksdb_transactiondb_delete(rocksdb_transactiondb_t* txn_db, const rocksdb_writeoptions_t* options, const char* key, size_t klen, char** errptr);`
	private static final MethodHandle MH_DELETE;
	/// `char* rocksdb_transactiondb_get(rocksdb_transactiondb_t* txn_db, const rocksdb_readoptions_t* options, const char* key, size_t klen, size_t* vlen, char** errptr);`
	private static final MethodHandle MH_GET;

	// Column-family variants
	/// `void rocksdb_transactiondb_put_cf(rocksdb_transactiondb_t* txn_db, const rocksdb_writeoptions_t* options, rocksdb_column_family_handle_t* column_family, const char* key, size_t keylen, const char* val, size_t vallen, char** errptr);`
	private static final MethodHandle MH_PUT_CF;
	/// `void rocksdb_transactiondb_delete_cf(rocksdb_transactiondb_t* txn_db, const rocksdb_writeoptions_t* options, rocksdb_column_family_handle_t* column_family, const char* key, size_t keylen, char** errptr);`
	private static final MethodHandle MH_DELETE_CF;
	/// `rocksdb_pinnableslice_t* rocksdb_transactiondb_get_pinned_cf(rocksdb_transactiondb_t* txn_db, const rocksdb_readoptions_t* options, rocksdb_column_family_handle_t* column_family, const char* key, size_t keylen, char** errptr);`
	private static final MethodHandle MH_GET_PINNED_CF;
	/// `rocksdb_iterator_t* rocksdb_transactiondb_create_iterator_cf(rocksdb_transactiondb_t* txn_db, const rocksdb_readoptions_t* options, rocksdb_column_family_handle_t* column_family);`
	private static final MethodHandle MH_CREATE_ITERATOR_CF;
	/// `void rocksdb_transactiondb_flush_cf(rocksdb_transactiondb_t* txn_db, const rocksdb_flushoptions_t* options, rocksdb_column_family_handle_t* column_family, char** errptr);`
	private static final MethodHandle MH_FLUSH_CF;
	/// `const char* rocksdb_pinnableslice_value(const rocksdb_pinnableslice_t* t, size_t* vlen);`
	private static final MethodHandle MH_PINNABLESLICE_VALUE;
	/// `void rocksdb_pinnableslice_destroy(rocksdb_pinnableslice_t* v);`
	private static final MethodHandle MH_PINNABLESLICE_DESTROY;


	static {
		MH_CLOSE = NativeLibrary.lookup("rocksdb_transactiondb_close",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

		MH_BEGIN = NativeLibrary.lookup("rocksdb_transaction_begin",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_PUT = NativeLibrary.lookup("rocksdb_transactiondb_put",
				FunctionDescriptor.ofVoid(
						ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS));

		MH_DELETE = NativeLibrary.lookup("rocksdb_transactiondb_delete",
				FunctionDescriptor.ofVoid(
						ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS));

		MH_GET = NativeLibrary.lookup("rocksdb_transactiondb_get",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_PUT_CF = NativeLibrary.lookup("rocksdb_transactiondb_put_cf",
				FunctionDescriptor.ofVoid(
						ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS));

		MH_DELETE_CF = NativeLibrary.lookup("rocksdb_transactiondb_delete_cf",
				FunctionDescriptor.ofVoid(
						ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS));

		MH_GET_PINNED_CF = NativeLibrary.lookup("rocksdb_transactiondb_get_pinned_cf",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS));

		MH_CREATE_ITERATOR_CF = NativeLibrary.lookup("rocksdb_transactiondb_create_iterator_cf",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_FLUSH_CF = NativeLibrary.lookup("rocksdb_transactiondb_flush_cf",
				FunctionDescriptor.ofVoid(
						ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_PINNABLESLICE_VALUE = NativeLibrary.lookup("rocksdb_pinnableslice_value",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_PINNABLESLICE_DESTROY = NativeLibrary.lookup("rocksdb_pinnableslice_destroy",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));


		MH_CREATE_SNAPSHOT = NativeLibrary.lookup("rocksdb_transactiondb_create_snapshot",
				FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_FLUSH = NativeLibrary.lookup("rocksdb_transactiondb_flush",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_FLUSH_WAL = NativeLibrary.lookup("rocksdb_transactiondb_flush_wal",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_BYTE, ValueLayout.ADDRESS));

		MH_PROPERTY_VALUE = NativeLibrary.lookup("rocksdb_transactiondb_property_value",
				FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_PROPERTY_INT = NativeLibrary.lookup("rocksdb_transactiondb_property_int",
				FunctionDescriptor.of(ValueLayout.JAVA_INT,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));
	}

	// -----------------------------------------------------------------------
	// Instance state
	// -----------------------------------------------------------------------

	private final MemorySegment baseDb;  // rocksdb_t* — for CF management and CF property queries
	private final WriteOptions writeOpts; // default write options for direct ops
	private final ReadOptions readOpts;  // default read options for direct ops

	TransactionDB(MemorySegment ptr, MemorySegment baseDb, WriteOptions writeOpts, ReadOptions readOpts) {
		super(ptr);
		this.baseDb = baseDb;
		this.writeOpts = writeOpts;
		this.readOpts = readOpts;
	}

	// -----------------------------------------------------------------------
	// Flush
	// -----------------------------------------------------------------------

	/// Flushes all memtable data to SST files on disk.
	/// Blocks until the flush completes when [FlushOptions#isWait()] is `true`.
	public void flush(FlushOptions flushOptions) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = RocksDB.errHolder(arena);
			MH_FLUSH.invokeExact(ptr(), flushOptions.ptr(), err);
			RocksDB.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("flush failed", t);
		}
	}

	/// Flushes the WAL (write-ahead log) to disk.
	///
	/// @param sync if `true`, performs an `fsync` after writing
	public void flushWal(boolean sync) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = RocksDB.errHolder(arena);
			MH_FLUSH_WAL.invokeExact(ptr(), sync ? (byte) 1 : (byte) 0, err);
			RocksDB.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("flushWal failed", t);
		}
	}

	// -----------------------------------------------------------------------
	// Snapshot
	// -----------------------------------------------------------------------

	/// Creates a snapshot of the current TransactionDB state.
	/// The returned snapshot must be closed after use.
	public Snapshot getSnapshot() {
		try {
			MemorySegment snapPtr = (MemorySegment) MH_CREATE_SNAPSHOT.invokeExact(ptr());
			return new Snapshot(ptr(), snapPtr);
		} catch (Throwable t) {
			throw RocksDBException.wrap("getSnapshot failed", t);
		}
	}

	// -----------------------------------------------------------------------
	// Transaction API
	// -----------------------------------------------------------------------

	/// Begins a new transaction using the supplied write options and default
	/// transaction options.
	public Transaction beginTransaction(WriteOptions writeOptions) {
		try (TransactionOptions txnOpts = TransactionOptions.newTransactionOptions()) {
			return beginTransaction(writeOptions, txnOpts);
		}
	}

	/// Begins a new transaction using the supplied write options and transaction options.
	public Transaction beginTransaction(WriteOptions writeOptions, TransactionOptions txnOptions) {
		try {
			MemorySegment txnPtr = (MemorySegment) MH_BEGIN.invokeExact(
					ptr(), writeOptions.ptr(), txnOptions.ptr(), MemorySegment.NULL);
			return new Transaction(txnPtr);
		} catch (Throwable t) {
			throw new RocksDBException("beginTransaction failed", t);
		}
	}

	// -----------------------------------------------------------------------
	// Direct (non-transactional) operations
	// -----------------------------------------------------------------------

	/// Direct put, bypassing any active transaction. Slow path: allocates native memory.
	public void put(byte[] key, byte[] value) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = RocksDB.errHolder(arena);
			MemorySegment k = RocksDB.toNative(arena, key);
			MemorySegment v = RocksDB.toNative(arena, value);
			MH_PUT.invokeExact(ptr(), writeOpts.ptr(), k, (long) key.length, v, (long) value.length, err);
			RocksDB.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("put failed", t);
		}
	}

	/// Zero-copy put: wraps the direct buffers' native memory without heap→native copy.
	public void put(java.nio.ByteBuffer key, java.nio.ByteBuffer value) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = RocksDB.errHolder(arena);
			MH_PUT.invokeExact(ptr(), writeOpts.ptr(),
					MemorySegment.ofBuffer(key), (long) key.remaining(),
					MemorySegment.ofBuffer(value), (long) value.remaining(),
					err);
			RocksDB.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("put failed", t);
		}
	}

	/// Zero-copy put: caller supplies pre-allocated native segments.
	public void put(MemorySegment key, MemorySegment value) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = RocksDB.errHolder(arena);
			MH_PUT.invokeExact(ptr(), writeOpts.ptr(), key, key.byteSize(), value, value.byteSize(), err);
			RocksDB.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("put failed", t);
		}
	}

	/// Direct get with explicit ReadOptions (e.g. for snapshot-pinned reads). Returns `null` if not found.
	public byte[] get(ReadOptions readOptions, byte[] key) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = RocksDB.errHolder(arena);
			MemorySegment k = RocksDB.toNative(arena, key);
			MemorySegment valLenSeg = arena.allocate(ValueLayout.JAVA_LONG);

			MemorySegment valPtr = (MemorySegment) MH_GET.invokeExact(
					ptr(), readOptions.ptr(), k, (long) key.length, valLenSeg, err);

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

	/// Direct get, reading committed data only. Returns `null` if not found. Slow path.
	public byte[] get(byte[] key) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = RocksDB.errHolder(arena);
			MemorySegment k = RocksDB.toNative(arena, key);
			MemorySegment valLenSeg = arena.allocate(ValueLayout.JAVA_LONG);

			MemorySegment valPtr = (MemorySegment) MH_GET.invokeExact(
					ptr(), readOpts.ptr(), k, (long) key.length, valLenSeg, err);

			RocksDB.checkError(err);

			if (MemorySegment.NULL.equals(valPtr)) {
				return null;
			}

			long valLen = valLenSeg.get(ValueLayout.JAVA_LONG, 0);
			byte[] result = valPtr.reinterpret(valLen).toArray(ValueLayout.JAVA_BYTE);
			RocksDB.free(valPtr);
			return result;
		} catch (Throwable t) {
			throw RocksDBException.wrap("get failed", t);
		}
	}

	/// Single-copy get + direct output [java.nio.ByteBuffer].
	/// Returns the actual value length, or -1 if not found.
	public int get(java.nio.ByteBuffer key, java.nio.ByteBuffer value) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = RocksDB.errHolder(arena);
			MemorySegment valLenSeg = arena.allocate(ValueLayout.JAVA_LONG);
			MemorySegment valPtr = (MemorySegment) MH_GET.invokeExact(
					ptr(), readOpts.ptr(),
					MemorySegment.ofBuffer(key), (long) key.remaining(),
					valLenSeg, err);
			RocksDB.checkError(err);
			if (MemorySegment.NULL.equals(valPtr)) {
				return -1;
			}
			long valLen = valLenSeg.get(ValueLayout.JAVA_LONG, 0);
			int toCopy = (int) Math.min(valLen, value.remaining());
			MemorySegment.ofBuffer(value).copyFrom(valPtr.reinterpret(toCopy));
			value.position(value.position() + toCopy);
			RocksDB.free(valPtr);
			return (int) valLen;
		} catch (Throwable t) {
			throw RocksDBException.wrap("get failed", t);
		}
	}

	/// Zero-copy get into a caller-supplied native segment.
	/// Returns the actual value length, or -1 if not found.
	public long get(MemorySegment key, MemorySegment value) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = RocksDB.errHolder(arena);
			MemorySegment valLenSeg = arena.allocate(ValueLayout.JAVA_LONG);
			MemorySegment valPtr = (MemorySegment) MH_GET.invokeExact(
					ptr(), readOpts.ptr(), key, key.byteSize(), valLenSeg, err);
			RocksDB.checkError(err);
			if (MemorySegment.NULL.equals(valPtr)) {
				return -1L;
			}
			long valLen = valLenSeg.get(ValueLayout.JAVA_LONG, 0);
			long toCopy = Math.min(valLen, value.byteSize());
			value.copyFrom(valPtr.reinterpret(toCopy));
			RocksDB.free(valPtr);
			return valLen;
		} catch (Throwable t) {
			throw RocksDBException.wrap("get failed", t);
		}
	}

	// -----------------------------------------------------------------------
	// DB Properties
	// -----------------------------------------------------------------------
	//
	public Optional<String> getProperty(Property property) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment propSeg = arena.allocateFrom(property.propertyName());
			MemorySegment result = (MemorySegment) MH_PROPERTY_VALUE.invokeExact(ptr(), propSeg);
			if (MemorySegment.NULL.equals(result)) {
				return Optional.empty();
			}
			String value = result.reinterpret(Long.MAX_VALUE).getString(0);
			RocksDB.free(result);
			return Optional.of(value);
		} catch (Throwable t) {
			throw RocksDBException.wrap("getProperty failed", t);
		}
	}

	/// @see ReadWriteDB#getLongProperty(Property)
	public OptionalLong getLongProperty(Property property) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment propSeg = arena.allocateFrom(property.propertyName());
			MemorySegment out = arena.allocate(ValueLayout.JAVA_LONG);
			int rc = (int) MH_PROPERTY_INT.invokeExact(ptr(), propSeg, out);
			if (rc != 0) {
				return OptionalLong.empty();
			}
			return OptionalLong.of(out.get(ValueLayout.JAVA_LONG, 0));
		} catch (Throwable t) {
			throw RocksDBException.wrap("getLongProperty failed", t);
		}
	}

	/// Direct delete, bypassing any active transaction. Slow path.
	public void delete(byte[] key) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = RocksDB.errHolder(arena);
			MemorySegment k = RocksDB.toNative(arena, key);
			MH_DELETE.invokeExact(ptr(), writeOpts.ptr(), k, (long) key.length, err);
			RocksDB.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("delete failed", t);
		}
	}

	/// Zero-copy for direct [java.nio.ByteBuffer]s.
	public void delete(java.nio.ByteBuffer key) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = RocksDB.errHolder(arena);
			MH_DELETE.invokeExact(ptr(), writeOpts.ptr(),
					MemorySegment.ofBuffer(key), (long) key.remaining(), err);
			RocksDB.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("delete failed", t);
		}
	}

	/// Zero-copy native-first path.
	public void delete(MemorySegment key) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = RocksDB.errHolder(arena);
			MH_DELETE.invokeExact(ptr(), writeOpts.ptr(), key, key.byteSize(), err);
			RocksDB.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("delete failed", t);
		}
	}

	// -----------------------------------------------------------------------
	// Column family management
	// -----------------------------------------------------------------------

	/// Creates a new column family described by `descriptor` and returns its handle.
	public ColumnFamilyHandle createColumnFamily(ColumnFamilyDescriptor descriptor) {
		return RocksDB.createCf(baseDb, descriptor);
	}

	/// Drops the column family identified by `handle`.
	public void dropColumnFamily(ColumnFamilyHandle handle) {
		RocksDB.dropCf(baseDb, handle);
	}

	// -----------------------------------------------------------------------
	// Put — column family overloads
	// -----------------------------------------------------------------------

	/// Stores `value` under `key` in `cf`, bypassing any active transaction. Slow path.
	public void put(ColumnFamilyHandle cf, byte[] key, byte[] value) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = RocksDB.errHolder(arena);
			MH_PUT_CF.invokeExact(ptr(), writeOpts.ptr(), cf.ptr(),
					RocksDB.toNative(arena, key), (long) key.length,
					RocksDB.toNative(arena, value), (long) value.length, err);
			RocksDB.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("put failed", t);
		}
	}

	/// Zero-copy put into `cf` for direct [java.nio.ByteBuffer]s.
	public void put(ColumnFamilyHandle cf, java.nio.ByteBuffer key, java.nio.ByteBuffer value) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = RocksDB.errHolder(arena);
			MH_PUT_CF.invokeExact(ptr(), writeOpts.ptr(), cf.ptr(),
					MemorySegment.ofBuffer(key), (long) key.remaining(),
					MemorySegment.ofBuffer(value), (long) value.remaining(), err);
			RocksDB.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("put failed", t);
		}
	}

	/// Zero-copy put into `cf` for [MemorySegment]s.
	public void put(ColumnFamilyHandle cf, MemorySegment key, MemorySegment value) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = RocksDB.errHolder(arena);
			MH_PUT_CF.invokeExact(ptr(), writeOpts.ptr(), cf.ptr(),
					key, key.byteSize(), value, value.byteSize(), err);
			RocksDB.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("put failed", t);
		}
	}

	// -----------------------------------------------------------------------
	// Get — column family overloads
	// -----------------------------------------------------------------------

	/// Get from `cf` via PinnableSlice. Returns `null` if not found.
	public byte[] get(ColumnFamilyHandle cf, byte[] key) {
		return get(cf, readOpts, key);
	}

	/// Get from `cf` with explicit [ReadOptions]. Returns `null` if not found.
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
			throw RocksDBException.wrap("get failed", t);
		}
	}

	// -----------------------------------------------------------------------
	// Delete — column family overloads
	// -----------------------------------------------------------------------

	/// Removes `key` from `cf`, bypassing any active transaction. Slow path.
	public void delete(ColumnFamilyHandle cf, byte[] key) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = RocksDB.errHolder(arena);
			MH_DELETE_CF.invokeExact(ptr(), writeOpts.ptr(), cf.ptr(),
					RocksDB.toNative(arena, key), (long) key.length, err);
			RocksDB.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("delete failed", t);
		}
	}

	/// Zero-copy delete from `cf` for direct [java.nio.ByteBuffer]s.
	public void delete(ColumnFamilyHandle cf, java.nio.ByteBuffer key) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = RocksDB.errHolder(arena);
			MH_DELETE_CF.invokeExact(ptr(), writeOpts.ptr(), cf.ptr(),
					MemorySegment.ofBuffer(key), (long) key.remaining(), err);
			RocksDB.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("delete failed", t);
		}
	}

	/// Zero-copy delete from `cf` for [MemorySegment]s.
	public void delete(ColumnFamilyHandle cf, MemorySegment key) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = RocksDB.errHolder(arena);
			MH_DELETE_CF.invokeExact(ptr(), writeOpts.ptr(), cf.ptr(), key, key.byteSize(), err);
			RocksDB.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("delete failed", t);
		}
	}

	// -----------------------------------------------------------------------
	// Iterator — column family overloads
	// -----------------------------------------------------------------------

	/// Returns a new iterator scoped to `cf` using the database's default read options.
	public RocksIterator newIterator(ColumnFamilyHandle cf) {
		try {
			MemorySegment iterPtr = (MemorySegment) MH_CREATE_ITERATOR_CF.invokeExact(
					ptr(), readOpts.ptr(), cf.ptr());
			return RocksIterator.create(iterPtr);
		} catch (Throwable t) {
			throw RocksDBException.wrap("newIterator failed", t);
		}
	}

	/// Returns a new iterator scoped to `cf` with explicit [ReadOptions].
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
	// Flush — column family overloads
	// -----------------------------------------------------------------------

	/// Flushes the memtable for `cf` to SST files.
	public void flush(ColumnFamilyHandle cf, FlushOptions flushOptions) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = RocksDB.errHolder(arena);
			MH_FLUSH_CF.invokeExact(ptr(), flushOptions.ptr(), cf.ptr(), err);
			RocksDB.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("flush failed", t);
		}
	}

	// -----------------------------------------------------------------------
	// DB Properties — column family overloads
	// -----------------------------------------------------------------------

	/// Returns the value of a property for `cf`, or [Optional#empty()] if not supported.
	public Optional<String> getProperty(ColumnFamilyHandle cf, Property property) {
		return RocksDB.getPropertyCf(baseDb, cf, property);
	}

	/// Returns the value of a numeric property for `cf`, or [OptionalLong#empty()] if not supported.
	public OptionalLong getLongProperty(ColumnFamilyHandle cf, Property property) {
		return RocksDB.getLongPropertyCf(baseDb, cf, property);
	}

	// -----------------------------------------------------------------------
	// AutoCloseable
	// -----------------------------------------------------------------------

	@Override
	protected void tryClose(MemorySegment ptr) throws Throwable {
		writeOpts.close();
		readOpts.close();
		MH_CLOSE.invokeExact(ptr);
	}

}
