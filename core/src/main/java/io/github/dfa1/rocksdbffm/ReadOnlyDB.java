package io.github.dfa1.rocksdbffm;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.OptionalLong;

/// FFM wrapper for a read-only `rocksdb_t*` instance.
///
/// Obtain via [RocksDB#openReadOnly].
///
/// ```
/// try (var db = RocksDB.openReadOnly(path)) {
///     byte[] value = db.get("key".getBytes());
/// }
/// ```
public final class ReadOnlyDB extends NativeObject {

	private final ReadOptions readOpts;

	ReadOnlyDB(MemorySegment ptr, ReadOptions readOpts) {
		super(ptr);
		this.readOpts = readOpts;
	}

	// -----------------------------------------------------------------------
	// Get
	// -----------------------------------------------------------------------

	/// Returns the value for `key`, or `null` if the key does not exist.
	/// Uses PinnableSlice to avoid an intermediate copy from the block cache.
	public byte[] get(byte[] key) {
		return RocksDB.getBytes(ptr(), readOpts.ptr(), key);
	}

	/// Get with explicit [ReadOptions], e.g. for snapshot-pinned reads. Returns `null` if not found.
	public byte[] get(ReadOptions readOptions, byte[] key) {
		return RocksDB.getBytes(ptr(), readOptions.ptr(), key);
	}

	/// Single-copy get via PinnableSlice + direct output [ByteBuffer].
	/// Returns the actual value length, or -1 if not found.
	public int get(ByteBuffer key, ByteBuffer value) {
		return RocksDB.getIntoBuffer(ptr(), readOpts.ptr(),
				MemorySegment.ofBuffer(key), key.remaining(), value);
	}

	/// Zero-copy get via PinnableSlice into a caller-supplied native segment.
	/// Returns the actual value length.
	public long get(MemorySegment key, MemorySegment value) {
		return RocksDB.getIntoSegment(ptr(), readOpts.ptr(), key, key.byteSize(), value);
	}

	/// Returns the value for `key` in `cf`, or `null` if not found.
	public byte[] get(ColumnFamilyHandle cf, byte[] key) {
		return RocksDB.getCfBytes(ptr(), readOpts.ptr(), cf, key);
	}

	/// Get from `cf` with explicit [ReadOptions]. Returns `null` if not found.
	public byte[] get(ColumnFamilyHandle cf, ReadOptions readOptions, byte[] key) {
		return RocksDB.getCfBytes(ptr(), readOptions.ptr(), cf, key);
	}

	/// Single-copy get from `cf` via PinnableSlice + direct output [ByteBuffer].
	/// Returns the actual value length, or -1 if not found.
	public int get(ColumnFamilyHandle cf, ByteBuffer key, ByteBuffer value) {
		return RocksDB.getCfIntoBuffer(ptr(), readOpts.ptr(), cf,
				MemorySegment.ofBuffer(key), key.remaining(), value);
	}

	/// Zero-copy get from `cf` into a caller-supplied native segment.
	/// Returns the actual value length.
	public long get(ColumnFamilyHandle cf, MemorySegment key, MemorySegment value) {
		return RocksDB.getCfIntoSegment(ptr(), readOpts.ptr(), cf, key, key.byteSize(), value);
	}

	// -----------------------------------------------------------------------
	// Iterator
	// -----------------------------------------------------------------------

	/// Returns a new iterator using the database's default read options.
	public RocksIterator newIterator() {
		return RocksIterator.create(ptr(), readOpts.ptr());
	}

	/// Returns a new iterator using the supplied [ReadOptions].
	public RocksIterator newIterator(ReadOptions readOptions) {
		return RocksIterator.create(ptr(), readOptions.ptr());
	}

	/// Returns a new iterator scoped to `cf` using the default read options.
	public RocksIterator newIterator(ColumnFamilyHandle cf) {
		return RocksDB.createIteratorCf(ptr(), readOpts.ptr(), cf);
	}

	/// Returns a new iterator scoped to `cf` with explicit [ReadOptions].
	public RocksIterator newIterator(ColumnFamilyHandle cf, ReadOptions readOptions) {
		return RocksDB.createIteratorCf(ptr(), readOptions.ptr(), cf);
	}

	// -----------------------------------------------------------------------
	// Snapshot
	// -----------------------------------------------------------------------

	/// Creates a snapshot of the current DB state. Must be closed after use.
	public Snapshot getSnapshot() {
		return RocksDB.createSnapshot(ptr());
	}

	// -----------------------------------------------------------------------
	// DB Properties
	// -----------------------------------------------------------------------

	/// Returns the value of a DB property as a string, or [Optional#empty()] if not supported.
	public Optional<String> getProperty(Property property) {
		return RocksDB.getProperty(ptr(), property);
	}

	/// Returns the value of a numeric DB property, or [OptionalLong#empty()] if not supported.
	public OptionalLong getLongProperty(Property property) {
		return RocksDB.getLongProperty(ptr(), property);
	}

	/// Returns the value of a property scoped to `cf`, or [Optional#empty()] if not supported.
	public Optional<String> getProperty(ColumnFamilyHandle cf, Property property) {
		return RocksDB.getPropertyCf(ptr(), cf, property);
	}

	/// Returns the value of a numeric property scoped to `cf`, or [OptionalLong#empty()] if not supported.
	public OptionalLong getLongProperty(ColumnFamilyHandle cf, Property property) {
		return RocksDB.getLongPropertyCf(ptr(), cf, property);
	}

	// -----------------------------------------------------------------------
	// AutoCloseable
	// -----------------------------------------------------------------------

	@Override
	protected void tryClose(MemorySegment ptr) throws Throwable {
		readOpts.close();
		RocksDB.close(ptr);
	}
}
