package io.github.dfa1.rocksdbffm;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

/// FFM wrapper for a blob-enabled read-write `rocksdb_t*` instance.
///
/// BlobDB is a regular RocksDB opened with blob file options set in [Options].
/// Large values (≥ [Options#setMinBlobSize]) are stored in separate blob files
/// rather than inline in SSTs, reducing write amplification for value-heavy workloads.
///
/// Obtain via [RocksDB#openWithBlobFiles]:
///
/// ```
/// try (Options opts = Options.newOptions()
///         .setCreateIfMissing(true)
///         .setEnableBlobFiles(true)
///         .setMinBlobSize(MemorySize.ofKB(4))) {
///     try (var db = RocksDB.openWithBlobFiles(opts, path)) {
///         db.put("key".getBytes(), largeValue);
///     }
/// }
/// ```
///
/// Blob-specific statistics are available via [Property#BLOB_STATS],
/// [Property#NUM_BLOB_FILES], [Property#TOTAL_BLOB_FILE_SIZE], etc.
public final class BlobDB extends NativeObject {

	private final WriteOptions writeOpts;
	private final ReadOptions readOpts;

	BlobDB(MemorySegment ptr, WriteOptions writeOpts, ReadOptions readOpts) {
		super(ptr);
		this.writeOpts = writeOpts;
		this.readOpts = readOpts;
	}

	// -----------------------------------------------------------------------
	// Put
	// -----------------------------------------------------------------------

	/// Stores `value` under `key`. Slow path: copies key/value into native memory.
	///
	/// @param key the key bytes
	/// @param value the value bytes
	public void put(byte[] key, byte[] value) {
		RocksDB.putBytes(ptr(), writeOpts.ptr(), key, value);
	}

	/// Stores `value` under `key` using the caller's [Arena] for native allocation.
	///
	/// @param arena the arena used to allocate native key/value segments
	/// @param key the key bytes
	/// @param value the value bytes
	public void put(Arena arena, byte[] key, byte[] value) {
		RocksDB.putBytes(arena, ptr(), writeOpts.ptr(), key, value);
	}

	/// Zero-copy put: wraps the direct buffers' native memory without heap→native copy.
	///
	/// @param key direct [ByteBuffer] containing the key
	/// @param value direct [ByteBuffer] containing the value
	public void put(ByteBuffer key, ByteBuffer value) {
		RocksDB.putSegment(ptr(), writeOpts.ptr(),
				MemorySegment.ofBuffer(key), key.remaining(),
				MemorySegment.ofBuffer(value), value.remaining());
	}

	/// Zero-copy put: caller supplies pre-allocated native segments.
	///
	/// @param key native segment containing the key
	/// @param value native segment containing the value
	public void put(MemorySegment key, MemorySegment value) {
		RocksDB.putSegment(ptr(), writeOpts.ptr(), key, key.byteSize(), value, value.byteSize());
	}

	// -----------------------------------------------------------------------
	// Get
	// -----------------------------------------------------------------------

	/// Get via PinnableSlice — pins data directly from the block/blob cache.
	///
	/// @param key the key bytes to look up
	/// @return the value bytes, or `null` if not found
	public byte[] get(byte[] key) {
		return RocksDB.getBytes(ptr(), readOpts.ptr(), key);
	}

	/// Get with explicit [ReadOptions], e.g. for snapshot-pinned reads.
	///
	/// @param readOptions options controlling the read (e.g. snapshot)
	/// @param key the key bytes to look up
	/// @return the value bytes, or `null` if not found
	public byte[] get(ReadOptions readOptions, byte[] key) {
		return RocksDB.getBytes(ptr(), readOptions.ptr(), key);
	}

	/// Single-copy get via PinnableSlice + direct output [ByteBuffer].
	///
	/// @param key direct [ByteBuffer] containing the key
	/// @param value direct [ByteBuffer] to write the value into
	/// @return the actual value length, or `-1` if not found
	public int get(ByteBuffer key, ByteBuffer value) {
		return RocksDB.getIntoBuffer(ptr(), readOpts.ptr(),
				MemorySegment.ofBuffer(key), key.remaining(), value);
	}

	/// Zero-copy get via PinnableSlice into a caller-supplied native segment.
	///
	/// @param key native segment containing the key
	/// @param value native segment to write the value into
	/// @return the actual value length in bytes
	public long get(MemorySegment key, MemorySegment value) {
		return RocksDB.getIntoSegment(ptr(), readOpts.ptr(), key, key.byteSize(), value);
	}

	// -----------------------------------------------------------------------
	// Delete
	// -----------------------------------------------------------------------

	/// Removes `key` from the database. Slow path: copies the key into native memory.
	///
	/// @param key the key bytes to remove
	public void delete(byte[] key) {
		RocksDB.deleteBytes(ptr(), writeOpts.ptr(), key);
	}

	/// Zero-copy for direct [ByteBuffer]s.
	///
	/// @param key direct [ByteBuffer] containing the key to remove
	public void delete(ByteBuffer key) {
		RocksDB.deleteSegment(ptr(), writeOpts.ptr(), MemorySegment.ofBuffer(key), key.remaining());
	}

	/// Zero-copy native-first path.
	///
	/// @param key native segment containing the key to remove
	public void delete(MemorySegment key) {
		RocksDB.deleteSegment(ptr(), writeOpts.ptr(), key, key.byteSize());
	}

	// -----------------------------------------------------------------------
	// Write (batch)
	// -----------------------------------------------------------------------

	public void write(WriteBatch batch) {
		RocksDB.writeBatch(ptr(), writeOpts.ptr(), batch);
	}

	// -----------------------------------------------------------------------
	// Snapshot
	// -----------------------------------------------------------------------

	/// Creates a snapshot of the current DB state. Must be closed after use.
	///
	/// @return a new [Snapshot] pinning the current sequence number
	public Snapshot getSnapshot() {
		return RocksDB.createSnapshot(ptr());
	}

	// -----------------------------------------------------------------------
	// Iterator
	// -----------------------------------------------------------------------

	/// Returns a new iterator using the database's default read options.
	///
	/// @return a new [RocksIterator] positioned before the first entry
	public RocksIterator newIterator() {
		return RocksIterator.create(ptr(), readOpts.ptr());
	}

	/// Returns a new iterator using the supplied [ReadOptions].
	///
	/// @param readOptions options controlling iteration (e.g. snapshot)
	/// @return a new [RocksIterator] positioned before the first entry
	public RocksIterator newIterator(ReadOptions readOptions) {
		return RocksIterator.create(ptr(), readOptions.ptr());
	}

	// -----------------------------------------------------------------------
	// Flush
	// -----------------------------------------------------------------------

	/// Flushes all memtable data to SST/blob files. Blocks when [FlushOptions#isWait()] is `true`.
	///
	/// @param flushOptions options controlling flush behaviour (e.g. wait)
	public void flush(FlushOptions flushOptions) {
		RocksDB.flush(ptr(), flushOptions);
	}

	/// Flushes the WAL to disk.
	///
	/// @param sync if `true`, performs an `fsync` after writing
	public void flushWal(boolean sync) {
		RocksDB.flushWal(ptr(), sync);
	}

	// -----------------------------------------------------------------------
	// DB Properties
	// -----------------------------------------------------------------------

	/// Returns the value of a DB property as a string, or [Optional#empty()] if not supported.
	/// Use [Property#BLOB_STATS], [Property#NUM_BLOB_FILES], etc. for blob-specific metrics.
	///
	/// @param property the property to query
	/// @return the property value, or empty if the property is not supported
	public Optional<String> getProperty(Property property) {
		return RocksDB.getProperty(ptr(), property);
	}

	/// Returns the value of a numeric DB property, or [OptionalLong#empty()] if not supported.
	///
	/// @param property the numeric property to query
	/// @return the property value as a `long`, or empty if not supported
	public OptionalLong getLongProperty(Property property) {
		return RocksDB.getLongProperty(ptr(), property);
	}

	// -----------------------------------------------------------------------
	// SST File Ingest
	// -----------------------------------------------------------------------

	/// Ingests SST files produced by [SstFileWriter] into the database.
	///
	/// @param files paths to SST files to ingest
	/// @param options options controlling the ingest behaviour
	public void ingestExternalFile(List<Path> files, IngestExternalFileOptions options) {
		if (files.isEmpty()) {
			return;
		}
		RocksDB.ingestExternalFile(ptr(), files, options);
	}

	// -----------------------------------------------------------------------
	// AutoCloseable
	// -----------------------------------------------------------------------

	@Override
	protected void tryClose(MemorySegment ptr) throws Throwable {
		writeOpts.close();
		readOpts.close();
		RocksDB.close(ptr);
	}
}
