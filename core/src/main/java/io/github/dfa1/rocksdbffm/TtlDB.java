package io.github.dfa1.rocksdbffm;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

/// FFM wrapper for a TTL-aware read-write `rocksdb_t*` instance.
///
/// Obtain via [RocksDB#openWithTtl].
///
/// Keys are lazily expired during the next compaction that covers their range.
/// A [Duration#ZERO] TTL disables expiry entirely.
///
/// ```
/// try (var db = RocksDB.openWithTtl(path, Duration.ofSeconds(60))) {
///     db.put("key".getBytes(), "value".getBytes());
///     byte[] value = db.get("key".getBytes());
/// }
/// ```
public final class TtlDB extends NativeObject {

	private final WriteOptions writeOpts;
	private final ReadOptions readOpts;
	private final Duration ttl;

	TtlDB(MemorySegment ptr, WriteOptions writeOpts, ReadOptions readOpts, Duration ttl) {
		super(ptr);
		this.writeOpts = writeOpts;
		this.readOpts = readOpts;
		this.ttl = ttl;
	}

	/// Returns the TTL configured when this database was opened.
	/// [Duration#ZERO] means expiry is disabled.
	public Duration getTtl() {
		return ttl;
	}

	// -----------------------------------------------------------------------
	// Put
	// -----------------------------------------------------------------------

	/// Stores `value` under `key`. Slow path: copies key/value into native memory.
	public void put(byte[] key, byte[] value) {
		RocksDB.putBytes(ptr(), writeOpts.ptr(), key, value);
	}

	/// Stores `value` under `key` using the caller's [Arena] for native allocation.
	public void put(Arena arena, byte[] key, byte[] value) {
		RocksDB.putBytes(arena, ptr(), writeOpts.ptr(), key, value);
	}

	/// Zero-copy put: wraps the direct buffers' native memory without heap→native copy.
	public void put(ByteBuffer key, ByteBuffer value) {
		RocksDB.putSegment(ptr(), writeOpts.ptr(),
				MemorySegment.ofBuffer(key), key.remaining(),
				MemorySegment.ofBuffer(value), value.remaining());
	}

	/// Zero-copy put: caller supplies pre-allocated native segments.
	public void put(MemorySegment key, MemorySegment value) {
		RocksDB.putSegment(ptr(), writeOpts.ptr(), key, key.byteSize(), value, value.byteSize());
	}

	/// Zero-copy put using the caller's [Arena].
	public void put(Arena arena, MemorySegment key, MemorySegment value) {
		RocksDB.putSegment(arena, ptr(), writeOpts.ptr(), key, key.byteSize(), value, value.byteSize());
	}

	// -----------------------------------------------------------------------
	// Get
	// -----------------------------------------------------------------------

	/// Get via PinnableSlice — pins data directly from the block cache.
	/// Returns `null` if not found.
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

	// -----------------------------------------------------------------------
	// Delete
	// -----------------------------------------------------------------------

	/// Removes `key` from the database. Slow path: copies the key into native memory.
	public void delete(byte[] key) {
		RocksDB.deleteBytes(ptr(), writeOpts.ptr(), key);
	}

	/// Zero-copy for direct [ByteBuffer]s.
	public void delete(ByteBuffer key) {
		RocksDB.deleteSegment(ptr(), writeOpts.ptr(), MemorySegment.ofBuffer(key), key.remaining());
	}

	/// Zero-copy native-first path.
	public void delete(MemorySegment key) {
		RocksDB.deleteSegment(ptr(), writeOpts.ptr(), key, key.byteSize());
	}

	/// Deletes all keys in the half-open range [`startKey`, `endKey`).
	/// Slow path: copies keys into native memory.
	public void deleteRange(byte[] startKey, byte[] endKey) {
		RocksDB.deleteRangeCfBytes(ptr(), writeOpts.ptr(), startKey, endKey);
	}

	/// Zero-copy for direct [ByteBuffer]s.
	public void deleteRange(ByteBuffer startKey, ByteBuffer endKey) {
		RocksDB.deleteRangeCfBuffer(ptr(), writeOpts.ptr(), startKey, endKey);
	}

	/// Zero-copy native-first path.
	public void deleteRange(MemorySegment startKey, MemorySegment endKey) {
		RocksDB.deleteRangeCfSegment(ptr(), writeOpts.ptr(), startKey, endKey);
	}

	// -----------------------------------------------------------------------
	// KeyMayExist (Bloom filter check)
	// -----------------------------------------------------------------------

	/// Returns `false` if the key definitely does not exist; `true` means it _may_ exist.
	/// Slow path: copies the key into native memory.
	public boolean keyMayExist(byte[] key) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment k = Native.toNative(arena, key);
			return RocksDB.keyMayExistSegment(ptr(), readOpts.ptr(), k, key.length);
		} catch (Throwable t) {
			throw RocksDBException.wrap("keyMayExist failed", t);
		}
	}

	/// [#keyMayExist(byte\[\])] with explicit [ReadOptions].
	public boolean keyMayExist(ReadOptions readOptions, byte[] key) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment k = Native.toNative(arena, key);
			return RocksDB.keyMayExistSegment(ptr(), readOptions.ptr(), k, key.length);
		} catch (Throwable t) {
			throw RocksDBException.wrap("keyMayExist failed", t);
		}
	}

	/// Zero-copy for direct [ByteBuffer]s.
	public boolean keyMayExist(ByteBuffer key) {
		try {
			return RocksDB.keyMayExistSegment(ptr(), readOpts.ptr(), MemorySegment.ofBuffer(key), key.remaining());
		} catch (Throwable t) {
			throw RocksDBException.wrap("keyMayExist failed", t);
		}
	}

	/// Zero-copy for [MemorySegment]s.
	public boolean keyMayExist(MemorySegment key) {
		try {
			return RocksDB.keyMayExistSegment(ptr(), readOpts.ptr(), key, key.byteSize());
		} catch (Throwable t) {
			throw RocksDBException.wrap("keyMayExist failed", t);
		}
	}

	// -----------------------------------------------------------------------
	// Write (batch)
	// -----------------------------------------------------------------------

	public void write(WriteBatch batch) {
		RocksDB.writeBatch(ptr(), writeOpts.ptr(), batch);
	}

	public void write(Arena arena, WriteBatch batch) {
		RocksDB.writeBatch(arena, ptr(), writeOpts.ptr(), batch);
	}

	// -----------------------------------------------------------------------
	// Snapshot
	// -----------------------------------------------------------------------

	/// Creates a snapshot of the current DB state. Must be closed after use.
	public Snapshot getSnapshot() {
		return RocksDB.createSnapshot(ptr());
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

	// -----------------------------------------------------------------------
	// Flush
	// -----------------------------------------------------------------------

	/// Flushes all memtable data to SST files. Blocks when [FlushOptions#isWait()] is `true`.
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
	public Optional<String> getProperty(Property property) {
		return RocksDB.getProperty(ptr(), property);
	}

	/// Returns the value of a numeric DB property, or [OptionalLong#empty()] if not supported.
	public OptionalLong getLongProperty(Property property) {
		return RocksDB.getLongProperty(ptr(), property);
	}

	// -----------------------------------------------------------------------
	// Compaction
	// -----------------------------------------------------------------------

	/// Manually triggers compaction over the entire key space.
	public void compactRange() {
		RocksDB.compactRangeBytes(ptr(), null, null);
	}

	/// Manually triggers compaction over `[startKey, endKey]`.
	/// Pass `null` for either bound to indicate the beginning/end of the key space.
	public void compactRange(byte[] startKey, byte[] endKey) {
		RocksDB.compactRangeBytes(ptr(), startKey, endKey);
	}

	/// [ByteBuffer] overload of [#compactRange(byte\[\], byte\[\])].
	public void compactRange(ByteBuffer startKey, ByteBuffer endKey) {
		RocksDB.compactRangeBuffer(ptr(), startKey, endKey);
	}

	/// [MemorySegment] overload of [#compactRange(byte\[\], byte\[\])].
	public void compactRange(MemorySegment startKey, MemorySegment endKey) {
		RocksDB.compactRangeSegment(ptr(), startKey, endKey);
	}

	/// Compaction with explicit options.
	public void compactRange(CompactOptions opts, byte[] startKey, byte[] endKey) {
		RocksDB.compactRangeOptBytes(ptr(), opts, startKey, endKey);
	}

	/// Hints that `[startKey, endKey]` may benefit from compaction, but does not block.
	public void suggestCompactRange(byte[] startKey, byte[] endKey) {
		RocksDB.suggestCompactRangeBytes(ptr(), startKey, endKey);
	}

	// -----------------------------------------------------------------------
	// File deletions
	// -----------------------------------------------------------------------

	/// Prevents new SST files from being deleted. Must be paired with [#enableFileDeletions()].
	public void disableFileDeletions() {
		RocksDB.disableFileDeletions(ptr());
	}

	/// Re-enables SST file deletions after [#disableFileDeletions()].
	public void enableFileDeletions() {
		RocksDB.enableFileDeletions(ptr());
	}

	// -----------------------------------------------------------------------
	// SST File Ingest
	// -----------------------------------------------------------------------

	/// Ingests SST files produced by [SstFileWriter] into the database.
	public void ingestExternalFile(List<Path> files, IngestExternalFileOptions options) {
		if (files.isEmpty()) {
			return;
		}
		RocksDB.ingestExternalFile(ptr(), files, options);
	}

	public void ingestExternalFile(List<Path> files) {
		try (IngestExternalFileOptions opts = IngestExternalFileOptions.newIngestExternalFileOptions()) {
			ingestExternalFile(files, opts);
		}
	}

	public void ingestExternalFile(Path file, IngestExternalFileOptions options) {
		ingestExternalFile(List.of(file), options);
	}

	public void ingestExternalFile(Path file) {
		ingestExternalFile(List.of(file));
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
