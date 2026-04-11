package io.github.dfa1.rocksdbffm;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

/// FFM wrapper for a read-write `rocksdb_t*` instance.
///
/// Obtain via [RocksDB#open] or [RocksDB#openWithTtl].
///
/// ```
/// try (var db = RocksDB.open(path)) {
///     db.put("key".getBytes(), "value".getBytes());
///     byte[] value = db.get("key".getBytes());
/// }
/// ```
public final class ReadWriteDB extends NativeObject {

	private final WriteOptions writeOpts;
	private final ReadOptions readOpts;

	ReadWriteDB(MemorySegment ptr, WriteOptions writeOpts, ReadOptions readOpts) {
		super(ptr);
		this.writeOpts = writeOpts;
		this.readOpts = readOpts;
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
	// Merge
	// -----------------------------------------------------------------------

	/// Applies a merge operand to `key`. Slow path: copies key/value.
	public void merge(byte[] key, byte[] value) {
		RocksDB.mergeBytes(ptr(), writeOpts.ptr(), key, value);
	}

	/// Zero-copy for direct [ByteBuffer]s. Both buffers must be direct (`ByteBuffer.allocateDirect`);
	/// heap buffers will throw [IllegalArgumentException].
	public void merge(ByteBuffer key, ByteBuffer value) {
		RocksDB.mergeBuffer(ptr(), writeOpts.ptr(), key, value);
	}

	/// Zero-copy native-first path.
	public void merge(MemorySegment key, MemorySegment value) {
		RocksDB.mergeSegment(ptr(), writeOpts.ptr(), key, value);
	}

	// -----------------------------------------------------------------------
	// KeyMayExist (Bloom filter check)
	// -----------------------------------------------------------------------

	/// Returns `false` if the key definitely does not exist; `true` means it _may_ exist.
	/// Slow path: copies the key into native memory.
	public boolean keyMayExist(byte[] key) {
		try (Arena arena = Arena.ofConfined()) {
			return RocksDB.keyMayExistSegment(ptr(), readOpts.ptr(), Native.toNative(arena, key), key.length);
		} catch (Throwable t) {
			throw RocksDBException.wrap("keyMayExist failed", t);
		}
	}

	/// [#keyMayExist(byte\[\])] with explicit [ReadOptions].
	public boolean keyMayExist(ReadOptions readOptions, byte[] key) {
		try (Arena arena = Arena.ofConfined()) {
			return RocksDB.keyMayExistSegment(ptr(), readOptions.ptr(), Native.toNative(arena, key), key.length);
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
	// Background jobs
	// -----------------------------------------------------------------------

	/// Cancels all background work (compaction, flush, etc.).
	///
	/// @param wait if `true`, blocks until all running jobs have finished
	public void cancelAllBackgroundWork(boolean wait) {
		RocksDB.cancelAllBackgroundWork(ptr(), wait);
	}

	/// Prevents new manual compactions from starting.
	/// In-progress manual compactions are not affected.
	/// Call [#enableManualCompaction()] to reverse.
	public void disableManualCompaction() {
		RocksDB.disableManualCompaction(ptr());
	}

	/// Re-enables manual compactions after [#disableManualCompaction()].
	public void enableManualCompaction() {
		RocksDB.enableManualCompaction(ptr());
	}

	/// Blocks until all current compactions finish, subject to the given [WaitForCompactOptions].
	///
	/// @throws RocksDBException on I/O error or if [WaitForCompactOptions#isAbortOnPause()] is
	///                          `true` and background work is paused
	public void waitForCompact(WaitForCompactOptions options) {
		RocksDB.waitForCompact(ptr(), options);
	}

	// -----------------------------------------------------------------------
	// WAL iteration
	// -----------------------------------------------------------------------

	/// Returns the sequence number of the most recent committed transaction.
	public SequenceNumber getLatestSequenceNumber() {
		return RocksDB.getLatestSequenceNumber(ptr());
	}

	/// Returns a [WalIterator] positioned at the first [WriteBatch] with a sequence number
	/// greater than or equal to `sequenceNumber`.
	///
	/// The caller must close the iterator after use.
	public WalIterator getUpdatesSince(SequenceNumber sequenceNumber) {
		return RocksDB.getUpdatesSince(ptr(), sequenceNumber);
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

	// TODO: drop, too many variants
	public void ingestExternalFile(Path file, IngestExternalFileOptions options) {
		ingestExternalFile(List.of(file), options);
	}

	// TODO: drop, too many variants
	public void ingestExternalFile(Path file) {
		ingestExternalFile(List.of(file));
	}

	// -----------------------------------------------------------------------
	// Compression probe
	// -----------------------------------------------------------------------

	// TODO: move to rocksdb or just delete it
	/// Returns the set of compression types compiled into the loaded RocksDB library.
	public Set<CompressionType> getSupportedCompressions() {
		Set<CompressionType> result = java.util.EnumSet.of(CompressionType.NO_COMPRESSION);
		java.nio.file.Path tmpDir = null;
		try {
			tmpDir = java.nio.file.Files.createTempDirectory("rocksdbffm-compress-probe-");
			boolean isWindows = System.getProperty("os.name", "").toLowerCase().contains("win");
			for (CompressionType type : CompressionType.values()) {
				if (type == CompressionType.NO_COMPRESSION) {
					continue;
				}
				if (type == CompressionType.XPRESS && !isWindows) {
					continue;
				}
				java.nio.file.Path sstFile = tmpDir.resolve(type.name().toLowerCase() + ".sst");
				try (Options opts = Options.newOptions().setCompression(type);
				     SstFileWriter writer = SstFileWriter.newSstFileWriter(opts)) {
					writer.open(sstFile);
					writer.put(new byte[]{0}, new byte[]{0});
					writer.finish();
					result.add(type);
				} catch (RocksDBException ignored) {
				} finally {
					java.nio.file.Files.deleteIfExists(sstFile);
				}
			}
		} catch (java.io.IOException ignored) {
		} finally {
			if (tmpDir != null) {
				try {
					java.nio.file.Files.deleteIfExists(tmpDir);
				} catch (java.io.IOException ignored) {
				}
			}
		}
		return java.util.Collections.unmodifiableSet(result);
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
