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
	///
	/// @return the configured TTL duration
	public Duration getTtl() {
		return ttl;
	}

	// -----------------------------------------------------------------------
	// Put
	// -----------------------------------------------------------------------

	/// Stores `value` under `key`. Slow path: copies key/value into native memory.
	///
	/// @param key   key bytes
	/// @param value value bytes
	public void put(byte[] key, byte[] value) {
		RocksDB.putBytes(ptr(), writeOpts.ptr(), key, value);
	}

	/// Stores `value` under `key` using the caller's [Arena] for native allocation.
	///
	/// @param arena arena for native allocations
	/// @param key   key bytes
	/// @param value value bytes
	public void put(Arena arena, byte[] key, byte[] value) {
		RocksDB.putBytes(arena, ptr(), writeOpts.ptr(), key, value);
	}

	/// Zero-copy put: wraps the direct buffers' native memory without heap→native copy.
	///
	/// @param key   direct [ByteBuffer] containing the key
	/// @param value direct [ByteBuffer] containing the value
	public void put(ByteBuffer key, ByteBuffer value) {
		RocksDB.putSegment(ptr(), writeOpts.ptr(),
				MemorySegment.ofBuffer(key), key.remaining(),
				MemorySegment.ofBuffer(value), value.remaining());
	}

	/// Zero-copy put: caller supplies pre-allocated native segments.
	///
	/// @param key   native segment containing the key
	/// @param value native segment containing the value
	public void put(MemorySegment key, MemorySegment value) {
		RocksDB.putSegment(ptr(), writeOpts.ptr(), key, key.byteSize(), value, value.byteSize());
	}

	/// Zero-copy put using the caller's [Arena].
	///
	/// @param arena arena for native allocations
	/// @param key   native segment containing the key
	/// @param value native segment containing the value
	public void put(Arena arena, MemorySegment key, MemorySegment value) {
		RocksDB.putSegment(arena, ptr(), writeOpts.ptr(), key, key.byteSize(), value, value.byteSize());
	}

	// -----------------------------------------------------------------------
	// Get
	// -----------------------------------------------------------------------

	/// Get via PinnableSlice — pins data directly from the block cache.
	/// Returns `null` if not found.
	///
	/// @param key key bytes to look up
	/// @return value bytes, or `null` if the key does not exist
	public byte[] get(byte[] key) {
		return RocksDB.getBytes(ptr(), readOpts.ptr(), key);
	}

	/// Get with explicit [ReadOptions], e.g. for snapshot-pinned reads. Returns `null` if not found.
	///
	/// @param readOptions read options, e.g. containing a snapshot
	/// @param key         key bytes to look up
	/// @return value bytes, or `null` if the key does not exist
	public byte[] get(ReadOptions readOptions, byte[] key) {
		return RocksDB.getBytes(ptr(), readOptions.ptr(), key);
	}

	/// Single-copy get via PinnableSlice + direct output [ByteBuffer].
	/// Returns the actual value length, or -1 if not found.
	///
	/// @param key   direct [ByteBuffer] containing the key
	/// @param value direct [ByteBuffer] to write the value into
	/// @return actual value length in bytes, or -1 if the key does not exist
	public int get(ByteBuffer key, ByteBuffer value) {
		return RocksDB.getIntoBuffer(ptr(), readOpts.ptr(),
				MemorySegment.ofBuffer(key), key.remaining(), value);
	}

	/// Zero-copy get via PinnableSlice into a caller-supplied native segment.
	/// Returns the actual value length.
	///
	/// @param key   native segment containing the key
	/// @param value native segment to write the value into
	/// @return actual value length in bytes
	public long get(MemorySegment key, MemorySegment value) {
		return RocksDB.getIntoSegment(ptr(), readOpts.ptr(), key, key.byteSize(), value);
	}

	// -----------------------------------------------------------------------
	// Delete
	// -----------------------------------------------------------------------

	/// Removes `key` from the database. Slow path: copies the key into native memory.
	///
	/// @param key key bytes to remove
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

	/// Deletes all keys in the half-open range [`startKey`, `endKey`).
	/// Slow path: copies keys into native memory.
	///
	/// @param startKey inclusive start of the deleted range
	/// @param endKey   exclusive end of the deleted range
	public void deleteRange(byte[] startKey, byte[] endKey) {
		RocksDB.deleteRangeCfBytes(ptr(), writeOpts.ptr(), startKey, endKey);
	}

	/// Zero-copy for direct [ByteBuffer]s.
	///
	/// @param startKey direct [ByteBuffer] with the inclusive start key
	/// @param endKey   direct [ByteBuffer] with the exclusive end key
	public void deleteRange(ByteBuffer startKey, ByteBuffer endKey) {
		RocksDB.deleteRangeCfBuffer(ptr(), writeOpts.ptr(), startKey, endKey);
	}

	/// Zero-copy native-first path.
	///
	/// @param startKey native segment with the inclusive start key
	/// @param endKey   native segment with the exclusive end key
	public void deleteRange(MemorySegment startKey, MemorySegment endKey) {
		RocksDB.deleteRangeCfSegment(ptr(), writeOpts.ptr(), startKey, endKey);
	}

	// -----------------------------------------------------------------------
	// KeyMayExist (Bloom filter check)
	// -----------------------------------------------------------------------

	/// Returns `false` if the key definitely does not exist; `true` means it _may_ exist.
	/// Slow path: copies the key into native memory.
	///
	/// @param key key bytes to probe
	/// @return `false` if the key is definitely absent, `true` if it may exist
	public boolean keyMayExist(byte[] key) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment k = RocksDB.toNative(arena, key);
			return RocksDB.keyMayExistSegment(ptr(), readOpts.ptr(), k, key.length);
		} catch (Throwable t) {
			throw RocksDBException.wrap("keyMayExist failed", t);
		}
	}

	/// [#keyMayExist(byte\[\])] with explicit [ReadOptions].
	///
	/// @param readOptions read options to use for the probe
	/// @param key         key bytes to probe
	/// @return `false` if the key is definitely absent, `true` if it may exist
	public boolean keyMayExist(ReadOptions readOptions, byte[] key) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment k = RocksDB.toNative(arena, key);
			return RocksDB.keyMayExistSegment(ptr(), readOptions.ptr(), k, key.length);
		} catch (Throwable t) {
			throw RocksDBException.wrap("keyMayExist failed", t);
		}
	}

	/// Zero-copy for direct [ByteBuffer]s.
	///
	/// @param key direct [ByteBuffer] containing the key to probe
	/// @return `false` if the key is definitely absent, `true` if it may exist
	public boolean keyMayExist(ByteBuffer key) {
		try {
			return RocksDB.keyMayExistSegment(ptr(), readOpts.ptr(), MemorySegment.ofBuffer(key), key.remaining());
		} catch (Throwable t) {
			throw RocksDBException.wrap("keyMayExist failed", t);
		}
	}

	/// Zero-copy for [MemorySegment]s.
	///
	/// @param key native segment containing the key to probe
	/// @return `false` if the key is definitely absent, `true` if it may exist
	public boolean keyMayExist(MemorySegment key) {
		try {
			return RocksDB.keyMayExistSegment(ptr(), readOpts.ptr(), key, key.byteSize());
		} catch (Throwable t) {
			throw RocksDBException.wrap("keyMayExist failed", t);
		}
	}

	// -----------------------------------------------------------------------
	// Column family management
	// -----------------------------------------------------------------------

	/// Creates a new column family described by `descriptor` and returns its handle.
	///
	/// @param descriptor column family name and options
	/// @return handle to the newly created column family; caller must close it
	public ColumnFamilyHandle createColumnFamily(ColumnFamilyDescriptor descriptor) {
		return RocksDB.createCf(ptr(), descriptor);
	}

	/// Drops the column family identified by `handle`.
	///
	/// @param handle handle to the column family to drop
	public void dropColumnFamily(ColumnFamilyHandle handle) {
		RocksDB.dropCf(ptr(), handle);
	}

	// -----------------------------------------------------------------------
	// Put — column family overloads
	// -----------------------------------------------------------------------

	/// Stores `value` under `key` in `cf`. Slow path: copies key/value into native memory.
	///
	/// @param cf    target column family
	/// @param key   key bytes
	/// @param value value bytes
	public void put(ColumnFamilyHandle cf, byte[] key, byte[] value) {
		RocksDB.putCfBytes(ptr(), writeOpts.ptr(), cf, key, value);
	}

	/// Zero-copy put into `cf` for direct [ByteBuffer]s.
	///
	/// @param cf    target column family
	/// @param key   direct [ByteBuffer] containing the key
	/// @param value direct [ByteBuffer] containing the value
	public void put(ColumnFamilyHandle cf, ByteBuffer key, ByteBuffer value) {
		RocksDB.putCfSegment(ptr(), writeOpts.ptr(), cf,
				MemorySegment.ofBuffer(key), key.remaining(),
				MemorySegment.ofBuffer(value), value.remaining());
	}

	/// Zero-copy put into `cf` for [MemorySegment]s.
	///
	/// @param cf    target column family
	/// @param key   native segment containing the key
	/// @param value native segment containing the value
	public void put(ColumnFamilyHandle cf, MemorySegment key, MemorySegment value) {
		RocksDB.putCfSegment(ptr(), writeOpts.ptr(), cf, key, key.byteSize(), value, value.byteSize());
	}

	// -----------------------------------------------------------------------
	// Get — column family overloads
	// -----------------------------------------------------------------------

	/// Returns the value for `key` in `cf`, or `null` if not found.
	///
	/// @param cf  column family to read from
	/// @param key key bytes to look up
	/// @return value bytes, or `null` if the key does not exist
	public byte[] get(ColumnFamilyHandle cf, byte[] key) {
		return RocksDB.getCfBytes(ptr(), readOpts.ptr(), cf, key);
	}

	/// Get from `cf` with explicit [ReadOptions]. Returns `null` if not found.
	///
	/// @param cf          column family to read from
	/// @param readOptions read options, e.g. containing a snapshot
	/// @param key         key bytes to look up
	/// @return value bytes, or `null` if the key does not exist
	public byte[] get(ColumnFamilyHandle cf, ReadOptions readOptions, byte[] key) {
		return RocksDB.getCfBytes(ptr(), readOptions.ptr(), cf, key);
	}

	/// Single-copy get from `cf` via PinnableSlice + direct output [ByteBuffer].
	/// Returns the actual value length, or -1 if not found.
	///
	/// @param cf    column family to read from
	/// @param key   direct [ByteBuffer] containing the key
	/// @param value direct [ByteBuffer] to write the value into
	/// @return actual value length in bytes, or -1 if the key does not exist
	public int get(ColumnFamilyHandle cf, ByteBuffer key, ByteBuffer value) {
		return RocksDB.getCfIntoBuffer(ptr(), readOpts.ptr(), cf,
				MemorySegment.ofBuffer(key), key.remaining(), value);
	}

	/// Zero-copy get from `cf` into a caller-supplied native segment.
	/// Returns the actual value length.
	///
	/// @param cf    column family to read from
	/// @param key   native segment containing the key
	/// @param value native segment to write the value into
	/// @return actual value length in bytes
	public long get(ColumnFamilyHandle cf, MemorySegment key, MemorySegment value) {
		return RocksDB.getCfIntoSegment(ptr(), readOpts.ptr(), cf, key, key.byteSize(), value);
	}

	// -----------------------------------------------------------------------
	// Delete — column family overloads
	// -----------------------------------------------------------------------

	/// Removes `key` from `cf`. Slow path: copies the key into native memory.
	///
	/// @param cf  column family to delete from
	/// @param key key bytes to remove
	public void delete(ColumnFamilyHandle cf, byte[] key) {
		RocksDB.deleteCfBytes(ptr(), writeOpts.ptr(), cf, key);
	}

	/// Zero-copy delete from `cf` for direct [ByteBuffer]s.
	///
	/// @param cf  column family to delete from
	/// @param key direct [ByteBuffer] containing the key to remove
	public void delete(ColumnFamilyHandle cf, ByteBuffer key) {
		RocksDB.deleteCfSegment(ptr(), writeOpts.ptr(), cf, MemorySegment.ofBuffer(key), key.remaining());
	}

	/// Zero-copy delete from `cf` for [MemorySegment]s.
	///
	/// @param cf  column family to delete from
	/// @param key native segment containing the key to remove
	public void delete(ColumnFamilyHandle cf, MemorySegment key) {
		RocksDB.deleteCfSegment(ptr(), writeOpts.ptr(), cf, key, key.byteSize());
	}

	// -----------------------------------------------------------------------
	// DeleteRange — column family overloads
	// -----------------------------------------------------------------------

	/// Deletes all keys in `[startKey, endKey)` within `cf`. Slow path.
	///
	/// @param cf       column family to delete from
	/// @param startKey inclusive start of the deleted range
	/// @param endKey   exclusive end of the deleted range
	public void deleteRange(ColumnFamilyHandle cf, byte[] startKey, byte[] endKey) {
		RocksDB.deleteRangeCfBytesExplicit(ptr(), writeOpts.ptr(), cf, startKey, endKey);
	}

	/// Zero-copy deleteRange for direct [ByteBuffer]s.
	///
	/// @param cf       column family to delete from
	/// @param startKey direct [ByteBuffer] with the inclusive start key
	/// @param endKey   direct [ByteBuffer] with the exclusive end key
	public void deleteRange(ColumnFamilyHandle cf, ByteBuffer startKey, ByteBuffer endKey) {
		RocksDB.deleteRangeCfBufferExplicit(ptr(), writeOpts.ptr(), cf, startKey, endKey);
	}

	/// Zero-copy deleteRange for [MemorySegment]s.
	///
	/// @param cf       column family to delete from
	/// @param startKey native segment with the inclusive start key
	/// @param endKey   native segment with the exclusive end key
	public void deleteRange(ColumnFamilyHandle cf, MemorySegment startKey, MemorySegment endKey) {
		RocksDB.deleteRangeCfSegmentExplicit(ptr(), writeOpts.ptr(), cf, startKey, endKey);
	}

	// -----------------------------------------------------------------------
	// KeyMayExist — column family overloads
	// -----------------------------------------------------------------------

	/// Returns `false` if `key` definitely does not exist in `cf`; `true` means it _may_ exist.
	///
	/// @param cf  column family to probe
	/// @param key key bytes to probe
	/// @return `false` if the key is definitely absent, `true` if it may exist
	public boolean keyMayExist(ColumnFamilyHandle cf, byte[] key) {
		try (var arena = Arena.ofConfined()) {
			return RocksDB.keyMayExistCfSegment(ptr(), readOpts.ptr(), cf,
					RocksDB.toNative(arena, key), key.length);
		} catch (Throwable t) {
			throw RocksDBException.wrap("keyMayExist failed", t);
		}
	}

	/// [#keyMayExist(ColumnFamilyHandle, byte\[\])] with explicit [ReadOptions].
	///
	/// @param cf          column family to probe
	/// @param readOptions read options to use for the probe
	/// @param key         key bytes to probe
	/// @return `false` if the key is definitely absent, `true` if it may exist
	public boolean keyMayExist(ColumnFamilyHandle cf, ReadOptions readOptions, byte[] key) {
		try (var arena = Arena.ofConfined()) {
			return RocksDB.keyMayExistCfSegment(ptr(), readOptions.ptr(), cf,
					RocksDB.toNative(arena, key), key.length);
		} catch (Throwable t) {
			throw RocksDBException.wrap("keyMayExist failed", t);
		}
	}

	/// Zero-copy for direct [ByteBuffer]s.
	///
	/// @param cf  column family to probe
	/// @param key direct [ByteBuffer] containing the key to probe
	/// @return `false` if the key is definitely absent, `true` if it may exist
	public boolean keyMayExist(ColumnFamilyHandle cf, ByteBuffer key) {
		try {
			return RocksDB.keyMayExistCfSegment(ptr(), readOpts.ptr(), cf,
					MemorySegment.ofBuffer(key), key.remaining());
		} catch (Throwable t) {
			throw RocksDBException.wrap("keyMayExist failed", t);
		}
	}

	/// Zero-copy for [MemorySegment]s.
	///
	/// @param cf  column family to probe
	/// @param key native segment containing the key to probe
	/// @return `false` if the key is definitely absent, `true` if it may exist
	public boolean keyMayExist(ColumnFamilyHandle cf, MemorySegment key) {
		try {
			return RocksDB.keyMayExistCfSegment(ptr(), readOpts.ptr(), cf, key, key.byteSize());
		} catch (Throwable t) {
			throw RocksDBException.wrap("keyMayExist failed", t);
		}
	}

	// -----------------------------------------------------------------------
	// Iterator — column family overloads
	// -----------------------------------------------------------------------

	/// Returns a new iterator scoped to `cf` using the default read options.
	///
	/// @param cf column family to iterate over
	/// @return a new [RocksIterator]; caller must close it
	public RocksIterator newIterator(ColumnFamilyHandle cf) {
		return RocksDB.createIteratorCf(ptr(), readOpts.ptr(), cf);
	}

	/// Returns a new iterator scoped to `cf` with explicit [ReadOptions].
	///
	/// @param cf          column family to iterate over
	/// @param readOptions read options, e.g. containing a snapshot
	/// @return a new [RocksIterator]; caller must close it
	public RocksIterator newIterator(ColumnFamilyHandle cf, ReadOptions readOptions) {
		return RocksDB.createIteratorCf(ptr(), readOptions.ptr(), cf);
	}

	// -----------------------------------------------------------------------
	// Flush — column family overloads
	// -----------------------------------------------------------------------

	/// Flushes the memtable for `cf` to SST files.
	///
	/// @param cf           column family to flush
	/// @param flushOptions flush options controlling wait behaviour
	public void flush(ColumnFamilyHandle cf, FlushOptions flushOptions) {
		RocksDB.flushCf(ptr(), flushOptions, cf);
	}

	// -----------------------------------------------------------------------
	// DB Properties — column family overloads
	// -----------------------------------------------------------------------

	/// Returns the value of a property scoped to `cf`, or [Optional#empty()] if not supported.
	///
	/// @param cf       column family to query
	/// @param property the property to query
	/// @return the property value, or [Optional#empty()] if not supported
	public Optional<String> getProperty(ColumnFamilyHandle cf, Property property) {
		return RocksDB.getPropertyCf(ptr(), cf, property);
	}

	/// Returns the value of a numeric property scoped to `cf`, or [OptionalLong#empty()] if not supported.
	///
	/// @param cf       column family to query
	/// @param property the property to query
	/// @return the numeric property value, or [OptionalLong#empty()] if not supported
	public OptionalLong getLongProperty(ColumnFamilyHandle cf, Property property) {
		return RocksDB.getLongPropertyCf(ptr(), cf, property);
	}

	// -----------------------------------------------------------------------
	// Write (batch)
	// -----------------------------------------------------------------------

	/// Applies all mutations in `batch` atomically.
	///
	/// @param batch the write batch to apply
	public void write(WriteBatch batch) {
		RocksDB.writeBatch(ptr(), writeOpts.ptr(), batch);
	}

	/// Applies all mutations in `batch` atomically using the caller's [Arena].
	///
	/// @param arena arena for native allocations
	/// @param batch the write batch to apply
	public void write(Arena arena, WriteBatch batch) {
		RocksDB.writeBatch(arena, ptr(), writeOpts.ptr(), batch);
	}

	// -----------------------------------------------------------------------
	// Snapshot
	// -----------------------------------------------------------------------

	/// Creates a snapshot of the current DB state. Must be closed after use.
	///
	/// @return a new [Snapshot]; caller must close it
	public Snapshot getSnapshot() {
		return RocksDB.createSnapshot(ptr());
	}

	// -----------------------------------------------------------------------
	// Iterator
	// -----------------------------------------------------------------------

	/// Returns a new iterator using the database's default read options.
	///
	/// @return a new [RocksIterator]; caller must close it
	public RocksIterator newIterator() {
		return RocksIterator.create(ptr(), readOpts.ptr());
	}

	/// Returns a new iterator using the supplied [ReadOptions].
	///
	/// @param readOptions read options, e.g. containing a snapshot
	/// @return a new [RocksIterator]; caller must close it
	public RocksIterator newIterator(ReadOptions readOptions) {
		return RocksIterator.create(ptr(), readOptions.ptr());
	}

	// -----------------------------------------------------------------------
	// Flush
	// -----------------------------------------------------------------------

	/// Flushes all memtable data to SST files. Blocks when [FlushOptions#isWait()] is `true`.
	///
	/// @param flushOptions flush options controlling wait behaviour
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
	///
	/// @param property the property to query
	/// @return the property value, or [Optional#empty()] if not supported
	public Optional<String> getProperty(Property property) {
		return RocksDB.getProperty(ptr(), property);
	}

	/// Returns the value of a numeric DB property, or [OptionalLong#empty()] if not supported.
	///
	/// @param property the property to query
	/// @return the numeric property value, or [OptionalLong#empty()] if not supported
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
	///
	/// @param startKey inclusive start key, or `null` for the beginning of the key space
	/// @param endKey   inclusive end key, or `null` for the end of the key space
	public void compactRange(byte[] startKey, byte[] endKey) {
		RocksDB.compactRangeBytes(ptr(), startKey, endKey);
	}

	/// [ByteBuffer] overload of [#compactRange(byte\[\], byte\[\])].
	///
	/// @param startKey [ByteBuffer] with the inclusive start key
	/// @param endKey   [ByteBuffer] with the inclusive end key
	public void compactRange(ByteBuffer startKey, ByteBuffer endKey) {
		RocksDB.compactRangeBuffer(ptr(), startKey, endKey);
	}

	/// [MemorySegment] overload of [#compactRange(byte\[\], byte\[\])].
	///
	/// @param startKey native segment with the inclusive start key
	/// @param endKey   native segment with the inclusive end key
	public void compactRange(MemorySegment startKey, MemorySegment endKey) {
		RocksDB.compactRangeSegment(ptr(), startKey, endKey);
	}

	/// Compaction with explicit options.
	///
	/// @param opts     compaction options
	/// @param startKey inclusive start key, or `null` for the beginning of the key space
	/// @param endKey   inclusive end key, or `null` for the end of the key space
	public void compactRange(CompactOptions opts, byte[] startKey, byte[] endKey) {
		RocksDB.compactRangeOptBytes(ptr(), opts, startKey, endKey);
	}

	/// Hints that `[startKey, endKey]` may benefit from compaction, but does not block.
	///
	/// @param startKey inclusive start key
	/// @param endKey   inclusive end key
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
	///
	/// @param files   list of SST file paths to ingest
	/// @param options ingest options controlling behaviour on conflicts
	public void ingestExternalFile(List<Path> files, IngestExternalFileOptions options) {
		if (files.isEmpty()) {
			return;
		}
		RocksDB.ingestExternalFile(ptr(), files, options);
	}

	/// Ingests SST files using default [IngestExternalFileOptions].
	///
	/// @param files list of SST file paths to ingest
	public void ingestExternalFile(List<Path> files) {
		try (IngestExternalFileOptions opts = IngestExternalFileOptions.newIngestExternalFileOptions()) {
			ingestExternalFile(files, opts);
		}
	}

	/// Ingests a single SST file with explicit [IngestExternalFileOptions].
	///
	/// @param file    SST file path to ingest
	/// @param options ingest options controlling behaviour on conflicts
	public void ingestExternalFile(Path file, IngestExternalFileOptions options) {
		ingestExternalFile(List.of(file), options);
	}

	/// Ingests a single SST file using default [IngestExternalFileOptions].
	///
	/// @param file SST file path to ingest
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
