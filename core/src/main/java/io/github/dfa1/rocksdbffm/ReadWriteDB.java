package io.github.dfa1.rocksdbffm;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
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

	// -----------------------------------------------------------------------
	// Method handles unique to ReadWriteDB (shared rocksdb_t* ops live in RocksDB)
	// -----------------------------------------------------------------------

	/// `void rocksdb_merge(rocksdb_t* db, const rocksdb_writeoptions_t* options, const char* key, size_t keylen, const char* val, size_t vallen, char** errptr);`
	private static final MethodHandle MH_MERGE;
	/// `void rocksdb_delete_range_cf(rocksdb_t* db, const rocksdb_writeoptions_t* options, rocksdb_column_family_handle_t* column_family, const char* start_key, size_t start_key_len, const char* end_key, size_t end_key_len, char** errptr);`
	private static final MethodHandle MH_DELETE_RANGE_CF;
	/// `rocksdb_column_family_handle_t* rocksdb_get_default_column_family_handle(rocksdb_t* db);`
	private static final MethodHandle MH_GET_DEFAULT_CF;
	/// `void rocksdb_write(rocksdb_t* db, const rocksdb_writeoptions_t* options, rocksdb_writebatch_t* batch, char** errptr);`
	private static final MethodHandle MH_WRITE;
	/// `unsigned char rocksdb_key_may_exist(rocksdb_t* db, const rocksdb_readoptions_t* options, const char* key, size_t key_len, char** value, size_t* val_len, const char* timestamp, size_t timestamp_len, unsigned char* value_found);`
	private static final MethodHandle MH_KEY_MAY_EXIST;
	/// `void rocksdb_compact_range(rocksdb_t* db, const char* start_key, size_t start_key_len, const char* limit_key, size_t limit_key_len);`
	private static final MethodHandle MH_COMPACT_RANGE;
	/// `void rocksdb_compact_range_opt(rocksdb_t* db, rocksdb_compactoptions_t* opt, const char* start_key, size_t start_key_len, const char* limit_key, size_t limit_key_len);`
	private static final MethodHandle MH_COMPACT_RANGE_OPT;
	/// `void rocksdb_suggest_compact_range(rocksdb_t* db, const char* start_key, size_t start_key_len, const char* limit_key, size_t limit_key_len, char** errptr);`
	private static final MethodHandle MH_SUGGEST_COMPACT_RANGE;
	/// `void rocksdb_disable_file_deletions(rocksdb_t* db, char** errptr);`
	private static final MethodHandle MH_DISABLE_FILE_DELETIONS;
	/// `void rocksdb_enable_file_deletions(rocksdb_t* db, char** errptr);`
	private static final MethodHandle MH_ENABLE_FILE_DELETIONS;
	/// `void rocksdb_ingest_external_file(rocksdb_t* db, const char* const* file_list, const size_t list_len, const rocksdb_ingestexternalfileoptions_t* opt, char** errptr);`
	private static final MethodHandle MH_INGEST_EXTERNAL_FILE;

	static {
		MH_MERGE = RocksDB.lookup("rocksdb_merge",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));

		MH_DELETE_RANGE_CF = RocksDB.lookup("rocksdb_delete_range_cf",
				FunctionDescriptor.ofVoid(
						ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS));

		MH_GET_DEFAULT_CF = RocksDB.lookup("rocksdb_get_default_column_family_handle",
				FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_WRITE = RocksDB.lookup("rocksdb_write",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_KEY_MAY_EXIST = RocksDB.lookup("rocksdb_key_may_exist",
				FunctionDescriptor.of(ValueLayout.JAVA_BYTE,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS));

		MH_COMPACT_RANGE = RocksDB.lookup("rocksdb_compact_range",
				FunctionDescriptor.ofVoid(
						ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));

		MH_COMPACT_RANGE_OPT = RocksDB.lookup("rocksdb_compact_range_opt",
				FunctionDescriptor.ofVoid(
						ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));

		MH_SUGGEST_COMPACT_RANGE = RocksDB.lookup("rocksdb_suggest_compact_range",
				FunctionDescriptor.ofVoid(
						ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS));

		MH_DISABLE_FILE_DELETIONS = RocksDB.lookup("rocksdb_disable_file_deletions",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_ENABLE_FILE_DELETIONS = RocksDB.lookup("rocksdb_enable_file_deletions",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_INGEST_EXTERNAL_FILE = RocksDB.lookup("rocksdb_ingest_external_file",
				FunctionDescriptor.ofVoid(
						ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS));
	}

	// -----------------------------------------------------------------------
	// Instance state
	// -----------------------------------------------------------------------

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

	/// Like [#put(MemorySegment, MemorySegment)] but uses a pooled error segment instead of
	/// allocating a new arena, trading allocation overhead for pool contention on high-concurrency paths.
	public void put2(MemorySegment key, MemorySegment value) {
		RocksDB.putPool(ptr(), writeOpts.ptr(), key, key.byteSize(), value, value.byteSize());
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
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = Native.errHolder(arena);
			MemorySegment cf = (MemorySegment) MH_GET_DEFAULT_CF.invokeExact(ptr());
			MH_DELETE_RANGE_CF.invokeExact(ptr(), writeOpts.ptr(), cf,
					Native.toNative(arena, startKey), (long) startKey.length,
					Native.toNative(arena, endKey), (long) endKey.length,
					err);
			Native.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("deleteRange failed", t);
		}
	}

	/// Zero-copy for direct [ByteBuffer]s.
	public void deleteRange(ByteBuffer startKey, ByteBuffer endKey) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = Native.errHolder(arena);
			MemorySegment cf = (MemorySegment) MH_GET_DEFAULT_CF.invokeExact(ptr());
			MH_DELETE_RANGE_CF.invokeExact(ptr(), writeOpts.ptr(), cf,
					MemorySegment.ofBuffer(startKey), (long) startKey.remaining(),
					MemorySegment.ofBuffer(endKey), (long) endKey.remaining(),
					err);
			Native.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("deleteRange failed", t);
		}
	}

	/// Zero-copy native-first path.
	public void deleteRange(MemorySegment startKey, MemorySegment endKey) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = Native.errHolder(arena);
			MemorySegment cf = (MemorySegment) MH_GET_DEFAULT_CF.invokeExact(ptr());
			MH_DELETE_RANGE_CF.invokeExact(ptr(), writeOpts.ptr(), cf,
					startKey, startKey.byteSize(),
					endKey, endKey.byteSize(),
					err);
			Native.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("deleteRange failed", t);
		}
	}

	// -----------------------------------------------------------------------
	// Merge
	// -----------------------------------------------------------------------

	/// Applies a merge operand to `key`. Slow path: copies key/value.
	public void merge(byte[] key, byte[] value) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = Native.errHolder(arena);
			MH_MERGE.invokeExact(ptr(), writeOpts.ptr(),
					Native.toNative(arena, key), (long) key.length,
					Native.toNative(arena, value), (long) value.length,
					err);
			Native.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("merge failed", t);
		}
	}

	/// Zero-copy for direct [ByteBuffer]s.
	public void merge(ByteBuffer key, ByteBuffer value) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = Native.errHolder(arena);
			MH_MERGE.invokeExact(ptr(), writeOpts.ptr(),
					MemorySegment.ofBuffer(key), (long) key.remaining(),
					MemorySegment.ofBuffer(value), (long) value.remaining(),
					err);
			Native.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("merge failed", t);
		}
	}

	/// Zero-copy native-first path.
	public void merge(MemorySegment key, MemorySegment value) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = Native.errHolder(arena);
			MH_MERGE.invokeExact(ptr(), writeOpts.ptr(),
					key, key.byteSize(),
					value, value.byteSize(),
					err);
			Native.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("merge failed", t);
		}
	}

	// -----------------------------------------------------------------------
	// KeyMayExist (Bloom filter check)
	// -----------------------------------------------------------------------

	/// Returns `false` if the key definitely does not exist; `true` means it _may_ exist.
	/// Slow path: copies the key into native memory.
	public boolean keyMayExist(byte[] key) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment k = Native.toNative(arena, key);
			return keyMayExistNative(readOpts.ptr(), k, key.length);
		} catch (Throwable t) {
			throw RocksDBException.wrap("keyMayExist failed", t);
		}
	}

	/// [#keyMayExist(byte\[\])] with explicit [ReadOptions].
	public boolean keyMayExist(ReadOptions readOptions, byte[] key) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment k = Native.toNative(arena, key);
			return keyMayExistNative(readOptions.ptr(), k, key.length);
		} catch (Throwable t) {
			throw RocksDBException.wrap("keyMayExist failed", t);
		}
	}

	/// Zero-copy for direct [ByteBuffer]s.
	public boolean keyMayExist(ByteBuffer key) {
		try {
			return keyMayExistNative(readOpts.ptr(), MemorySegment.ofBuffer(key), key.remaining());
		} catch (Throwable t) {
			throw RocksDBException.wrap("keyMayExist failed", t);
		}
	}

	/// Zero-copy for [MemorySegment]s.
	public boolean keyMayExist(MemorySegment key) {
		try {
			return keyMayExistNative(readOpts.ptr(), key, (int) key.byteSize());
		} catch (Throwable t) {
			throw RocksDBException.wrap("keyMayExist failed", t);
		}
	}

	private boolean keyMayExistNative(MemorySegment roPtr, MemorySegment key, long keyLen)
			throws Throwable {
		return ((byte) MH_KEY_MAY_EXIST.invokeExact(
				ptr(), roPtr,
				key, keyLen,
				MemorySegment.NULL, MemorySegment.NULL,
				MemorySegment.NULL, 0L,
				MemorySegment.NULL)) != 0;
	}

	// -----------------------------------------------------------------------
	// Write (batch)
	// -----------------------------------------------------------------------

	public void write(WriteBatch batch) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = Native.errHolder(arena);
			MH_WRITE.invokeExact(ptr(), writeOpts.ptr(), batch.ptr(), err);
			Native.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("write failed", t);
		}
	}

	public void write(Arena arena, WriteBatch batch) {
		try {
			MemorySegment err = Native.errHolder(arena);
			MH_WRITE.invokeExact(ptr(), writeOpts.ptr(), batch.ptr(), err);
			Native.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("write failed", t);
		}
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
		compactRange((MemorySegment) null, (MemorySegment) null);
	}

	/// Manually triggers compaction over `[startKey, endKey]`.
	/// Pass `null` for either bound to indicate the beginning/end of the key space.
	public void compactRange(byte[] startKey, byte[] endKey) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment s = startKey == null ? MemorySegment.NULL : Native.toNative(arena, startKey);
			MemorySegment e = endKey == null ? MemorySegment.NULL : Native.toNative(arena, endKey);
			MH_COMPACT_RANGE.invokeExact(ptr(),
					s, startKey == null ? 0L : (long) startKey.length,
					e, endKey == null ? 0L : (long) endKey.length);
		} catch (Throwable t) {
			throw RocksDBException.wrap("compactRange failed", t);
		}
	}

	/// [ByteBuffer] overload of [#compactRange(byte\[\], byte\[\])].
	public void compactRange(ByteBuffer startKey, ByteBuffer endKey) {
		try {
			MemorySegment s = startKey == null ? MemorySegment.NULL : MemorySegment.ofBuffer(startKey);
			MemorySegment e = endKey == null ? MemorySegment.NULL : MemorySegment.ofBuffer(endKey);
			MH_COMPACT_RANGE.invokeExact(ptr(),
					s, startKey == null ? 0L : (long) startKey.remaining(),
					e, endKey == null ? 0L : (long) endKey.remaining());
		} catch (Throwable t) {
			throw RocksDBException.wrap("compactRange failed", t);
		}
	}

	/// [MemorySegment] overload of [#compactRange(byte\[\], byte\[\])].
	public void compactRange(MemorySegment startKey, MemorySegment endKey) {
		try {
			MemorySegment s = startKey == null ? MemorySegment.NULL : startKey;
			MemorySegment e = endKey == null ? MemorySegment.NULL : endKey;
			MH_COMPACT_RANGE.invokeExact(ptr(),
					s, s == MemorySegment.NULL ? 0L : s.byteSize(),
					e, e == MemorySegment.NULL ? 0L : e.byteSize());
		} catch (Throwable t) {
			throw RocksDBException.wrap("compactRange failed", t);
		}
	}

	/// Compaction with explicit options.
	public void compactRange(CompactOptions opts, byte[] startKey, byte[] endKey) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment s = startKey == null ? MemorySegment.NULL : Native.toNative(arena, startKey);
			MemorySegment e = endKey == null ? MemorySegment.NULL : Native.toNative(arena, endKey);
			MH_COMPACT_RANGE_OPT.invokeExact(ptr(), opts.ptr(),
					s, startKey == null ? 0L : (long) startKey.length,
					e, endKey == null ? 0L : (long) endKey.length);
		} catch (Throwable t) {
			throw RocksDBException.wrap("compactRange failed", t);
		}
	}

	/// Hints that `[startKey, endKey]` may benefit from compaction, but does not block.
	public void suggestCompactRange(byte[] startKey, byte[] endKey) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = Native.errHolder(arena);
			MemorySegment s = startKey == null ? MemorySegment.NULL : Native.toNative(arena, startKey);
			MemorySegment e = endKey == null ? MemorySegment.NULL : Native.toNative(arena, endKey);
			MH_SUGGEST_COMPACT_RANGE.invokeExact(ptr(),
					s, startKey == null ? 0L : (long) startKey.length,
					e, endKey == null ? 0L : (long) endKey.length,
					err);
			Native.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("suggestCompactRange failed", t);
		}
	}

	// -----------------------------------------------------------------------
	// File deletions
	// -----------------------------------------------------------------------

	/// Prevents new SST files from being deleted. Must be paired with [#enableFileDeletions()].
	public void disableFileDeletions() {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = Native.errHolder(arena);
			MH_DISABLE_FILE_DELETIONS.invokeExact(ptr(), err);
			Native.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("disableFileDeletions failed", t);
		}
	}

	/// Re-enables SST file deletions after [#disableFileDeletions()].
	public void enableFileDeletions() {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = Native.errHolder(arena);
			MH_ENABLE_FILE_DELETIONS.invokeExact(ptr(), err);
			Native.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("enableFileDeletions failed", t);
		}
	}

	// -----------------------------------------------------------------------
	// SST File Ingest
	// -----------------------------------------------------------------------

	/// Ingests SST files produced by [SstFileWriter] into the database.
	public void ingestExternalFile(List<Path> files, IngestExternalFileOptions options) {
		if (files.isEmpty()) {
			return;
		}
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = Native.errHolder(arena);
			MemorySegment fileArray = arena.allocate(ValueLayout.ADDRESS, files.size());
			for (int i = 0; i < files.size(); i++) {
				fileArray.setAtIndex(ValueLayout.ADDRESS, i, arena.allocateFrom(files.get(i).toString()));
			}
			MH_INGEST_EXTERNAL_FILE.invokeExact(ptr(), fileArray, (long) files.size(), options.ptr(), err);
			Native.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("ingestExternalFile failed", t);
		}
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
	// Compression probe
	// -----------------------------------------------------------------------

	/// Returns the set of compression types compiled into the loaded RocksDB library.
	public Set<CompressionType> getSupportedCompressions() {
		Set<CompressionType> result = java.util.EnumSet.of(CompressionType.NO_COMPRESSION);
		java.nio.file.Path tmpDir = null;
		try {
			tmpDir = java.nio.file.Files.createTempDirectory("rocksdbffm-compress-probe-");
			boolean isWindows = System.getProperty("os.name", "").toLowerCase().contains("win");
			for (CompressionType type : CompressionType.values()) {
				if (type == CompressionType.NO_COMPRESSION) continue;
				if (type == CompressionType.XPRESS && !isWindows) continue;
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
