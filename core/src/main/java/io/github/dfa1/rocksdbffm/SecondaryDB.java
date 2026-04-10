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

/// FFM wrapper for a RocksDB secondary instance (`rocksdb_t*` opened via
/// `rocksdb_open_as_secondary`).
///
/// A secondary instance is a _read-only replica_ of a primary database.
/// It tails the primary's WAL and MANIFEST files from a dedicated
/// `secondaryPath` directory. Call [#tryCatchUpWithPrimary()] to
/// apply any new writes that the primary has flushed since the last catch-up.
///
/// Write operations are not available on a secondary instance; attempting a
/// write via the underlying `rocksdb_t*` would return an error from RocksDB.
///
/// ```
/// // Primary (already open elsewhere)
/// try (Options opts = Options.newOptions().setCreateIfMissing(true);
///      SecondaryDB secondary = SecondaryDB.open(opts, primaryPath, secondaryPath)) {
///     // Catch up with whatever the primary has written
///     secondary.tryCatchUpWithPrimary();
///     byte[] value = secondary.get("key".getBytes());
/// }
/// ```
public final class SecondaryDB extends NativeObject {

	// -----------------------------------------------------------------------
	// Method handles unique to SecondaryDB
	// -----------------------------------------------------------------------

	/// `rocksdb_t* rocksdb_open_as_secondary(const rocksdb_options_t* options, const char* name, const char* secondary_path, char** errptr);`
	private static final MethodHandle MH_OPEN;
	/// `void rocksdb_try_catch_up_with_primary(rocksdb_t* db, char** errptr);`
	private static final MethodHandle MH_CATCH_UP;

	static {
		MH_OPEN = RocksDB.lookup("rocksdb_open_as_secondary",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_CATCH_UP = RocksDB.lookup("rocksdb_try_catch_up_with_primary",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS));
	}

	// -----------------------------------------------------------------------
	// Instance state
	// -----------------------------------------------------------------------

	private final ReadOptions readOpts;

	private SecondaryDB(MemorySegment ptr, ReadOptions readOpts) {
		super(ptr);
		this.readOpts = readOpts;
	}

	// -----------------------------------------------------------------------
	// Factory
	// -----------------------------------------------------------------------

	/// Opens a secondary instance of the RocksDB database at `primaryPath`.
	///
	/// @param dbOptions     options (caller retains ownership); `createIfMissing`
	///                      is typically `false` for a secondary
	/// @param primaryPath   path to the primary database directory
	/// @param secondaryPath a dedicated directory for this secondary's own MANIFEST/WAL
	///                      tails; created automatically if it does not exist
	public static SecondaryDB open(Options dbOptions, Path primaryPath, Path secondaryPath) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = Native.errHolder(arena);
			MemorySegment primary = arena.allocateFrom(primaryPath.toString());
			MemorySegment secondary = arena.allocateFrom(secondaryPath.toString());

			MemorySegment ptr = (MemorySegment) MH_OPEN.invokeExact(
					dbOptions.ptr(), primary, secondary, err);
			Native.checkError(err);

			return new SecondaryDB(ptr, ReadOptions.newReadOptions());
		} catch (Throwable t) {
			throw RocksDBException.wrap("SecondaryDB open failed", t);
		}
	}

	// -----------------------------------------------------------------------
	// Catch-up
	// -----------------------------------------------------------------------

	/// Tries to catch up with the primary by reading and applying any new records
	/// from the primary's WAL and newly flushed SST files.
	///
	/// This is a best-effort operation; the secondary may still lag the primary
	/// if the primary has not yet flushed a write.
	public void tryCatchUpWithPrimary() {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = Native.errHolder(arena);
			MH_CATCH_UP.invokeExact(ptr(), err);
			Native.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("tryCatchUpWithPrimary failed", t);
		}
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

	// -----------------------------------------------------------------------
	// Iterator
	// -----------------------------------------------------------------------

	/// Returns a new iterator over the secondary's current view.
	/// Call [#tryCatchUpWithPrimary()] first if you need the latest data.
	public RocksIterator newIterator() {
		return RocksIterator.create(ptr(), readOpts.ptr());
	}

	/// Returns a new iterator using the supplied [ReadOptions].
	public RocksIterator newIterator(ReadOptions readOptions) {
		return RocksIterator.create(ptr(), readOptions.ptr());
	}

	// -----------------------------------------------------------------------
	// Snapshot
	// -----------------------------------------------------------------------

	/// Creates a point-in-time snapshot of the secondary's current view.
	/// Must be closed after use.
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

	// -----------------------------------------------------------------------
	// AutoCloseable
	// -----------------------------------------------------------------------

	@Override
	protected void tryClose(MemorySegment ptr) throws Throwable {
		readOpts.close();
		RocksDB.close(ptr);
	}
}
