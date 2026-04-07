package io.github.dfa1.rocksdbffm;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.file.Path;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * FFM wrapper for a RocksDB secondary instance ({@code rocksdb_t*} opened via
 * {@code rocksdb_open_as_secondary}).
 *
 * <p>A secondary instance is a <em>read-only replica</em> of a primary database.
 * It tails the primary's WAL and MANIFEST files from a dedicated
 * {@code secondaryPath} directory. Call {@link #tryCatchUpWithPrimary()} to
 * apply any new writes that the primary has flushed since the last catch-up.
 *
 * <p>Write operations are not available on a secondary instance; attempting a
 * write via the underlying {@code rocksdb_t*} would return an error from RocksDB.
 *
 * <pre>{@code
 * // Primary (already open elsewhere)
 * try (Options opts = Options.newOptions().setCreateIfMissing(true);
 *      SecondaryDB secondary = SecondaryDB.open(opts, primaryPath, secondaryPath)) {
 *
 *     // Catch up with whatever the primary has written
 *     secondary.tryCatchUpWithPrimary();
 *
 *     byte[] value = secondary.get("key".getBytes());
 * }
 * }</pre>
 */
public final class SecondaryDB extends NativeObject {

	// -----------------------------------------------------------------------
	// Method handles
	// -----------------------------------------------------------------------

	private static final MethodHandle MH_OPEN;
	private static final MethodHandle MH_CLOSE;
	private static final MethodHandle MH_CATCH_UP;
	private static final MethodHandle MH_GET_PINNED;
	private static final MethodHandle MH_PINNABLESLICE_VALUE;
	private static final MethodHandle MH_PINNABLESLICE_DESTROY;
	private static final MethodHandle MH_CREATE_SNAPSHOT;
	private static final MethodHandle MH_PROPERTY_VALUE;
	private static final MethodHandle MH_PROPERTY_INT;
	private static final MethodHandle MH_FREE;

	static {
		// rocksdb_t* rocksdb_open_as_secondary(opts*, primary_path*, secondary_path*, errptr**)
		MH_OPEN = RocksDB.lookup("rocksdb_open_as_secondary",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_CLOSE = RocksDB.lookup("rocksdb_close",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

		// void rocksdb_try_catch_up_with_primary(db*, errptr**)
		MH_CATCH_UP = RocksDB.lookup("rocksdb_try_catch_up_with_primary",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		// rocksdb_pinnableslice_t* rocksdb_get_pinned(db*, ro*, key*, klen, errptr**)
		MH_GET_PINNED = RocksDB.lookup("rocksdb_get_pinned",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS));

		// const char* rocksdb_pinnableslice_value(slice*, size_t* vlen)
		MH_PINNABLESLICE_VALUE = RocksDB.lookup("rocksdb_pinnableslice_value",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		// void rocksdb_pinnableslice_destroy(slice*)
		MH_PINNABLESLICE_DESTROY = RocksDB.lookup("rocksdb_pinnableslice_destroy",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

		// const rocksdb_snapshot_t* rocksdb_create_snapshot(db*)
		MH_CREATE_SNAPSHOT = RocksDB.lookup("rocksdb_create_snapshot",
				FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_PROPERTY_VALUE = RocksDB.lookup("rocksdb_property_value",
				FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_PROPERTY_INT = RocksDB.lookup("rocksdb_property_int",
				FunctionDescriptor.of(ValueLayout.JAVA_INT,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_FREE = RocksDB.lookup("rocksdb_free",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));
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

	/**
	 * Opens a secondary instance of the RocksDB database at {@code primaryPath}.
	 *
	 * @param dbOptions     options (caller retains ownership); {@code createIfMissing}
	 *                      is typically {@code false} for a secondary
	 * @param primaryPath   path to the primary database directory
	 * @param secondaryPath a dedicated directory for this secondary's own MANIFEST/WAL
	 *                      tails; created automatically if it does not exist
	 */
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

	/**
	 * Tries to catch up with the primary by reading and applying any new records
	 * from the primary's WAL and newly flushed SST files.
	 *
	 * <p>This is a best-effort operation; the secondary may still lag the primary
	 * if the primary has not yet flushed a write.
	 */
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
	// Read operations
	// -----------------------------------------------------------------------

	/**
	 * Returns the value for {@code key}, or {@code null} if the key does not exist.
	 * Uses PinnableSlice to avoid an intermediate copy from the block cache.
	 */
	public byte[] get(byte[] key) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = Native.errHolder(arena);
			MemorySegment k = Native.toNative(arena, key);

			MemorySegment pin = (MemorySegment) MH_GET_PINNED.invokeExact(
					ptr(), readOpts.ptr(), k, (long) key.length, err);
			Native.checkError(err);

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

	/**
	 * Returns the value for {@code key} using the supplied {@link ReadOptions}
	 * (e.g. for snapshot-pinned reads), or {@code null} if the key does not exist.
	 */
	public byte[] get(ReadOptions readOptions, byte[] key) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = Native.errHolder(arena);
			MemorySegment k = Native.toNative(arena, key);

			MemorySegment pin = (MemorySegment) MH_GET_PINNED.invokeExact(
					ptr(), readOptions.ptr(), k, (long) key.length, err);
			Native.checkError(err);

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
	// Iterator
	// -----------------------------------------------------------------------

	/**
	 * Returns a new iterator over the secondary's current view.
	 * Call {@link #tryCatchUpWithPrimary()} first if you need the latest data.
	 */
	public RocksIterator newIterator() {
		return RocksIterator.create(ptr(), readOpts.ptr());
	}

	/**
	 * Returns a new iterator using the supplied {@link ReadOptions}.
	 */
	public RocksIterator newIterator(ReadOptions readOptions) {
		return RocksIterator.create(ptr(), readOptions.ptr());
	}

	// -----------------------------------------------------------------------
	// Snapshot
	// -----------------------------------------------------------------------

	/**
	 * Creates a point-in-time snapshot of the secondary's current view.
	 * Must be closed after use.
	 */
	public Snapshot getSnapshot() {
		try {
			MemorySegment snapPtr = (MemorySegment) MH_CREATE_SNAPSHOT.invokeExact(ptr());
			return new Snapshot(ptr(), snapPtr);
		} catch (Throwable t) {
			throw RocksDBException.wrap("getSnapshot failed", t);
		}
	}

	// -----------------------------------------------------------------------
	// DB Properties
	// -----------------------------------------------------------------------

	/**
	 * @see RocksDB#getProperty(Property)
	 */
	public Optional<String> getProperty(Property property) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment propSeg = arena.allocateFrom(property.propertyName());
			MemorySegment result = (MemorySegment) MH_PROPERTY_VALUE.invokeExact(ptr(), propSeg);
			if (MemorySegment.NULL.equals(result)) {
				return Optional.empty();
			}
			String value = result.reinterpret(Long.MAX_VALUE).getString(0);
			MH_FREE.invokeExact(result);
			return Optional.of(value);
		} catch (Throwable t) {
			throw RocksDBException.wrap("getProperty failed", t);
		}
	}

	/**
	 * @see RocksDB#getLongProperty(Property)
	 */
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

	// -----------------------------------------------------------------------
	// AutoCloseable
	// -----------------------------------------------------------------------

	@Override
	protected void tryClose(MemorySegment ptr) throws Throwable {
		readOpts.close();
		MH_CLOSE.invokeExact(ptr);
	}
}
