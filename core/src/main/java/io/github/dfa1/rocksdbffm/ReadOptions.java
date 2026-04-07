package io.github.dfa1.rocksdbffm;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/**
 * FFM wrapper for rocksdb_readoptions_t.
 */
public final class ReadOptions extends NativeObject {

	// rocksdb_readoptions_create(void) -> rocksdb_readoptions_t*
	private static final MethodHandle MH_CREATE;
	// rocksdb_readoptions_destroy(rocksdb_readoptions_t*) -> void
	private static final MethodHandle MH_DESTROY;
	// rocksdb_readoptions_set_snapshot(rocksdb_readoptions_t*, const rocksdb_snapshot_t*) -> void
	private static final MethodHandle MH_SET_SNAPSHOT;

	static {
		MH_CREATE = RocksDB.lookup("rocksdb_readoptions_create",
				FunctionDescriptor.of(ValueLayout.ADDRESS));

		MH_DESTROY = RocksDB.lookup("rocksdb_readoptions_destroy",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

		// void rocksdb_readoptions_set_snapshot(ro*, snap*)
		MH_SET_SNAPSHOT = RocksDB.lookup("rocksdb_readoptions_set_snapshot",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS));
	}

	private ReadOptions(MemorySegment ptr) {
		super(ptr);
	}

	public static ReadOptions newReadOptions() {
		try {
			return new ReadOptions((MemorySegment) MH_CREATE.invokeExact());
		} catch (Throwable t) {
			throw new RocksDBException("readoptions create failed", t);
		}
	}

	/**
	 * Pins reads to the given snapshot, providing a consistent point-in-time view.
	 * The snapshot must remain open for the lifetime of these read options.
	 * Pass {@code null} to clear a previously set snapshot.
	 *
	 * @return {@code this} for chaining
	 */
	public ReadOptions setSnapshot(Snapshot snapshot) {
		try {
			MemorySegment snapPtr = (snapshot == null) ? MemorySegment.NULL : snapshot.ptr();
			MH_SET_SNAPSHOT.invokeExact(ptr(), snapPtr);
			return this;
		} catch (Throwable t) {
			throw new RocksDBException("readoptions setSnapshot failed", t);
		}
	}

	@Override
	protected void tryClose(MemorySegment ptr) throws Throwable {
		MH_DESTROY.invokeExact(ptr);
	}
}
