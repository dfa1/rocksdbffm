package io.github.dfa1.rocksdbffm;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/**
 * FFM wrapper for rocksdb_readoptions_t.
 */
public final class ReadOptions implements AutoCloseable {

	static final MethodHandle MH_CREATE;
	static final MethodHandle MH_DESTROY;
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

	/**
	 * Package-private: accessed by Transaction.get() and RocksDB.
	 */
	final MemorySegment ptr;

	public ReadOptions() {
		try {
			this.ptr = (MemorySegment) MH_CREATE.invokeExact();
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
			MemorySegment snapPtr = (snapshot == null) ? MemorySegment.NULL : snapshot.ptr;
			MH_SET_SNAPSHOT.invokeExact(ptr, snapPtr);
			return this;
		} catch (Throwable t) {
			throw new RocksDBException("readoptions setSnapshot failed", t);
		}
	}

	@Override
	public void close() {
		Native.closeQuietly(() -> MH_DESTROY.invokeExact(ptr));
	}
}
