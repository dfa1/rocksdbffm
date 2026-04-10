package io.github.dfa1.rocksdbffm;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

// TODO: a lot of options are missing?
/// FFM wrapper for `rocksdb_readoptions_t`.
public final class ReadOptions extends NativeObject {

	/// `rocksdb_readoptions_t* rocksdb_readoptions_create(void);`
	private static final MethodHandle MH_CREATE;
	/// `void rocksdb_readoptions_destroy(rocksdb_readoptions_t*);`
	private static final MethodHandle MH_DESTROY;
	/// `void rocksdb_readoptions_set_snapshot(rocksdb_readoptions_t*, const rocksdb_snapshot_t*);`
	private static final MethodHandle MH_SET_SNAPSHOT;

	static {
		MH_CREATE = NativeLibrary.lookup("rocksdb_readoptions_create",
				FunctionDescriptor.of(ValueLayout.ADDRESS));

		MH_DESTROY = NativeLibrary.lookup("rocksdb_readoptions_destroy",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

		MH_SET_SNAPSHOT = NativeLibrary.lookup("rocksdb_readoptions_set_snapshot",
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

	/// Pins reads to the given snapshot, providing a consistent point-in-time view.
	/// The snapshot must remain open for the lifetime of these read options.
	/// Pass `null` to clear a previously set snapshot.
	///
	/// @return `this` for chaining
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
