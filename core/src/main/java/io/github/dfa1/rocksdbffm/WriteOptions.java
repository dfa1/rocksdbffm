package io.github.dfa1.rocksdbffm;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/// FFM wrapper for rocksdb\_writeoptions\_t.
public final class WriteOptions extends NativeObject {

	// rocksdb_writeoptions_create(void) -> rocksdb_writeoptions_t*
	private static final MethodHandle MH_CREATE;
	// rocksdb_writeoptions_destroy(rocksdb_writeoptions_t*) -> void
	private static final MethodHandle MH_DESTROY;

	static {
		MH_CREATE = RocksDB.lookup("rocksdb_writeoptions_create",
				FunctionDescriptor.of(ValueLayout.ADDRESS));

		MH_DESTROY = RocksDB.lookup("rocksdb_writeoptions_destroy",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));
	}

	private WriteOptions(MemorySegment ptr) {
		super(ptr);
	}

	public static WriteOptions newWriteOptions() {
		try {
			return new WriteOptions((MemorySegment) MH_CREATE.invokeExact());
		} catch (Throwable t) {
			throw new RocksDBException("writeoptions create failed", t);
		}
	}

	@Override
	protected void tryClose(MemorySegment ptr) throws Throwable {
		MH_DESTROY.invokeExact(ptr);
	}
}
