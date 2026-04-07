package io.github.dfa1.rocksdbffm;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/**
 * FFM wrapper for rocksdb_writeoptions_t.
 */
public final class WriteOptions implements AutoCloseable {

	static final MethodHandle MH_CREATE;
	static final MethodHandle MH_DESTROY;

	static {
		MH_CREATE = RocksDB.lookup("rocksdb_writeoptions_create",
				FunctionDescriptor.of(ValueLayout.ADDRESS));

		MH_DESTROY = RocksDB.lookup("rocksdb_writeoptions_destroy",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));
	}

	/**
	 * Package-private: accessed by TransactionDB.beginTransaction().
	 */
	final MemorySegment ptr;

	public WriteOptions() {
		try {
			this.ptr = (MemorySegment) MH_CREATE.invokeExact();
		} catch (Throwable t) {
			throw new RocksDBException("writeoptions create failed", t);
		}
	}

	@Override
	public void close() {
		try {
			MH_DESTROY.invokeExact(ptr);
		} catch (Throwable t) {
			throw new RocksDBException("writeoptions destroy failed", t);
		}
	}
}
