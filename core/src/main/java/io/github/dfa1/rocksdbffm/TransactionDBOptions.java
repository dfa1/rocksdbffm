package io.github.dfa1.rocksdbffm;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/**
 * FFM wrapper for rocksdb_transactiondb_options_t.
 */
public final class TransactionDBOptions extends NativeObject {

	// rocksdb_transactiondb_options_create(void) -> rocksdb_transactiondb_options_t*
	private static final MethodHandle MH_CREATE;
	// rocksdb_transactiondb_options_destroy(rocksdb_transactiondb_options_t* opt) -> void
	private static final MethodHandle MH_DESTROY;
	// rocksdb_transactiondb_options_set_max_num_locks(rocksdb_transactiondb_options_t* opt, int64_t max_num_locks) -> void
	private static final MethodHandle MH_SET_MAX_NUM_LOCKS;
	// rocksdb_transactiondb_options_set_num_stripes(rocksdb_transactiondb_options_t* opt, size_t num_stripes) -> void
	private static final MethodHandle MH_SET_NUM_STRIPES;

	static {
		MH_CREATE = RocksDB.lookup("rocksdb_transactiondb_options_create",
				FunctionDescriptor.of(ValueLayout.ADDRESS));

		MH_DESTROY = RocksDB.lookup("rocksdb_transactiondb_options_destroy",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

		// void rocksdb_transactiondb_options_set_max_num_locks(opt*, int64_t)
		MH_SET_MAX_NUM_LOCKS = RocksDB.lookup("rocksdb_transactiondb_options_set_max_num_locks",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));

		// void rocksdb_transactiondb_options_set_num_stripes(opt*, size_t)
		MH_SET_NUM_STRIPES = RocksDB.lookup("rocksdb_transactiondb_options_set_num_stripes",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));
	}

	private TransactionDBOptions(MemorySegment ptr) {
		super(ptr);
	}

	public static TransactionDBOptions newTransactionDBOptions() {
		try {
			return new TransactionDBOptions((MemorySegment) MH_CREATE.invokeExact());
		} catch (Throwable t) {
			throw new RocksDBException("transactiondb options create failed", t);
		}
	}

	/**
	 * Maximum number of locks held simultaneously. Default: -1 (unlimited).
	 */
	public TransactionDBOptions setMaxNumLocks(long maxNumLocks) {
		try {
			MH_SET_MAX_NUM_LOCKS.invokeExact(ptr(), maxNumLocks);
		} catch (Throwable t) {
			throw new RocksDBException("setMaxNumLocks failed", t);
		}
		return this;
	}

	/**
	 * Number of sub-lock-tables. Increasing reduces lock contention. Default: 16.
	 */
	public TransactionDBOptions setNumStripes(long numStripes) {
		try {
			MH_SET_NUM_STRIPES.invokeExact(ptr(), numStripes);
		} catch (Throwable t) {
			throw new RocksDBException("setNumStripes failed", t);
		}
		return this;
	}

	@Override
	protected void tryClose(MemorySegment ptr) throws Throwable {
		MH_DESTROY.invokeExact(ptr);
	}
}
