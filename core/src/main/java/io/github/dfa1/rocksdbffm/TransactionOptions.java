package io.github.dfa1.rocksdbffm;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/// FFM wrapper for `rocksdb_transaction_options_t`.
public final class TransactionOptions extends NativeObject {

	// rocksdb_transaction_options_create(void) -> rocksdb_transaction_options_t*
	private static final MethodHandle MH_CREATE;
	// rocksdb_transaction_options_destroy(rocksdb_transaction_options_t* opt) -> void
	private static final MethodHandle MH_DESTROY;
	// rocksdb_transaction_options_set_set_snapshot(rocksdb_transaction_options_t* opt, unsigned char v) -> void
	private static final MethodHandle MH_SET_SET_SNAPSHOT;
	// rocksdb_transaction_options_set_deadlock_detect(rocksdb_transaction_options_t* opt, unsigned char v) -> void
	private static final MethodHandle MH_SET_DEADLOCK_DETECT;
	// rocksdb_transaction_options_set_lock_timeout(rocksdb_transaction_options_t* opt, int64_t lock_timeout) -> void
	private static final MethodHandle MH_SET_LOCK_TIMEOUT;

	static {
		MH_CREATE = RocksDB.lookup("rocksdb_transaction_options_create",
				FunctionDescriptor.of(ValueLayout.ADDRESS));

		MH_DESTROY = RocksDB.lookup("rocksdb_transaction_options_destroy",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

		// void rocksdb_transaction_options_set_set_snapshot(opt*, unsigned char v)
		MH_SET_SET_SNAPSHOT = RocksDB.lookup("rocksdb_transaction_options_set_set_snapshot",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_BYTE));

		// void rocksdb_transaction_options_set_deadlock_detect(opt*, unsigned char v)
		MH_SET_DEADLOCK_DETECT = RocksDB.lookup("rocksdb_transaction_options_set_deadlock_detect",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_BYTE));

		// void rocksdb_transaction_options_set_lock_timeout(opt*, int64_t lock_timeout)
		MH_SET_LOCK_TIMEOUT = RocksDB.lookup("rocksdb_transaction_options_set_lock_timeout",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));
	}

	private TransactionOptions(MemorySegment ptr) {
		super(ptr);
	}

	public static TransactionOptions newTransactionOptions() {
		try {
			return new TransactionOptions((MemorySegment) MH_CREATE.invokeExact());
		} catch (Throwable t) {
			throw new RocksDBException("transaction options create failed", t);
		}
	}

	/// If true, a snapshot is taken at the start of each transaction.
	/// Default: false.
	public TransactionOptions setSetSnapshot(boolean value) {
		try {
			MH_SET_SET_SNAPSHOT.invokeExact(ptr(), value ? (byte) 1 : (byte) 0);
		} catch (Throwable t) {
			throw new RocksDBException("setSetSnapshot failed", t);
		}
		return this;
	}

	/// If true, the transaction will detect deadlocks and return an error
	/// instead of waiting. Default: false.
	public TransactionOptions setDeadlockDetect(boolean value) {
		try {
			MH_SET_DEADLOCK_DETECT.invokeExact(ptr(), value ? (byte) 1 : (byte) 0);
		} catch (Throwable t) {
			throw new RocksDBException("setDeadlockDetect failed", t);
		}
		return this;
	}

	/// Timeout (in milliseconds) to wait for a lock. `-1` means wait forever,
	/// `0` means fail immediately if a lock is not available. Default: -1.
	public TransactionOptions setLockTimeout(long lockTimeout) {
		try {
			MH_SET_LOCK_TIMEOUT.invokeExact(ptr(), lockTimeout);
		} catch (Throwable t) {
			throw new RocksDBException("setLockTimeout failed", t);
		}
		return this;
	}

	@Override
	protected void tryClose(MemorySegment ptr) throws Throwable {
		MH_DESTROY.invokeExact(ptr);
	}
}
