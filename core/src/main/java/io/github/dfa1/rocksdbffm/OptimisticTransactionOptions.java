package io.github.dfa1.rocksdbffm;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/// FFM wrapper for `rocksdb_optimistictransaction_options_t`.
///
/// Unlike pessimistic [TransactionOptions], there is no deadlock detection
/// or lock timeout — conflicts are detected at [Transaction#commit()] time.
public final class OptimisticTransactionOptions extends NativeObject {

	/// `rocksdb_optimistictransaction_options_t* rocksdb_optimistictransaction_options_create(void);`
	private static final MethodHandle MH_CREATE;
	/// `void rocksdb_optimistictransaction_options_destroy(rocksdb_optimistictransaction_options_t* opt);`
	private static final MethodHandle MH_DESTROY;
	/// `void rocksdb_optimistictransaction_options_set_set_snapshot(rocksdb_optimistictransaction_options_t* opt, unsigned char v);`
	private static final MethodHandle MH_SET_SET_SNAPSHOT;

	static {
		MH_CREATE = NativeLibrary.lookup("rocksdb_optimistictransaction_options_create",
				FunctionDescriptor.of(ValueLayout.ADDRESS));

		MH_DESTROY = NativeLibrary.lookup("rocksdb_optimistictransaction_options_destroy",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

		MH_SET_SET_SNAPSHOT = NativeLibrary.lookup(
				"rocksdb_optimistictransaction_options_set_set_snapshot",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_BYTE));
	}

	private OptimisticTransactionOptions(MemorySegment ptr) {
		super(ptr);
	}

	public static OptimisticTransactionOptions newOptimisticTransactionOptions() {
		try {
			return new OptimisticTransactionOptions((MemorySegment) MH_CREATE.invokeExact());
		} catch (Throwable t) {
			throw new RocksDBException("optimistic transaction options create failed", t);
		}
	}

	/// If `true`, a snapshot is taken at the start of the transaction.
	/// The transaction will then check whether any keys it reads or writes
	/// have been modified since that snapshot when [Transaction#commit()] is called.
	/// Default: `false`.
	public OptimisticTransactionOptions setSetSnapshot(boolean value) {
		try {
			MH_SET_SET_SNAPSHOT.invokeExact(ptr(), value ? (byte) 1 : (byte) 0);
		} catch (Throwable t) {
			throw new RocksDBException("setSetSnapshot failed", t);
		}
		return this;
	}

	@Override
	protected void tryClose(MemorySegment ptr) throws Throwable {
		MH_DESTROY.invokeExact(ptr);
	}
}
