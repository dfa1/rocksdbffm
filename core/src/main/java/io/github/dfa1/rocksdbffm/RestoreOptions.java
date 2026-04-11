package io.github.dfa1.rocksdbffm;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/// FFM wrapper for `rocksdb_restore_options_t`.
///
/// Controls behaviour when restoring a backup via [BackupEngine].
///
/// ```
/// try (var restoreOpts = RestoreOptions.create().setKeepLogFiles(false);
///      var engine = BackupEngine.open(backupEngineOpts, env)) {
///     engine.restoreDbFromLatestBackup(dbDir, restoreOpts);
/// }
/// ```
public final class RestoreOptions extends NativeObject {

	/// `rocksdb_restore_options_t* rocksdb_restore_options_create(void);`
	private static final MethodHandle MH_CREATE;
	/// `void rocksdb_restore_options_destroy(rocksdb_restore_options_t* opt);`
	private static final MethodHandle MH_DESTROY;
	/// `void rocksdb_restore_options_set_keep_log_files(rocksdb_restore_options_t* opt, int v);`
	private static final MethodHandle MH_SET_KEEP_LOG_FILES;

	static {
		MH_CREATE = NativeLibrary.lookup("rocksdb_restore_options_create",
				FunctionDescriptor.of(ValueLayout.ADDRESS));

		MH_DESTROY = NativeLibrary.lookup("rocksdb_restore_options_destroy",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

		MH_SET_KEEP_LOG_FILES = NativeLibrary.lookup("rocksdb_restore_options_set_keep_log_files",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_INT));
	}

	private RestoreOptions(MemorySegment ptr) {
		super(ptr);
	}

	public static RestoreOptions create() {
		try {
			return new RestoreOptions((MemorySegment) MH_CREATE.invokeExact());
		} catch (Throwable t) {
			throw RocksDBException.wrap("RestoreOptions.create failed", t);
		}
	}

	/// If `true`, existing WAL files in the destination directory are kept.
	/// Default: `false` (WAL files are removed before restore).
	public RestoreOptions setKeepLogFiles(boolean keepLogFiles) {
		try {
			MH_SET_KEEP_LOG_FILES.invokeExact(ptr(), keepLogFiles ? 1 : 0);
			return this;
		} catch (Throwable t) {
			throw RocksDBException.wrap("setKeepLogFiles failed", t);
		}
	}

	@Override
	protected void tryClose(MemorySegment ptr) throws Throwable {
		MH_DESTROY.invokeExact(ptr);
	}
}
