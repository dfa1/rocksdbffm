package io.github.dfa1.rocksdbffm;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.file.Path;

/// FFM wrapper for `rocksdb_backup_engine_options_t`.
///
/// Configure and pass to [BackupEngine#open(BackupEngineOptions, Env)].
///
/// ```
/// try (Env env = Env.defaultEnv();
///      BackupEngineOptions opts = BackupEngineOptions.create(backupDir)
///          .setShareTableFiles(true)
///          .setBackupRateLimit(MemorySize.ofMB(100));
///      BackupEngine engine = BackupEngine.open(opts, env)) {
///     engine.createNewBackup(db);
/// }
/// ```
///
/// ## Env lifecycle
///
/// If [#setEnv(Env)] is called, the provided [Env] must remain open for the
/// lifetime of any [BackupEngine] opened with these options.
public final class BackupEngineOptions extends NativeObject {

	/// `rocksdb_backup_engine_options_t* rocksdb_backup_engine_options_create(const char* backup_dir);`
	private static final MethodHandle MH_CREATE;
	/// `void rocksdb_backup_engine_options_destroy(rocksdb_backup_engine_options_t*);`
	private static final MethodHandle MH_DESTROY;
	/// `void rocksdb_backup_engine_options_set_backup_dir(rocksdb_backup_engine_options_t* options, const char* backup_dir);`
	private static final MethodHandle MH_SET_BACKUP_DIR;
	/// `void rocksdb_backup_engine_options_set_env(rocksdb_backup_engine_options_t* options, rocksdb_env_t* env);`
	private static final MethodHandle MH_SET_ENV;
	/// `void rocksdb_backup_engine_options_set_share_table_files(rocksdb_backup_engine_options_t* options, unsigned char val);`
	private static final MethodHandle MH_SET_SHARE_TABLE_FILES;
	/// `unsigned char rocksdb_backup_engine_options_get_share_table_files(rocksdb_backup_engine_options_t* options);`
	private static final MethodHandle MH_GET_SHARE_TABLE_FILES;
	/// `void rocksdb_backup_engine_options_set_sync(rocksdb_backup_engine_options_t* options, unsigned char val);`
	private static final MethodHandle MH_SET_SYNC;
	/// `unsigned char rocksdb_backup_engine_options_get_sync(rocksdb_backup_engine_options_t* options);`
	private static final MethodHandle MH_GET_SYNC;
	/// `void rocksdb_backup_engine_options_set_destroy_old_data(rocksdb_backup_engine_options_t* options, unsigned char val);`
	private static final MethodHandle MH_SET_DESTROY_OLD_DATA;
	/// `unsigned char rocksdb_backup_engine_options_get_destroy_old_data(rocksdb_backup_engine_options_t* options);`
	private static final MethodHandle MH_GET_DESTROY_OLD_DATA;
	/// `void rocksdb_backup_engine_options_set_backup_log_files(rocksdb_backup_engine_options_t* options, unsigned char val);`
	private static final MethodHandle MH_SET_BACKUP_LOG_FILES;
	/// `unsigned char rocksdb_backup_engine_options_get_backup_log_files(rocksdb_backup_engine_options_t* options);`
	private static final MethodHandle MH_GET_BACKUP_LOG_FILES;
	/// `void rocksdb_backup_engine_options_set_backup_rate_limit(rocksdb_backup_engine_options_t* options, uint64_t limit);`
	private static final MethodHandle MH_SET_BACKUP_RATE_LIMIT;
	/// `uint64_t rocksdb_backup_engine_options_get_backup_rate_limit(rocksdb_backup_engine_options_t* options);`
	private static final MethodHandle MH_GET_BACKUP_RATE_LIMIT;
	/// `void rocksdb_backup_engine_options_set_restore_rate_limit(rocksdb_backup_engine_options_t* options, uint64_t limit);`
	private static final MethodHandle MH_SET_RESTORE_RATE_LIMIT;
	/// `uint64_t rocksdb_backup_engine_options_get_restore_rate_limit(rocksdb_backup_engine_options_t* options);`
	private static final MethodHandle MH_GET_RESTORE_RATE_LIMIT;
	/// `void rocksdb_backup_engine_options_set_max_background_operations(rocksdb_backup_engine_options_t* options, int val);`
	private static final MethodHandle MH_SET_MAX_BACKGROUND_OPERATIONS;
	/// `int rocksdb_backup_engine_options_get_max_background_operations(rocksdb_backup_engine_options_t* options);`
	private static final MethodHandle MH_GET_MAX_BACKGROUND_OPERATIONS;
	/// `void rocksdb_backup_engine_options_set_callback_trigger_interval_size(rocksdb_backup_engine_options_t* options, uint64_t size);`
	private static final MethodHandle MH_SET_CALLBACK_TRIGGER_INTERVAL_SIZE;
	/// `uint64_t rocksdb_backup_engine_options_get_callback_trigger_interval_size(rocksdb_backup_engine_options_t* options);`
	private static final MethodHandle MH_GET_CALLBACK_TRIGGER_INTERVAL_SIZE;
	/// `void rocksdb_backup_engine_options_set_max_valid_backups_to_open(rocksdb_backup_engine_options_t* options, int val);`
	private static final MethodHandle MH_SET_MAX_VALID_BACKUPS_TO_OPEN;
	/// `int rocksdb_backup_engine_options_get_max_valid_backups_to_open(rocksdb_backup_engine_options_t* options);`
	private static final MethodHandle MH_GET_MAX_VALID_BACKUPS_TO_OPEN;
	/// `void rocksdb_backup_engine_options_set_share_files_with_checksum_naming(rocksdb_backup_engine_options_t* options, int val);`
	private static final MethodHandle MH_SET_SHARE_FILES_WITH_CHECKSUM_NAMING;
	/// `int rocksdb_backup_engine_options_get_share_files_with_checksum_naming(rocksdb_backup_engine_options_t* options);`
	private static final MethodHandle MH_GET_SHARE_FILES_WITH_CHECKSUM_NAMING;

	static {
		MH_CREATE = NativeLibrary.lookup("rocksdb_backup_engine_options_create",
				FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_DESTROY = NativeLibrary.lookup("rocksdb_backup_engine_options_destroy",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

		MH_SET_BACKUP_DIR = NativeLibrary.lookup("rocksdb_backup_engine_options_set_backup_dir",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_SET_ENV = NativeLibrary.lookup("rocksdb_backup_engine_options_set_env",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_SET_SHARE_TABLE_FILES = NativeLibrary.lookup(
				"rocksdb_backup_engine_options_set_share_table_files",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_BYTE));

		MH_GET_SHARE_TABLE_FILES = NativeLibrary.lookup(
				"rocksdb_backup_engine_options_get_share_table_files",
				FunctionDescriptor.of(ValueLayout.JAVA_BYTE, ValueLayout.ADDRESS));

		MH_SET_SYNC = NativeLibrary.lookup("rocksdb_backup_engine_options_set_sync",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_BYTE));

		MH_GET_SYNC = NativeLibrary.lookup("rocksdb_backup_engine_options_get_sync",
				FunctionDescriptor.of(ValueLayout.JAVA_BYTE, ValueLayout.ADDRESS));

		MH_SET_DESTROY_OLD_DATA = NativeLibrary.lookup(
				"rocksdb_backup_engine_options_set_destroy_old_data",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_BYTE));

		MH_GET_DESTROY_OLD_DATA = NativeLibrary.lookup(
				"rocksdb_backup_engine_options_get_destroy_old_data",
				FunctionDescriptor.of(ValueLayout.JAVA_BYTE, ValueLayout.ADDRESS));

		MH_SET_BACKUP_LOG_FILES = NativeLibrary.lookup(
				"rocksdb_backup_engine_options_set_backup_log_files",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_BYTE));

		MH_GET_BACKUP_LOG_FILES = NativeLibrary.lookup(
				"rocksdb_backup_engine_options_get_backup_log_files",
				FunctionDescriptor.of(ValueLayout.JAVA_BYTE, ValueLayout.ADDRESS));

		MH_SET_BACKUP_RATE_LIMIT = NativeLibrary.lookup(
				"rocksdb_backup_engine_options_set_backup_rate_limit",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));

		MH_GET_BACKUP_RATE_LIMIT = NativeLibrary.lookup(
				"rocksdb_backup_engine_options_get_backup_rate_limit",
				FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));

		MH_SET_RESTORE_RATE_LIMIT = NativeLibrary.lookup(
				"rocksdb_backup_engine_options_set_restore_rate_limit",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));

		MH_GET_RESTORE_RATE_LIMIT = NativeLibrary.lookup(
				"rocksdb_backup_engine_options_get_restore_rate_limit",
				FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));

		MH_SET_MAX_BACKGROUND_OPERATIONS = NativeLibrary.lookup(
				"rocksdb_backup_engine_options_set_max_background_operations",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_INT));

		MH_GET_MAX_BACKGROUND_OPERATIONS = NativeLibrary.lookup(
				"rocksdb_backup_engine_options_get_max_background_operations",
				FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));

		MH_SET_CALLBACK_TRIGGER_INTERVAL_SIZE = NativeLibrary.lookup(
				"rocksdb_backup_engine_options_set_callback_trigger_interval_size",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));

		MH_GET_CALLBACK_TRIGGER_INTERVAL_SIZE = NativeLibrary.lookup(
				"rocksdb_backup_engine_options_get_callback_trigger_interval_size",
				FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));

		MH_SET_MAX_VALID_BACKUPS_TO_OPEN = NativeLibrary.lookup(
				"rocksdb_backup_engine_options_set_max_valid_backups_to_open",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_INT));

		MH_GET_MAX_VALID_BACKUPS_TO_OPEN = NativeLibrary.lookup(
				"rocksdb_backup_engine_options_get_max_valid_backups_to_open",
				FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));

		MH_SET_SHARE_FILES_WITH_CHECKSUM_NAMING = NativeLibrary.lookup(
				"rocksdb_backup_engine_options_set_share_files_with_checksum_naming",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_INT));

		MH_GET_SHARE_FILES_WITH_CHECKSUM_NAMING = NativeLibrary.lookup(
				"rocksdb_backup_engine_options_get_share_files_with_checksum_naming",
				FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));
	}

	private BackupEngineOptions(MemorySegment ptr) {
		super(ptr);
	}

	/// Creates backup engine options targeting `backupDir`.
	public static BackupEngineOptions create(Path backupDir) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment dirSeg = arena.allocateFrom(backupDir.toString());
			return new BackupEngineOptions((MemorySegment) MH_CREATE.invokeExact(dirSeg));
		} catch (Throwable t) {
			throw RocksDBException.wrap("BackupEngineOptions.create failed", t);
		}
	}

	/// Changes the backup directory. The directory is not created automatically.
	public BackupEngineOptions setBackupDir(Path backupDir) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment dirSeg = arena.allocateFrom(backupDir.toString());
			MH_SET_BACKUP_DIR.invokeExact(ptr(), dirSeg);
			return this;
		} catch (Throwable t) {
			throw RocksDBException.wrap("setBackupDir failed", t);
		}
	}

	/// Sets the environment used for backup I/O.
	///
	/// The [Env] must remain open for the lifetime of any [BackupEngine]
	/// opened with these options. No ownership transfer.
	public BackupEngineOptions setEnv(Env env) {
		try {
			MH_SET_ENV.invokeExact(ptr(), env.ptr());
			return this;
		} catch (Throwable t) {
			throw RocksDBException.wrap("setEnv failed", t);
		}
	}

	/// If `true`, SST files are shared between backups (hard-linked or de-duplicated).
	/// Default: `true`.
	public BackupEngineOptions setShareTableFiles(boolean val) {
		try {
			MH_SET_SHARE_TABLE_FILES.invokeExact(ptr(), val ? (byte) 1 : (byte) 0);
			return this;
		} catch (Throwable t) {
			throw RocksDBException.wrap("setShareTableFiles failed", t);
		}
	}

	public boolean isShareTableFiles() {
		try {
			return (byte) MH_GET_SHARE_TABLE_FILES.invokeExact(ptr()) != 0;
		} catch (Throwable t) {
			throw RocksDBException.wrap("isShareTableFiles failed", t);
		}
	}

	/// If `true`, each file is synced after writing. Safer but slower. Default: `true`.
	public BackupEngineOptions setSync(boolean val) {
		try {
			MH_SET_SYNC.invokeExact(ptr(), val ? (byte) 1 : (byte) 0);
			return this;
		} catch (Throwable t) {
			throw RocksDBException.wrap("setSync failed", t);
		}
	}

	public boolean isSync() {
		try {
			return (byte) MH_GET_SYNC.invokeExact(ptr()) != 0;
		} catch (Throwable t) {
			throw RocksDBException.wrap("isSync failed", t);
		}
	}

	/// If `true`, existing backup data in the backup directory is deleted when the
	/// engine is opened. Default: `false`.
	public BackupEngineOptions setDestroyOldData(boolean val) {
		try {
			MH_SET_DESTROY_OLD_DATA.invokeExact(ptr(), val ? (byte) 1 : (byte) 0);
			return this;
		} catch (Throwable t) {
			throw RocksDBException.wrap("setDestroyOldData failed", t);
		}
	}

	public boolean isDestroyOldData() {
		try {
			return (byte) MH_GET_DESTROY_OLD_DATA.invokeExact(ptr()) != 0;
		} catch (Throwable t) {
			throw RocksDBException.wrap("isDestroyOldData failed", t);
		}
	}

	/// If `true`, WAL/log files are included in backups. Default: `true`.
	public BackupEngineOptions setBackupLogFiles(boolean val) {
		try {
			MH_SET_BACKUP_LOG_FILES.invokeExact(ptr(), val ? (byte) 1 : (byte) 0);
			return this;
		} catch (Throwable t) {
			throw RocksDBException.wrap("setBackupLogFiles failed", t);
		}
	}

	public boolean isBackupLogFiles() {
		try {
			return (byte) MH_GET_BACKUP_LOG_FILES.invokeExact(ptr()) != 0;
		} catch (Throwable t) {
			throw RocksDBException.wrap("isBackupLogFiles failed", t);
		}
	}

	/// Maximum rate at which the backup engine copies files to the backup directory.
	/// `0` means unlimited. Default: `0`.
	public BackupEngineOptions setBackupRateLimit(MemorySize limit) {
		try {
			MH_SET_BACKUP_RATE_LIMIT.invokeExact(ptr(), limit.toBytes());
			return this;
		} catch (Throwable t) {
			throw RocksDBException.wrap("setBackupRateLimit failed", t);
		}
	}

	public MemorySize getBackupRateLimit() {
		try {
			return MemorySize.ofBytes((long) MH_GET_BACKUP_RATE_LIMIT.invokeExact(ptr()));
		} catch (Throwable t) {
			throw RocksDBException.wrap("getBackupRateLimit failed", t);
		}
	}

	/// Maximum rate at which the backup engine copies files during a restore.
	/// `0` means unlimited. Default: `0`.
	public BackupEngineOptions setRestoreRateLimit(MemorySize limit) {
		try {
			MH_SET_RESTORE_RATE_LIMIT.invokeExact(ptr(), limit.toBytes());
			return this;
		} catch (Throwable t) {
			throw RocksDBException.wrap("setRestoreRateLimit failed", t);
		}
	}

	public MemorySize getRestoreRateLimit() {
		try {
			return MemorySize.ofBytes((long) MH_GET_RESTORE_RATE_LIMIT.invokeExact(ptr()));
		} catch (Throwable t) {
			throw RocksDBException.wrap("getRestoreRateLimit failed", t);
		}
	}

	/// Number of background threads used for backup/restore. Default: `1`.
	public BackupEngineOptions setMaxBackgroundOperations(int val) {
		try {
			MH_SET_MAX_BACKGROUND_OPERATIONS.invokeExact(ptr(), val);
			return this;
		} catch (Throwable t) {
			throw RocksDBException.wrap("setMaxBackgroundOperations failed", t);
		}
	}

	public int getMaxBackgroundOperations() {
		try {
			return (int) MH_GET_MAX_BACKGROUND_OPERATIONS.invokeExact(ptr());
		} catch (Throwable t) {
			throw RocksDBException.wrap("getMaxBackgroundOperations failed", t);
		}
	}

	/// How many bytes to copy before invoking the progress callback. Default: `4 MB`.
	public BackupEngineOptions setCallbackTriggerIntervalSize(MemorySize size) {
		try {
			MH_SET_CALLBACK_TRIGGER_INTERVAL_SIZE.invokeExact(ptr(), size.toBytes());
			return this;
		} catch (Throwable t) {
			throw RocksDBException.wrap("setCallbackTriggerIntervalSize failed", t);
		}
	}

	public MemorySize getCallbackTriggerIntervalSize() {
		try {
			return MemorySize.ofBytes((long) MH_GET_CALLBACK_TRIGGER_INTERVAL_SIZE.invokeExact(ptr()));
		} catch (Throwable t) {
			throw RocksDBException.wrap("getCallbackTriggerIntervalSize failed", t);
		}
	}

	/// Number of the most recent backups to open on engine startup.
	/// `-1` means open all. Default: `-1`.
	public BackupEngineOptions setMaxValidBackupsToOpen(int val) {
		try {
			MH_SET_MAX_VALID_BACKUPS_TO_OPEN.invokeExact(ptr(), val);
			return this;
		} catch (Throwable t) {
			throw RocksDBException.wrap("setMaxValidBackupsToOpen failed", t);
		}
	}

	public int getMaxValidBackupsToOpen() {
		try {
			return (int) MH_GET_MAX_VALID_BACKUPS_TO_OPEN.invokeExact(ptr());
		} catch (Throwable t) {
			throw RocksDBException.wrap("getMaxValidBackupsToOpen failed", t);
		}
	}

	/// Naming scheme for shared SST files. See RocksDB docs for valid values.
	/// Default: `1` (kOptionalChecksumWithDbSessionId).
	public BackupEngineOptions setShareFilesWithChecksumNaming(int val) {
		try {
			MH_SET_SHARE_FILES_WITH_CHECKSUM_NAMING.invokeExact(ptr(), val);
			return this;
		} catch (Throwable t) {
			throw RocksDBException.wrap("setShareFilesWithChecksumNaming failed", t);
		}
	}

	public int getShareFilesWithChecksumNaming() {
		try {
			return (int) MH_GET_SHARE_FILES_WITH_CHECKSUM_NAMING.invokeExact(ptr());
		} catch (Throwable t) {
			throw RocksDBException.wrap("getShareFilesWithChecksumNaming failed", t);
		}
	}

	@Override
	protected void tryClose(MemorySegment ptr) throws Throwable {
		MH_DESTROY.invokeExact(ptr);
	}
}
