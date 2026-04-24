package io.github.dfa1.rocksdbffm;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/// FFM wrapper for `rocksdb_backup_engine_t`.
///
/// Creates, lists, verifies, and restores incremental RocksDB backups.
///
/// ```
/// var backupDir = Path.of("/backups");
/// var dbDir     = Path.of("/data");
///
/// // Create a backup
/// try (var opts   = Options.newOptions().setCreateIfMissing(true);
///      var db     = RocksDB.open(opts, dbDir);
///      var engine = BackupEngine.open(opts, backupDir)) {
///     db.put("k".getBytes(), "v".getBytes());
///     engine.createNewBackup(db);
/// }
///
/// // Restore the latest backup to a new directory
/// var restoreDir = Path.of("/restored");
/// try (var env       = Env.defaultEnv();
///      var beOpts    = BackupEngineOptions.create(backupDir);
///      var restOpts  = RestoreOptions.create();
///      var engine    = BackupEngine.open(beOpts, env)) {
///     engine.restoreDbFromLatestBackup(restoreDir, restOpts);
/// }
/// ```
public final class BackupEngine extends NativeObject {

	/// `rocksdb_backup_engine_t* rocksdb_backup_engine_open(const rocksdb_options_t* options, const char* path, char** errptr);`
	private static final MethodHandle MH_OPEN;
	/// `rocksdb_backup_engine_t* rocksdb_backup_engine_open_opts(const rocksdb_backup_engine_options_t* options, rocksdb_env_t* env, char** errptr);`
	private static final MethodHandle MH_OPEN_OPTS;
	/// `void rocksdb_backup_engine_create_new_backup_flush(rocksdb_backup_engine_t* be, rocksdb_t* db, unsigned char flush_before_backup, char** errptr);`
	private static final MethodHandle MH_CREATE_NEW_BACKUP_FLUSH;
	/// `void rocksdb_backup_engine_purge_old_backups(rocksdb_backup_engine_t* be, uint32_t num_backups_to_keep, char** errptr);`
	private static final MethodHandle MH_PURGE_OLD_BACKUPS;
	/// `void rocksdb_backup_engine_verify_backup(rocksdb_backup_engine_t* be, uint32_t backup_id, char** errptr);`
	private static final MethodHandle MH_VERIFY_BACKUP;
	/// `void rocksdb_backup_engine_restore_db_from_latest_backup(rocksdb_backup_engine_t* be, const char* db_dir, const char* wal_dir, const rocksdb_restore_options_t* restore_options, char** errptr);`
	private static final MethodHandle MH_RESTORE_FROM_LATEST;
	/// `void rocksdb_backup_engine_restore_db_from_backup(rocksdb_backup_engine_t* be, const char* db_dir, const char* wal_dir, const rocksdb_restore_options_t* restore_options, const uint32_t backup_id, char** errptr);`
	private static final MethodHandle MH_RESTORE_FROM_BACKUP;
	/// `const rocksdb_backup_engine_info_t* rocksdb_backup_engine_get_backup_info(rocksdb_backup_engine_t* be);`
	private static final MethodHandle MH_GET_BACKUP_INFO;
	/// `int rocksdb_backup_engine_info_count(const rocksdb_backup_engine_info_t* info);`
	private static final MethodHandle MH_INFO_COUNT;
	/// `int64_t rocksdb_backup_engine_info_timestamp(const rocksdb_backup_engine_info_t* info, int index);`
	private static final MethodHandle MH_INFO_TIMESTAMP;
	/// `uint32_t rocksdb_backup_engine_info_backup_id(const rocksdb_backup_engine_info_t* info, int index);`
	private static final MethodHandle MH_INFO_BACKUP_ID;
	/// `uint64_t rocksdb_backup_engine_info_size(const rocksdb_backup_engine_info_t* info, int index);`
	private static final MethodHandle MH_INFO_SIZE;
	/// `uint32_t rocksdb_backup_engine_info_number_files(const rocksdb_backup_engine_info_t* info, int index);`
	private static final MethodHandle MH_INFO_NUMBER_FILES;
	/// `void rocksdb_backup_engine_info_destroy(const rocksdb_backup_engine_info_t* info);`
	private static final MethodHandle MH_INFO_DESTROY;
	/// `void rocksdb_backup_engine_close(rocksdb_backup_engine_t* be);`
	private static final MethodHandle MH_CLOSE;

	static {
		MH_OPEN = NativeLibrary.lookup("rocksdb_backup_engine_open",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS,   // options
						ValueLayout.ADDRESS,   // path
						ValueLayout.ADDRESS)); // errptr

		MH_OPEN_OPTS = NativeLibrary.lookup("rocksdb_backup_engine_open_opts",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS,   // options
						ValueLayout.ADDRESS,   // env
						ValueLayout.ADDRESS)); // errptr

		MH_CREATE_NEW_BACKUP_FLUSH = NativeLibrary.lookup(
				"rocksdb_backup_engine_create_new_backup_flush",
				FunctionDescriptor.ofVoid(
						ValueLayout.ADDRESS,   // be
						ValueLayout.ADDRESS,   // db
						ValueLayout.JAVA_BYTE, // flush_before_backup
						ValueLayout.ADDRESS)); // errptr

		MH_PURGE_OLD_BACKUPS = NativeLibrary.lookup("rocksdb_backup_engine_purge_old_backups",
				FunctionDescriptor.ofVoid(
						ValueLayout.ADDRESS,  // be
						ValueLayout.JAVA_INT, // num_backups_to_keep (uint32_t)
						ValueLayout.ADDRESS)); // errptr

		MH_VERIFY_BACKUP = NativeLibrary.lookup("rocksdb_backup_engine_verify_backup",
				FunctionDescriptor.ofVoid(
						ValueLayout.ADDRESS,  // be
						ValueLayout.JAVA_INT, // backup_id (uint32_t)
						ValueLayout.ADDRESS)); // errptr

		MH_RESTORE_FROM_LATEST = NativeLibrary.lookup(
				"rocksdb_backup_engine_restore_db_from_latest_backup",
				FunctionDescriptor.ofVoid(
						ValueLayout.ADDRESS,  // be
						ValueLayout.ADDRESS,  // db_dir
						ValueLayout.ADDRESS,  // wal_dir
						ValueLayout.ADDRESS,  // restore_options
						ValueLayout.ADDRESS)); // errptr

		MH_RESTORE_FROM_BACKUP = NativeLibrary.lookup(
				"rocksdb_backup_engine_restore_db_from_backup",
				FunctionDescriptor.ofVoid(
						ValueLayout.ADDRESS,  // be
						ValueLayout.ADDRESS,  // db_dir
						ValueLayout.ADDRESS,  // wal_dir
						ValueLayout.ADDRESS,  // restore_options
						ValueLayout.JAVA_INT, // backup_id (uint32_t)
						ValueLayout.ADDRESS)); // errptr

		MH_GET_BACKUP_INFO = NativeLibrary.lookup("rocksdb_backup_engine_get_backup_info",
				FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_INFO_COUNT = NativeLibrary.lookup("rocksdb_backup_engine_info_count",
				FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));

		MH_INFO_TIMESTAMP = NativeLibrary.lookup("rocksdb_backup_engine_info_timestamp",
				FunctionDescriptor.of(ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS,  // info
						ValueLayout.JAVA_INT)); // index

		MH_INFO_BACKUP_ID = NativeLibrary.lookup("rocksdb_backup_engine_info_backup_id",
				FunctionDescriptor.of(ValueLayout.JAVA_INT,
						ValueLayout.ADDRESS,  // info
						ValueLayout.JAVA_INT)); // index

		MH_INFO_SIZE = NativeLibrary.lookup("rocksdb_backup_engine_info_size",
				FunctionDescriptor.of(ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS,  // info
						ValueLayout.JAVA_INT)); // index

		MH_INFO_NUMBER_FILES = NativeLibrary.lookup("rocksdb_backup_engine_info_number_files",
				FunctionDescriptor.of(ValueLayout.JAVA_INT,
						ValueLayout.ADDRESS,  // info
						ValueLayout.JAVA_INT)); // index

		MH_INFO_DESTROY = NativeLibrary.lookup("rocksdb_backup_engine_info_destroy",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

		MH_CLOSE = NativeLibrary.lookup("rocksdb_backup_engine_close",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));
	}

	private BackupEngine(MemorySegment ptr) {
		super(ptr);
	}

	// -----------------------------------------------------------------------
	// Factory methods
	// -----------------------------------------------------------------------

	/// Opens (or creates) a backup engine using the env and info_log from `options`,
	/// storing backups under `backupPath`.
	///
	/// The `backupPath` directory is created if it does not exist.
	///
	/// @param options the DB options supplying the env and info_log
	/// @param backupPath directory where backups are stored (created if absent)
	/// @return a new [BackupEngine] instance
	public static BackupEngine open(Options options, Path backupPath) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = RocksDB.errHolder(arena);
			MemorySegment pathSeg = arena.allocateFrom(backupPath.toString());
			MemorySegment ptr = (MemorySegment) MH_OPEN.invokeExact(options.ptr(), pathSeg, err);
			RocksDB.checkError(err);
			return new BackupEngine(ptr);
		} catch (Throwable t) {
			throw RocksDBException.wrap("BackupEngine.open failed", t);
		}
	}

	/// Opens (or creates) a backup engine from explicit [BackupEngineOptions] and [Env].
	///
	/// Use this overload when you need full control over backup options (rate limits,
	/// sharing strategy, custom env, etc.).
	///
	/// @param options the backup engine options
	/// @param env the environment for backup I/O
	/// @return a new [BackupEngine] instance
	public static BackupEngine open(BackupEngineOptions options, Env env) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = RocksDB.errHolder(arena);
			MemorySegment ptr = (MemorySegment) MH_OPEN_OPTS.invokeExact(options.ptr(), env.ptr(), err);
			RocksDB.checkError(err);
			return new BackupEngine(ptr);
		} catch (Throwable t) {
			throw RocksDBException.wrap("BackupEngine.open failed", t);
		}
	}

	// -----------------------------------------------------------------------
	// Create backup
	// -----------------------------------------------------------------------

	/// Creates a new incremental backup of `db`.
	///
	/// @param db the database to back up
	/// @param flushBeforeBackup if `true`, the memtable is flushed to SST before
	///                          the backup so that the backup does not include WAL entries
	public void createNewBackup(ReadWriteDB db, boolean flushBeforeBackup) {
		createBackup(db.ptr(), flushBeforeBackup);
	}

	/// Creates a new incremental backup without pre-flushing the memtable.
	///
	/// @param db the database to back up
	public void createNewBackup(ReadWriteDB db) {
		createBackup(db.ptr(), false);
	}

	/// Creates a new incremental backup of a [BlobDB].
	///
	/// @param db the blob database to back up
	/// @param flushBeforeBackup if `true`, flushes the memtable before backup
	/// @see #createNewBackup(ReadWriteDB, boolean)
	public void createNewBackup(BlobDB db, boolean flushBeforeBackup) {
		createBackup(db.ptr(), flushBeforeBackup);
	}

	/// Creates a new incremental backup of a [BlobDB] without pre-flushing the memtable.
	///
	/// @param db the blob database to back up
	/// @see #createNewBackup(ReadWriteDB)
	public void createNewBackup(BlobDB db) {
		createBackup(db.ptr(), false);
	}

	/// Creates a new incremental backup of a [TtlDB].
	///
	/// @param db the TTL database to back up
	/// @param flushBeforeBackup if `true`, flushes the memtable before backup
	/// @see #createNewBackup(ReadWriteDB, boolean)
	public void createNewBackup(TtlDB db, boolean flushBeforeBackup) {
		createBackup(db.ptr(), flushBeforeBackup);
	}

	/// Creates a new incremental backup of a [TtlDB] without pre-flushing the memtable.
	///
	/// @param db the TTL database to back up
	/// @see #createNewBackup(ReadWriteDB)
	public void createNewBackup(TtlDB db) {
		createBackup(db.ptr(), false);
	}

	private void createBackup(MemorySegment dbPtr, boolean flush) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = RocksDB.errHolder(arena);
			MH_CREATE_NEW_BACKUP_FLUSH.invokeExact(ptr(), dbPtr, flush ? (byte) 1 : (byte) 0, err);
			RocksDB.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("createNewBackup failed", t);
		}
	}

	// -----------------------------------------------------------------------
	// Maintenance
	// -----------------------------------------------------------------------

	/// Deletes all but the `numBackupsToKeep` most recent backups.
	///
	/// @param numBackupsToKeep number of recent backups to retain
	public void purgeOldBackups(int numBackupsToKeep) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = RocksDB.errHolder(arena);
			MH_PURGE_OLD_BACKUPS.invokeExact(ptr(), numBackupsToKeep, err);
			RocksDB.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("purgeOldBackups failed", t);
		}
	}

	/// Verifies the checksum and file count of the backup identified by `backupId`.
	///
	/// @param backupId the identifier of the backup to verify
	/// @throws RocksDBException if the backup is corrupt or not found
	public void verifyBackup(BackupId backupId) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = RocksDB.errHolder(arena);
			MH_VERIFY_BACKUP.invokeExact(ptr(), backupId.toNativeInt(), err);
			RocksDB.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("verifyBackup failed", t);
		}
	}

	// -----------------------------------------------------------------------
	// Restore
	// -----------------------------------------------------------------------

	/// Restores the most recent backup to `dbDir`, using the same path for WAL files.
	///
	/// The destination directory must not be an open database. Close the database
	/// before restoring.
	///
	/// @param dbDir destination directory for the restored database
	/// @param restoreOptions options controlling the restore behaviour
	public void restoreDbFromLatestBackup(Path dbDir, RestoreOptions restoreOptions) {
		restoreDbFromLatestBackup(dbDir, dbDir, restoreOptions);
	}

	/// Restores the most recent backup to `dbDir` with WAL files written to `walDir`.
	///
	/// @param dbDir destination directory for the restored database
	/// @param walDir destination directory for WAL files
	/// @param restoreOptions options controlling the restore behaviour
	public void restoreDbFromLatestBackup(Path dbDir, Path walDir, RestoreOptions restoreOptions) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = RocksDB.errHolder(arena);
			MemorySegment dbDirSeg = arena.allocateFrom(dbDir.toString());
			MemorySegment walDirSeg = arena.allocateFrom(walDir.toString());
			MH_RESTORE_FROM_LATEST.invokeExact(ptr(), dbDirSeg, walDirSeg, restoreOptions.ptr(), err);
			RocksDB.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("restoreDbFromLatestBackup failed", t);
		}
	}

	/// Restores the backup identified by `backupId` to `dbDir`, using the same path for WAL files.
	///
	/// @param backupId the identifier of the backup to restore
	/// @param dbDir destination directory for the restored database
	/// @param restoreOptions options controlling the restore behaviour
	public void restoreDbFromBackup(BackupId backupId, Path dbDir, RestoreOptions restoreOptions) {
		restoreDbFromBackup(backupId, dbDir, dbDir, restoreOptions);
	}

	/// Restores the backup identified by `backupId` to `dbDir` with WAL files written to `walDir`.
	///
	/// @param backupId the identifier of the backup to restore
	/// @param dbDir destination directory for the restored database
	/// @param walDir destination directory for WAL files
	/// @param restoreOptions options controlling the restore behaviour
	public void restoreDbFromBackup(BackupId backupId, Path dbDir, Path walDir, RestoreOptions restoreOptions) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = RocksDB.errHolder(arena);
			MemorySegment dbDirSeg = arena.allocateFrom(dbDir.toString());
			MemorySegment walDirSeg = arena.allocateFrom(walDir.toString());
			MH_RESTORE_FROM_BACKUP.invokeExact(ptr(), dbDirSeg, walDirSeg,
					restoreOptions.ptr(), backupId.toNativeInt(), err);
			RocksDB.checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("restoreDbFromBackup failed", t);
		}
	}

	// -----------------------------------------------------------------------
	// Query
	// -----------------------------------------------------------------------

	/// Returns an immutable snapshot of all backup metadata, ordered oldest-first.
	///
	/// The native info pointer is acquired, iterated, and destroyed entirely
	/// within this call — no native resource escapes.
	///
	/// @return unmodifiable list of [BackupInfo] records, one per existing backup
	public List<BackupInfo> getBackupInfo() {
		MemorySegment infoPtr;
		try {
			infoPtr = (MemorySegment) MH_GET_BACKUP_INFO.invokeExact(ptr());
		} catch (Throwable t) {
			throw RocksDBException.wrap("getBackupInfo failed", t);
		}
		try {
			int count = (int) MH_INFO_COUNT.invokeExact(infoPtr);
			List<BackupInfo> result = new ArrayList<>(count);
			for (int i = 0; i < count; i++) {
				BackupId backupId = BackupId.fromNative((int) MH_INFO_BACKUP_ID.invokeExact(infoPtr, i));
				long timestamp = (long) MH_INFO_TIMESTAMP.invokeExact(infoPtr, i);
				MemorySize size = MemorySize.ofBytes((long) MH_INFO_SIZE.invokeExact(infoPtr, i));
				long numberOfFiles = Integer.toUnsignedLong((int) MH_INFO_NUMBER_FILES.invokeExact(infoPtr, i));
				result.add(new BackupInfo(backupId, timestamp, size, numberOfFiles));
			}
			return Collections.unmodifiableList(result);
		} catch (Throwable t) {
			throw RocksDBException.wrap("getBackupInfo failed", t);
		} finally {
			try {
				MH_INFO_DESTROY.invokeExact(infoPtr);
			} catch (Throwable ignored) {
			}
		}
	}

	@Override
	protected void tryClose(MemorySegment ptr) throws Throwable {
		MH_CLOSE.invokeExact(ptr);
	}
}
