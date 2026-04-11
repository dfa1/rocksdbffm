package io.github.dfa1.rocksdbffm;

/// Immutable snapshot of a single RocksDB backup entry.
///
/// Instances are obtained from [BackupEngine#getBackupInfo()].
/// There is no native resource associated with this record — the native
/// `rocksdb_backup_engine_info_t*` is acquired, iterated, and destroyed
/// entirely inside [BackupEngine#getBackupInfo()].
///
/// ```
/// List<BackupInfo> infos = engine.getBackupInfo();
/// BackupInfo latest = infos.getLast();
/// System.out.printf("backup %s — %d bytes, %d files%n",
///     latest.backupId(), latest.size().toBytes(), latest.numberOfFiles());
/// ```
public record BackupInfo(
		/// Unique identifier for this backup, assigned by the engine.
		BackupId backupId,
		/// Unix timestamp (seconds since epoch) when the backup was created.
		long timestamp,
		/// Total size of all files belonging to this backup.
		MemorySize size,
		/// Number of SST/WAL/MANIFEST files in this backup.
		long numberOfFiles
) {
}
