package io.github.dfa1.rocksdbffm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BackupEngineIntegrationTest {

	@Test
	void createAndRestoreLatestBackup(@TempDir Path dir) {
		var dbDir = dir.resolve("db");
		var backupDir = dir.resolve("backup");
		var restoreDir = dir.resolve("restore");

		// Given — open DB, write data, create backup
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = RocksDB.open(opts, dbDir);
		     var engine = BackupEngine.open(opts, backupDir)) {

			db.put("k1".getBytes(), "v1".getBytes());
			db.put("k2".getBytes(), "v2".getBytes());
			engine.createNewBackup(db);

			List<BackupInfo> infos = engine.getBackupInfo();
			assertThat(infos).hasSize(1);
			assertThat(infos.getFirst().backupId()).isEqualTo(BackupId.of(1));
			assertThat(infos.getFirst().numberOfFiles()).isGreaterThan(0);
			assertThat(infos.getFirst().timestamp()).isGreaterThan(0);
			assertThat(infos.getFirst().size().toBytes()).isGreaterThan(0);
		}

		// When — restore from latest backup
		try (var env = Env.defaultEnv();
		     var beOpts = BackupEngineOptions.create(backupDir);
		     var restOpts = RestoreOptions.create();
		     var engine = BackupEngine.open(beOpts, env)) {

			engine.restoreDbFromLatestBackup(restoreDir, restOpts);
		}

		// Then — restored DB contains original data
		try (var opts = Options.newOptions();
		     var db = RocksDB.open(opts, restoreDir)) {

			assertThat(db.get("k1".getBytes())).isEqualTo("v1".getBytes());
			assertThat(db.get("k2".getBytes())).isEqualTo("v2".getBytes());
		}
	}

	@Test
	void multipleBackupsAndRestoreByBackupId(@TempDir Path dir) {
		var dbDir = dir.resolve("db");
		var backupDir = dir.resolve("backup");
		var restoreV1 = dir.resolve("restore-v1");
		var restoreV2 = dir.resolve("restore-v2");

		// Given — two backups at different states
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = RocksDB.open(opts, dbDir);
		     var engine = BackupEngine.open(opts, backupDir)) {

			db.put("k".getBytes(), "v1".getBytes());
			engine.createNewBackup(db, true);

			db.put("k".getBytes(), "v2".getBytes());
			engine.createNewBackup(db, true);

			assertThat(engine.getBackupInfo()).hasSize(2);

			// When — restore each backup by ID
			try (var restOpts = RestoreOptions.create()) {
				engine.restoreDbFromBackup(BackupId.of(1), restoreV1, restOpts);
				engine.restoreDbFromBackup(BackupId.of(2), restoreV2, restOpts);
			}
		}

		// Then — each restored directory reflects the state at backup time
		try (var opts = Options.newOptions();
		     var db1 = RocksDB.open(opts, restoreV1);
		     var db2 = RocksDB.open(opts, restoreV2)) {

			assertThat(db1.get("k".getBytes())).isEqualTo("v1".getBytes());
			assertThat(db2.get("k".getBytes())).isEqualTo("v2".getBytes());
		}
	}

	@Test
	void purgeOldBackups(@TempDir Path dir) {
		var dbDir = dir.resolve("db");
		var backupDir = dir.resolve("backup");

		// Given — three backups
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = RocksDB.open(opts, dbDir);
		     var engine = BackupEngine.open(opts, backupDir)) {

			engine.createNewBackup(db);
			engine.createNewBackup(db);
			engine.createNewBackup(db);
			assertThat(engine.getBackupInfo()).hasSize(3);

			// When — keep only the 2 most recent
			engine.purgeOldBackups(2);

			// Then
			List<BackupInfo> infos = engine.getBackupInfo();
			assertThat(infos).hasSize(2);
			assertThat(infos.getFirst().backupId()).isEqualTo(BackupId.of(2));
			assertThat(infos.getLast().backupId()).isEqualTo(BackupId.of(3));
		}
	}

	@Test
	void verifyBackup(@TempDir Path dir) {
		var dbDir = dir.resolve("db");
		var backupDir = dir.resolve("backup");

		// Given
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = RocksDB.open(opts, dbDir);
		     var engine = BackupEngine.open(opts, backupDir)) {

			db.put("k".getBytes(), "v".getBytes());
			engine.createNewBackup(db);

			// When / Then — verify should succeed for a valid backup
			engine.verifyBackup(BackupId.of(1));
		}
	}

	@Test
	void verifyNonExistentBackupThrows(@TempDir Path dir) {
		var dbDir = dir.resolve("db");
		var backupDir = dir.resolve("backup");

		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = RocksDB.open(opts, dbDir);
		     var engine = BackupEngine.open(opts, backupDir)) {

			engine.createNewBackup(db);

			// When / Then — verifying a non-existent ID throws
			assertThatThrownBy(() -> engine.verifyBackup(BackupId.of(999)))
					.isInstanceOf(RocksDBException.class);
		}
	}

	@Test
	void backupEngineOptions_fluent(@TempDir Path dir) {
		var backupDir = dir.resolve("backup");

		// Given / When
		try (var opts = BackupEngineOptions.create(backupDir)
				.setShareTableFiles(true)
				.setSync(false)
				.setDestroyOldData(false)
				.setBackupLogFiles(true)
				.setBackupRateLimit(MemorySize.ofMB(100))
				.setRestoreRateLimit(MemorySize.ofMB(200))
				.setMaxBackgroundOperations(2)
				.setCallbackTriggerIntervalSize(MemorySize.ofMB(8))
				.setMaxValidBackupsToOpen(-1)) {

			// Then — all options round-trip correctly
			assertThat(opts.isShareTableFiles()).isTrue();
			assertThat(opts.isSync()).isFalse();
			assertThat(opts.isDestroyOldData()).isFalse();
			assertThat(opts.isBackupLogFiles()).isTrue();
			assertThat(opts.getBackupRateLimit()).isEqualTo(MemorySize.ofMB(100));
			assertThat(opts.getRestoreRateLimit()).isEqualTo(MemorySize.ofMB(200));
			assertThat(opts.getMaxBackgroundOperations()).isEqualTo(2);
			assertThat(opts.getCallbackTriggerIntervalSize()).isEqualTo(MemorySize.ofMB(8));
			assertThat(opts.getMaxValidBackupsToOpen()).isEqualTo(-1);
		}
	}

	@Test
	void openWithBackupEngineOptions(@TempDir Path dir) {
		var dbDir = dir.resolve("db");
		var backupDir = dir.resolve("backup");
		var restoreDir = dir.resolve("restore");

		// Given — write data
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = RocksDB.open(opts, dbDir)) {
			db.put("key".getBytes(), "value".getBytes());

			// When — backup via explicit BackupEngineOptions + Env
			try (var env = Env.defaultEnv();
			     var beOpts = BackupEngineOptions.create(backupDir)
					     .setShareTableFiles(true)
					     .setMaxBackgroundOperations(1);
			     var engine = BackupEngine.open(beOpts, env)) {

				engine.createNewBackup(db);
				assertThat(engine.getBackupInfo()).hasSize(1);

				// When — restore
				try (var restOpts = RestoreOptions.create()) {
					engine.restoreDbFromLatestBackup(restoreDir, restOpts);
				}
			}
		}

		// Then
		try (var opts = Options.newOptions();
		     var db = RocksDB.open(opts, restoreDir)) {
			assertThat(db.get("key".getBytes())).isEqualTo("value".getBytes());
		}
	}

	@Test
	void restoreOptions_keepLogFiles(@TempDir Path dir) {
		// Given / When / Then — RestoreOptions can be created and configured
		try (var restOpts = RestoreOptions.create().setKeepLogFiles(true)) {
			assertThat(restOpts).isNotNull();
		}
	}
}
