package io.github.dfa1.rocksdbffm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class SstFileManagerIntegrationTest {

	@Test
	void openDbWithSstFileManager(@TempDir Path dir) {
		// Given
		try (var env = Env.defaultEnv();
		     var sfm = SstFileManager.create(env);
		     var opts = Options.newOptions()
				     .setCreateIfMissing(true)
				     .setSstFileManager(sfm);
		     var db = RocksDB.open(opts, dir)) {

			// When
			db.put("key".getBytes(), "value".getBytes());

			// Then
			assertThat(db.get("key".getBytes())).isEqualTo("value".getBytes());
			assertThat(sfm.getTotalSize().toBytes()).isGreaterThanOrEqualTo(0);
		}
	}

	@Test
	void maxAllowedSpaceUsage(@TempDir Path dir) {
		// Given
		try (var env = Env.defaultEnv();
		     var sfm = SstFileManager.create(env)
				     .setMaxAllowedSpaceUsage(MemorySize.ofGB(10))
				     .setCompactionBufferSize(MemorySize.ofMB(512));
		     var opts = Options.newOptions()
				     .setCreateIfMissing(true)
				     .setSstFileManager(sfm);
		     var db = RocksDB.open(opts, dir)) {

			// When
			db.put("k".getBytes(), "v".getBytes());

			// Then — space limit is generous so it should never be reached here
			assertThat(sfm.isMaxAllowedSpaceReached()).isFalse();
			assertThat(sfm.isMaxAllowedSpaceReachedIncludingCompactions()).isFalse();
		}
	}

	@Test
	void deleteRateAndTrashRatio(@TempDir Path dir) {
		// Given
		try (var env = Env.defaultEnv();
		     var sfm = SstFileManager.create(env)
				     .setDeleteRateBytesPerSecond(MemorySize.ofMB(64).toBytes())
				     .setMaxTrashDbRatio(0.5);
		     var opts = Options.newOptions()
				     .setCreateIfMissing(true)
				     .setSstFileManager(sfm);
		     var db = RocksDB.open(opts, dir)) {

			// When
			db.put("key".getBytes(), "value".getBytes());

			// Then
			assertThat(sfm.getDeleteRateBytesPerSecond()).isEqualTo(MemorySize.ofMB(64).toBytes());
			assertThat(sfm.getMaxTrashDbRatio()).isEqualTo(0.5);
			assertThat(sfm.getTotalTrashSize().toBytes()).isGreaterThanOrEqualTo(0);
		}
	}

	@Test
	void envBackgroundThreads() {
		// Given
		try (var env = Env.defaultEnv()) {

			// When
			env.setBackgroundThreads(2);
			env.setHighPriorityBackgroundThreads(1);

			// Then
			assertThat(env.getBackgroundThreads()).isEqualTo(2);
			assertThat(env.getHighPriorityBackgroundThreads()).isEqualTo(1);
		}
	}

	@Test
	void sfmCanBeClosedBeforeOptions(@TempDir Path dir) {
		// Given — sfm uses shared ownership, safe to close before Options
		try (var env = Env.defaultEnv();
		     var sfm = SstFileManager.create(env);
		     var opts = Options.newOptions()
				     .setCreateIfMissing(true)
				     .setSstFileManager(sfm)) {
			sfm.close();

			// When / Then — Options (and the underlying shared_ptr) still valid
			try (var db = RocksDB.open(opts, dir)) {
				db.put("key".getBytes(), "value".getBytes());
				assertThat(db.get("key".getBytes())).isEqualTo("value".getBytes());
			}
		}
	}
}
