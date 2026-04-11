package io.github.dfa1.rocksdbffm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OptimisticTransactionDBIntegrationTest {

	@Test
	void commit_makesChangesVisible(@TempDir Path dir) {
		// Given
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = RocksDB.openOptimistic(opts, dir);
		     var wo = WriteOptions.newWriteOptions();
		     var txn = db.beginTransaction(wo)) {

			txn.put("k".getBytes(), "v".getBytes());

			// When
			txn.commit();

			// Then
			assertThat(db.get("k".getBytes())).isEqualTo("v".getBytes());
		}
	}

	@Test
	void rollback_discardsChanges(@TempDir Path dir) {
		// Given
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = RocksDB.openOptimistic(opts, dir);
		     var wo = WriteOptions.newWriteOptions();
		     var txn = db.beginTransaction(wo)) {

			txn.put("k".getBytes(), "v".getBytes());

			// When
			txn.rollback();

			// Then
			assertThat(db.get("k".getBytes())).isNull();
		}
	}

	@Test
	void conflictingCommit_throwsOnSecondTransaction(@TempDir Path dir) {
		// Given — two transactions read and write the same key
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = RocksDB.openOptimistic(opts, dir);
		     var wo = WriteOptions.newWriteOptions();
		     var txn1 = db.beginTransaction(wo);
		     var txn2 = db.beginTransaction(wo)) {

			// Both transactions read k (establishing read set for conflict detection)
			txn1.get(ReadOptions.newReadOptions(), "k".getBytes());
			txn2.get(ReadOptions.newReadOptions(), "k".getBytes());

			txn1.put("k".getBytes(), "v1".getBytes());
			txn2.put("k".getBytes(), "v2".getBytes());

			// When — first commit wins
			txn1.commit();

			// Then — second commit detects the conflict
			assertThatThrownBy(txn2::commit)
					.isInstanceOf(RocksDBException.class);
		}
	}
}
