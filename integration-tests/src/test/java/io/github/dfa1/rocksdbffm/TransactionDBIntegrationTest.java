package io.github.dfa1.rocksdbffm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class TransactionDBIntegrationTest {

	private static TransactionDB openDb(Path path) {
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var txnDbOpts = TransactionDBOptions.newTransactionDBOptions()) {
			return RocksDB.openTransaction(opts, txnDbOpts, path);
		}
	}

	@Test
	void commit_makesChangesVisible(@TempDir Path dir) {
		// Given
		try (var db = openDb(dir);
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
		try (var db = openDb(dir);
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
	void uncommittedWrite_notVisibleOutsideTransaction(@TempDir Path dir) {
		// Given
		try (var db = openDb(dir);
		     var wo = WriteOptions.newWriteOptions();
		     var txn = db.beginTransaction(wo)) {

			// When — write inside transaction but do not commit yet
			txn.put("k".getBytes(), "v".getBytes());

			// Then — direct read on the db does not see the uncommitted write
			assertThat(db.get("k".getBytes())).isNull();

			txn.rollback();
		}
	}
}
