package io.github.dfa1.rocksdbffm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OptimisticTransactionDBTest {

	// -----------------------------------------------------------------------
	// Basic open / close
	// -----------------------------------------------------------------------

	@Test
	void open_createsDb(@TempDir Path dir) {
		// Given / When / Then — no exception means success
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = OptimisticTransactionDB.open(opts, dir)) {
			assertThat(db).isNotNull();
		}
	}

	// -----------------------------------------------------------------------
	// Direct (non-transactional) operations
	// -----------------------------------------------------------------------

	@Test
	void put_and_get_roundtrip(@TempDir Path dir) {
		// Given
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = OptimisticTransactionDB.open(opts, dir)) {

			// When
			db.put("key".getBytes(), "value".getBytes());

			// Then
			assertThat(db.get("key".getBytes())).isEqualTo("value".getBytes());
		}
	}

	@Test
	void delete_removesKey(@TempDir Path dir) {
		// Given
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = OptimisticTransactionDB.open(opts, dir)) {
			db.put("k".getBytes(), "v".getBytes());

			// When
			db.delete("k".getBytes());

			// Then
			assertThat(db.get("k".getBytes())).isNull();
		}
	}

	@Test
	void get_returnsNull_whenKeyMissing(@TempDir Path dir) {
		// Given
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = OptimisticTransactionDB.open(opts, dir)) {

			// When / Then
			assertThat(db.get("missing".getBytes())).isNull();
		}
	}

	// -----------------------------------------------------------------------
	// Transaction commit
	// -----------------------------------------------------------------------

	@Test
	void transaction_commit_persists(@TempDir Path dir) {
		// Given
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = OptimisticTransactionDB.open(opts, dir);
		     var wo = WriteOptions.newWriteOptions()) {

			// When
			try (var txn = db.beginTransaction(wo)) {
				txn.put("k".getBytes(), "v".getBytes());
				txn.commit();
			}

			// Then
			assertThat(db.get("k".getBytes())).isEqualTo("v".getBytes());
		}
	}

	@Test
	void transaction_rollback_discardsChanges(@TempDir Path dir) {
		// Given
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = OptimisticTransactionDB.open(opts, dir);
		     var wo = WriteOptions.newWriteOptions()) {

			// When
			try (var txn = db.beginTransaction(wo)) {
				txn.put("k".getBytes(), "v".getBytes());
				txn.rollback();
			}

			// Then
			assertThat(db.get("k".getBytes())).isNull();
		}
	}

	@Test
	void transaction_get_seesUncommittedWrites(@TempDir Path dir) {
		// Given
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = OptimisticTransactionDB.open(opts, dir);
		     var wo = WriteOptions.newWriteOptions()) {

			// When — transaction can read its own uncommitted writes
			try (var txn = db.beginTransaction(wo)) {
				txn.put("k".getBytes(), "v".getBytes());
				assertThat(txn.get(ReadOptions.newReadOptions(), "k".getBytes())).isEqualTo("v".getBytes());
				txn.rollback();
			}
		}
	}

	// -----------------------------------------------------------------------
	// Conflict detection
	// -----------------------------------------------------------------------

	@Test
	void transaction_conflict_throwsOnCommit(@TempDir Path dir) {
		// Given — two transactions read the same key; the first committer wins
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = OptimisticTransactionDB.open(opts, dir);
		     var wo = WriteOptions.newWriteOptions()) {

			db.put("k".getBytes(), "original".getBytes());

			try (var txn1 = db.beginTransaction(wo);
			     var txn2 = db.beginTransaction(wo)) {

				// Both read the key
				txn1.getForUpdate(ReadOptions.newReadOptions(), "k".getBytes(), true);
				txn2.getForUpdate(ReadOptions.newReadOptions(), "k".getBytes(), true);

				// Both write to it
				txn1.put("k".getBytes(), "txn1".getBytes());
				txn2.put("k".getBytes(), "txn2".getBytes());

				// txn1 commits first — succeeds
				txn1.commit();

				// txn2 commits second — conflict: txn1 already modified "k"
				assertThatThrownBy(txn2::commit)
						.isInstanceOf(RocksDBException.class);

				txn2.rollback();
			}

			// Then — txn1's write wins
			assertThat(db.get("k".getBytes())).isEqualTo("txn1".getBytes());
		}
	}

	// -----------------------------------------------------------------------
	// OptimisticTransactionOptions
	// -----------------------------------------------------------------------

	@Test
	void beginTransaction_withOptions_setSnapshot(@TempDir Path dir) {
		// Given
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = OptimisticTransactionDB.open(opts, dir);
		     var wo = WriteOptions.newWriteOptions();
		     var txnOpts = new OptimisticTransactionOptions().setSetSnapshot(true)) {

			db.put("before".getBytes(), "yes".getBytes());

			// When — transaction with snapshot sees only pre-snapshot data
			try (var txn = db.beginTransaction(wo, txnOpts)) {
				db.put("after".getBytes(), "no".getBytes());

				// getForUpdate through the transaction sees committed data
				assertThat(txn.get(ReadOptions.newReadOptions(), "before".getBytes())).isEqualTo("yes".getBytes());

				txn.commit();
			}
		}
	}

	// -----------------------------------------------------------------------
	// Snapshot
	// -----------------------------------------------------------------------

	@Test
	void getSnapshot_isolatesReads(@TempDir Path dir) {
		// Given
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = OptimisticTransactionDB.open(opts, dir)) {

			db.put("k".getBytes(), "v1".getBytes());

			// When — take snapshot, then overwrite
			try (var snap = db.getSnapshot();
			     var ro = ReadOptions.newReadOptions().setSnapshot(snap)) {

				db.put("k".getBytes(), "v2".getBytes());

				// Then — snapshot still sees v1
				assertThat(db.get(ro, "k".getBytes())).isEqualTo("v1".getBytes());
			}

			// And current read sees v2
			assertThat(db.get("k".getBytes())).isEqualTo("v2".getBytes());
		}
	}

	// -----------------------------------------------------------------------
	// Flush
	// -----------------------------------------------------------------------

	@Test
	void flush_doesNotThrow(@TempDir Path dir) {
		// Given
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = OptimisticTransactionDB.open(opts, dir);
		     var fo = FlushOptions.newFlushOptions()) {

			db.put("k".getBytes(), "v".getBytes());

			// When / Then — no exception
			db.flush(fo);
		}
	}

	// -----------------------------------------------------------------------
	// DB Properties
	// -----------------------------------------------------------------------

	@Test
	void getLongProperty_returnsValue(@TempDir Path dir) {
		// Given
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = OptimisticTransactionDB.open(opts, dir)) {

			db.put("k".getBytes(), "v".getBytes());

			// When / Then
			assertThat(db.getLongProperty(Property.ESTIMATE_NUM_KEYS)).isPresent();
		}
	}
}
