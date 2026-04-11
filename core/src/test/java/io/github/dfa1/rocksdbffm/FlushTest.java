package io.github.dfa1.rocksdbffm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class FlushTest {

	// -----------------------------------------------------------------------
	// FlushOptions
	// -----------------------------------------------------------------------

	@Test
	void flushOptions_defaultWaitIsTrue() {
		// Given / When
		try (FlushOptions fo = FlushOptions.newFlushOptions()) {
			// Then
			assertThat(fo.isWait()).isTrue();
		}
	}

	@Test
	void flushOptions_setWait_roundTrips() {
		// Given
		try (FlushOptions fo = FlushOptions.newFlushOptions()) {
			// When
			fo.setWait(false);
			// Then
			assertThat(fo.isWait()).isFalse();

			fo.setWait(true);
			assertThat(fo.isWait()).isTrue();
		}
	}

	// -----------------------------------------------------------------------
	// RocksDB flush
	// -----------------------------------------------------------------------

	@Test
	void flush_dataIsReadableAfterFlush(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir);
		     var fo = FlushOptions.newFlushOptions().setWait(true)) {
			db.put("k".getBytes(), "v".getBytes());

			// When
			db.flush(fo);

			// Then — data survives and is readable
			assertThat(db.get("k".getBytes())).isEqualTo("v".getBytes());
		}
	}

	@Test
	void flush_multipleKeys(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir);
		     var fo = FlushOptions.newFlushOptions()) {
			for (int i = 0; i < 100; i++) {
				db.put(("key" + i).getBytes(), ("val" + i).getBytes());
			}

			// When
			db.flush(fo);

			// Then
			assertThat(db.get("key0".getBytes())).isEqualTo("val0".getBytes());
			assertThat(db.get("key99".getBytes())).isEqualTo("val99".getBytes());
		}
	}

	@Test
	void flushWal_doesNotThrow(@TempDir Path dir) {
		// Given
		try (var db = RocksDB.open(dir)) {
			db.put("k".getBytes(), "v".getBytes());

			// When / Then — both sync modes complete without error
			db.flushWal(false);
			db.flushWal(true);
		}
	}

	// -----------------------------------------------------------------------
	// TransactionDB flush
	// -----------------------------------------------------------------------

	@Test
	void transactionDB_flush_dataIsReadableAfterFlush(@TempDir Path dir) {
		// Given
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var txnDbOpts = TransactionDBOptions.newTransactionDBOptions();
		     var db = RocksDB.openTransaction(opts, txnDbOpts, dir);
		     var fo = FlushOptions.newFlushOptions().setWait(true)) {
			db.put("k".getBytes(), "v".getBytes());

			// When
			db.flush(fo);

			// Then
			assertThat(db.get("k".getBytes())).isEqualTo("v".getBytes());
		}
	}

	@Test
	void transactionDB_flushWal_doesNotThrow(@TempDir Path dir) {
		// Given
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var txnDbOpts = TransactionDBOptions.newTransactionDBOptions();
		     var db = RocksDB.openTransaction(opts, txnDbOpts, dir)) {
			db.put("k".getBytes(), "v".getBytes());

			// When / Then
			db.flushWal(false);
			db.flushWal(true);
		}
	}
}
