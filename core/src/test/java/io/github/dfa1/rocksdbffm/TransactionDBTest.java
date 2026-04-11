package io.github.dfa1.rocksdbffm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.foreign.Arena;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class TransactionDBTest {

	private static TransactionDB openDb(Path path) {
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var txnDbOpts = TransactionDBOptions.newTransactionDBOptions()) {
			return RocksDB.openTransaction(opts, txnDbOpts, path);
		}
	}

	// -----------------------------------------------------------------------
	// commit / rollback
	// -----------------------------------------------------------------------

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

	// -----------------------------------------------------------------------
	// get within a transaction
	// -----------------------------------------------------------------------

	@Test
	void get_readsUncommittedWritesWithinSameTransaction(@TempDir Path dir) {
		// Given
		try (var db = openDb(dir);
		     var wo = WriteOptions.newWriteOptions();
		     var ro = ReadOptions.newReadOptions();
		     var txn = db.beginTransaction(wo)) {

			txn.put("k".getBytes(), "v".getBytes());

			// When
			var result = txn.get(ro, "k".getBytes());

			// Then
			assertThat(result).isEqualTo("v".getBytes());
			txn.commit();
		}
	}

	@Test
	void get_returnsNull_forAbsentKey(@TempDir Path dir) {
		// Given
		try (var db = openDb(dir);
		     var wo = WriteOptions.newWriteOptions();
		     var ro = ReadOptions.newReadOptions();
		     var txn = db.beginTransaction(wo)) {

			// When
			var result = txn.get(ro, "missing".getBytes());

			// Then
			assertThat(result).isNull();
			txn.rollback();
		}
	}

	@Test
	void getForUpdate_locksAndReturnsValue(@TempDir Path dir) {
		// Given
		seed(dir, "k", "original");

		try (var db = openDb(dir);
		     var wo = WriteOptions.newWriteOptions();
		     var ro = ReadOptions.newReadOptions();
		     var txn = db.beginTransaction(wo)) {

			// When
			var val = txn.getForUpdate(ro, "k".getBytes(), true);

			// Then
			assertThat(val).isEqualTo("original".getBytes());

			txn.put("k".getBytes(), "updated".getBytes());
			txn.commit();
		}

		try (var db = openDb(dir)) {
			assertThat(db.get("k".getBytes())).isEqualTo("updated".getBytes());
		}
	}

	// -----------------------------------------------------------------------
	// delete within a transaction
	// -----------------------------------------------------------------------

	@Test
	void delete_removesKeyWithinTransaction(@TempDir Path dir) {
		// Given
		seed(dir, "k", "v");

		try (var db = openDb(dir);
		     var wo = WriteOptions.newWriteOptions();
		     var ro = ReadOptions.newReadOptions();
		     var txn = db.beginTransaction(wo)) {

			// When
			txn.delete("k".getBytes());

			// Then — invisible within the transaction immediately
			assertThat(txn.get(ro, "k".getBytes())).isNull();
			txn.commit();
		}

		try (var db = openDb(dir)) {
			assertThat(db.get("k".getBytes())).isNull();
		}
	}

	// -----------------------------------------------------------------------
	// savepoints
	// -----------------------------------------------------------------------

	@Test
	void rollbackToSavePoint_restoresPartialState(@TempDir Path dir) {
		// Given
		try (var db = openDb(dir);
		     var wo = WriteOptions.newWriteOptions();
		     var ro = ReadOptions.newReadOptions();
		     var txn = db.beginTransaction(wo)) {

			txn.put("k1".getBytes(), "v1".getBytes());
			txn.setSavePoint();
			txn.put("k2".getBytes(), "v2".getBytes());

			// When
			txn.rollbackToSavePoint();

			// Then — k1 still staged, k2 discarded
			assertThat(txn.get(ro, "k1".getBytes())).isEqualTo("v1".getBytes());
			assertThat(txn.get(ro, "k2".getBytes())).isNull();

			txn.commit();
		}

		try (var db = openDb(dir)) {
			assertThat(db.get("k1".getBytes())).isEqualTo("v1".getBytes());
			assertThat(db.get("k2".getBytes())).isNull();
		}
	}

	// -----------------------------------------------------------------------
	// direct (non-transactional) operations
	// -----------------------------------------------------------------------

	@Test
	void directPut_isVisibleViaDirectGet(@TempDir Path dir) {
		// Given
		try (var db = openDb(dir)) {
			// When
			db.put("k".getBytes(), "v".getBytes());

			// Then
			assertThat(db.get("k".getBytes())).isEqualTo("v".getBytes());
		}
	}

	@Test
	void directDelete_removesKey(@TempDir Path dir) {
		// Given
		try (var db = openDb(dir)) {
			db.put("k".getBytes(), "v".getBytes());

			// When
			db.delete("k".getBytes());

			// Then
			assertThat(db.get("k".getBytes())).isNull();
		}
	}

	// -----------------------------------------------------------------------
	// direct operations — ByteBuffer tier
	// -----------------------------------------------------------------------

	@Test
	void directPut_byteBuffer_isVisibleViaGet(@TempDir Path dir) {
		// Given
		try (var db = openDb(dir)) {
			var key = ByteBuffer.allocateDirect(3);
			key.put("key".getBytes()).flip();
			var value = ByteBuffer.allocateDirect(5);
			value.put("value".getBytes()).flip();

			// When
			db.put(key, value);

			// Then
			assertThat(db.get("key".getBytes())).isEqualTo("value".getBytes());
		}
	}

	@Test
	void directGet_byteBuffer_returnsValue(@TempDir Path dir) {
		// Given
		try (var db = openDb(dir)) {
			db.put("k".getBytes(), "v".getBytes());

			var key = ByteBuffer.allocateDirect(1);
			key.put("k".getBytes()).flip();
			var out = ByteBuffer.allocateDirect(32);

			// When
			int len = db.get(key, out);

			// Then
			assertThat(len).isEqualTo(1);
		}
	}

	@Test
	void directDelete_byteBuffer_removesKey(@TempDir Path dir) {
		// Given
		try (var db = openDb(dir)) {
			db.put("k".getBytes(), "v".getBytes());

			var key = ByteBuffer.allocateDirect(1);
			key.put("k".getBytes()).flip();

			// When
			db.delete(key);

			// Then
			assertThat(db.get("k".getBytes())).isNull();
		}
	}

	// -----------------------------------------------------------------------
	// direct operations — MemorySegment tier
	// -----------------------------------------------------------------------

	@Test
	void directPut_memorySegment_isVisibleViaGet(@TempDir Path dir) {
		// Given
		try (var db = openDb(dir);
		     Arena arena = Arena.ofConfined()) {
			var key = arena.allocateFrom("seg-k");
			var value = arena.allocateFrom("seg-v");

			// When
			db.put(key.asSlice(0, 5), value.asSlice(0, 5));

			// Then
			assertThat(db.get("seg-k".getBytes())).isEqualTo("seg-v".getBytes());
		}
	}

	@Test
	void directGet_memorySegment_returnsValue(@TempDir Path dir) {
		// Given
		try (var db = openDb(dir);
		     Arena arena = Arena.ofConfined()) {
			db.put("k".getBytes(), "v".getBytes());

			var key = arena.allocateFrom("k");
			var out = arena.allocate(32);

			// When
			long len = db.get(key.asSlice(0, 1), out);

			// Then
			assertThat(len).isEqualTo(1);
			assertThat(out.asSlice(0, 1).toArray(ValueLayout.JAVA_BYTE)).isEqualTo("v".getBytes());
		}
	}

	@Test
	void directDelete_memorySegment_removesKey(@TempDir Path dir) {
		// Given
		try (var db = openDb(dir);
		     Arena arena = Arena.ofConfined()) {
			db.put("k".getBytes(), "v".getBytes());

			var key = arena.allocateFrom("k");

			// When
			db.delete(key.asSlice(0, 1));

			// Then
			assertThat(db.get("k".getBytes())).isNull();
		}
	}

	// -----------------------------------------------------------------------
	// TransactionOptions
	// -----------------------------------------------------------------------

	@Test
	void transactionOptions_setSnapshot_doesNotBreakNormalFlow(@TempDir Path dir) {
		// Given
		try (var db = openDb(dir);
		     var wo = WriteOptions.newWriteOptions();
		     var txnOpts = TransactionOptions.newTransactionOptions().setSetSnapshot(true);
		     var txn = db.beginTransaction(wo, txnOpts)) {

			txn.put("k".getBytes(), "v".getBytes());

			// When
			txn.commit();

			// Then
			assertThat(db.get("k".getBytes())).isEqualTo("v".getBytes());
		}
	}

	// -----------------------------------------------------------------------
	// Helpers
	// -----------------------------------------------------------------------

	private void seed(Path dir, String key, String value) {
		try (var db = openDb(dir);
		     var wo = WriteOptions.newWriteOptions();
		     var txn = db.beginTransaction(wo)) {
			txn.put(key.getBytes(), value.getBytes());
			txn.commit();
		}
	}
}
