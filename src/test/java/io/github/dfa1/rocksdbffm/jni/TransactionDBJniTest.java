package io.github.dfa1.rocksdbffm.jni;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.*;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class TransactionDBJniTest {

    static {
        RocksDB.loadLibrary();
    }

    private static TransactionDB openDb(Path path) throws RocksDBException {
        try (var opts = new Options().setCreateIfMissing(true);
             var txnDbOpts = new TransactionDBOptions()) {
            return TransactionDB.open(opts, txnDbOpts, path.toString());
        }
    }

    @Test
    void commit_makesChangesVisible(@TempDir Path dir) throws RocksDBException {
        // Given
        try (var db = openDb(dir);
             var wo = new WriteOptions();
             var txn = db.beginTransaction(wo)) {

            txn.put("k".getBytes(), "v".getBytes());

            // When
            txn.commit();

            // Then
            assertThat(db.get("k".getBytes())).isEqualTo("v".getBytes());
        }
    }

    @Test
    void rollback_discardsChanges(@TempDir Path dir) throws RocksDBException {
        // Given
        try (var db = openDb(dir);
             var wo = new WriteOptions();
             var txn = db.beginTransaction(wo)) {

            txn.put("k".getBytes(), "v".getBytes());

            // When
            txn.rollback();

            // Then
            assertThat(db.get("k".getBytes())).isNull();
        }
    }

    @Test
    void get_readsUncommittedWritesWithinSameTransaction(@TempDir Path dir) throws RocksDBException {
        // Given
        try (var db = openDb(dir);
             var wo = new WriteOptions();
             var ro = new ReadOptions();
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
    void get_returnsNull_forAbsentKey(@TempDir Path dir) throws RocksDBException {
        // Given
        try (var db = openDb(dir);
             var wo = new WriteOptions();
             var ro = new ReadOptions();
             var txn = db.beginTransaction(wo)) {

            // When
            var result = txn.get(ro, "missing".getBytes());

            // Then
            assertThat(result).isNull();
            txn.rollback();
        }
    }

    @Test
    void getForUpdate_locksAndReturnsValue(@TempDir Path dir) throws RocksDBException {
        // Given
        seed(dir, "k", "original");

        try (var db = openDb(dir);
             var wo = new WriteOptions();
             var ro = new ReadOptions();
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

    @Test
    void delete_removesKeyWithinTransaction(@TempDir Path dir) throws RocksDBException {
        // Given
        seed(dir, "k", "v");

        try (var db = openDb(dir);
             var wo = new WriteOptions();
             var ro = new ReadOptions();
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

    @Test
    void rollbackToSavePoint_restoresPartialState(@TempDir Path dir) throws RocksDBException {
        // Given
        try (var db = openDb(dir);
             var wo = new WriteOptions();
             var ro = new ReadOptions();
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
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private void seed(Path dir, String key, String value) throws RocksDBException {
        try (var db = openDb(dir);
             var wo = new WriteOptions();
             var txn = db.beginTransaction(wo)) {
            txn.put(key.getBytes(), value.getBytes());
            txn.commit();
        }
    }
}
