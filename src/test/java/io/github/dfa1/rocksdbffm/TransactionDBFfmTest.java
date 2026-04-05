package io.github.dfa1.rocksdbffm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class TransactionDBFfmTest {

    private static TransactionDB openDb(Path path) {
        try (Options opts = new Options().setCreateIfMissing(true);
             TransactionDBOptions txnDbOpts = new TransactionDBOptions()) {
            return TransactionDB.open(opts, txnDbOpts, path);
        }
    }

    // -----------------------------------------------------------------------
    // Basic transaction: commit
    // -----------------------------------------------------------------------

    @Test
    void commitMakesChangesVisible(@TempDir Path tempDir) {
        try (TransactionDB db = openDb(tempDir);
             WriteOptions wo = new WriteOptions();
             Transaction txn = db.beginTransaction(wo)) {

            txn.put("k".getBytes(), "v".getBytes());
            txn.commit();

            assertArrayEquals("v".getBytes(), db.get("k".getBytes()));
        }
    }

    @Test
    void rollbackDiscardsChanges(@TempDir Path tempDir) {
        try (TransactionDB db = openDb(tempDir);
             WriteOptions wo = new WriteOptions();
             Transaction txn = db.beginTransaction(wo)) {

            txn.put("k".getBytes(), "v".getBytes());
            txn.rollback();

            assertNull(db.get("k".getBytes()));
        }
    }

    // -----------------------------------------------------------------------
    // Transaction reads: get, getForUpdate
    // -----------------------------------------------------------------------

    @Test
    void getReadsUncommittedWritesInSameTransaction(@TempDir Path tempDir) {
        try (TransactionDB db = openDb(tempDir);
             WriteOptions wo = new WriteOptions();
             ReadOptions ro = new ReadOptions();
             Transaction txn = db.beginTransaction(wo)) {

            txn.put("k".getBytes(), "v".getBytes());
            assertArrayEquals("v".getBytes(), txn.get(ro, "k".getBytes()));
            txn.commit();
        }
    }

    @Test
    void getMissingKeyReturnsNull(@TempDir Path tempDir) {
        try (TransactionDB db = openDb(tempDir);
             WriteOptions wo = new WriteOptions();
             ReadOptions ro = new ReadOptions();
             Transaction txn = db.beginTransaction(wo)) {

            assertNull(txn.get(ro, "missing".getBytes()));
            txn.rollback();
        }
    }

    @Test
    void getForUpdateLocksKey(@TempDir Path tempDir) {
        db_put(tempDir, "k", "original");

        try (TransactionDB db = openDb(tempDir);
             WriteOptions wo = new WriteOptions();
             ReadOptions ro = new ReadOptions();
             Transaction txn = db.beginTransaction(wo)) {

            byte[] val = txn.getForUpdate(ro, "k".getBytes(), true);
            assertArrayEquals("original".getBytes(), val);

            txn.put("k".getBytes(), "updated".getBytes());
            txn.commit();
        }

        try (TransactionDB db = openDb(tempDir)) {
            assertArrayEquals("updated".getBytes(), db.get("k".getBytes()));
        }
    }

    // -----------------------------------------------------------------------
    // Delete inside transaction
    // -----------------------------------------------------------------------

    @Test
    void deleteInsideTransaction(@TempDir Path tempDir) {
        db_put(tempDir, "k", "v");

        try (TransactionDB db = openDb(tempDir);
             WriteOptions wo = new WriteOptions();
             ReadOptions ro = new ReadOptions();
             Transaction txn = db.beginTransaction(wo)) {

            txn.delete("k".getBytes());
            assertNull(txn.get(ro, "k".getBytes()));
            txn.commit();
        }

        try (TransactionDB db = openDb(tempDir)) {
            assertNull(db.get("k".getBytes()));
        }
    }

    // -----------------------------------------------------------------------
    // Savepoints
    // -----------------------------------------------------------------------

    @Test
    void rollbackToSavePointRestoresPartialState(@TempDir Path tempDir) {
        try (TransactionDB db = openDb(tempDir);
             WriteOptions wo = new WriteOptions();
             ReadOptions ro = new ReadOptions();
             Transaction txn = db.beginTransaction(wo)) {

            txn.put("k1".getBytes(), "v1".getBytes());
            txn.setSavePoint();
            txn.put("k2".getBytes(), "v2".getBytes());

            txn.rollbackToSavePoint();

            // k1 still staged, k2 gone
            assertArrayEquals("v1".getBytes(), txn.get(ro, "k1".getBytes()));
            assertNull(txn.get(ro, "k2".getBytes()));

            txn.commit();
        }

        try (TransactionDB db = openDb(tempDir)) {
            assertArrayEquals("v1".getBytes(), db.get("k1".getBytes()));
            assertNull(db.get("k2".getBytes()));
        }
    }

    // -----------------------------------------------------------------------
    // Direct (non-transactional) ops on TransactionDB
    // -----------------------------------------------------------------------

    @Test
    void directPutGetDelete(@TempDir Path tempDir) {
        try (TransactionDB db = openDb(tempDir)) {
            db.put("k".getBytes(), "v".getBytes());
            assertArrayEquals("v".getBytes(), db.get("k".getBytes()));
            db.delete("k".getBytes());
            assertNull(db.get("k".getBytes()));
        }
    }

    // -----------------------------------------------------------------------
    // TransactionOptions
    // -----------------------------------------------------------------------

    @Test
    void transactionOptionsSetSnapshot(@TempDir Path tempDir) {
        try (TransactionDB db = openDb(tempDir);
             WriteOptions wo = new WriteOptions();
             TransactionOptions txnOpts = new TransactionOptions().setSetSnapshot(true);
             Transaction txn = db.beginTransaction(wo, txnOpts)) {

            txn.put("k".getBytes(), "v".getBytes());
            txn.commit();

            assertArrayEquals("v".getBytes(), db.get("k".getBytes()));
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private void db_put(Path dir, String key, String value) {
        try (TransactionDB db = openDb(dir);
             WriteOptions wo = new WriteOptions();
             Transaction txn = db.beginTransaction(wo)) {
            txn.put(key.getBytes(), value.getBytes());
            txn.commit();
        }
    }
}
