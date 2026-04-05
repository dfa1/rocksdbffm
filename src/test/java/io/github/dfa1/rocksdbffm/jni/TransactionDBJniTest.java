package io.github.dfa1.rocksdbffm.jni;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.*;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * JNI parity tests for the transaction API, mirroring TransactionDBFfmTest.
 */
class TransactionDBJniTest {

    static {
        RocksDB.loadLibrary();
    }

    private static TransactionDB openDb(Path path) throws RocksDBException {
        Options opts = new Options().setCreateIfMissing(true);
        TransactionDBOptions txnDbOpts = new TransactionDBOptions();
        return TransactionDB.open(opts, txnDbOpts, path.toString());
    }

    @Test
    void commitMakesChangesVisible(@TempDir Path tempDir) throws RocksDBException {
        try (TransactionDB db = openDb(tempDir);
             WriteOptions wo = new WriteOptions();
             Transaction txn = db.beginTransaction(wo)) {

            txn.put("k".getBytes(), "v".getBytes());
            txn.commit();

            assertArrayEquals("v".getBytes(), db.get("k".getBytes()));
        }
    }

    @Test
    void rollbackDiscardsChanges(@TempDir Path tempDir) throws RocksDBException {
        try (TransactionDB db = openDb(tempDir);
             WriteOptions wo = new WriteOptions();
             Transaction txn = db.beginTransaction(wo)) {

            txn.put("k".getBytes(), "v".getBytes());
            txn.rollback();

            assertNull(db.get("k".getBytes()));
        }
    }

    @Test
    void getReadsUncommittedWritesInSameTransaction(@TempDir Path tempDir) throws RocksDBException {
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
    void getMissingKeyReturnsNull(@TempDir Path tempDir) throws RocksDBException {
        try (TransactionDB db = openDb(tempDir);
             WriteOptions wo = new WriteOptions();
             ReadOptions ro = new ReadOptions();
             Transaction txn = db.beginTransaction(wo)) {

            assertNull(txn.get(ro, "missing".getBytes()));
            txn.rollback();
        }
    }

    @Test
    void getForUpdateLocksKey(@TempDir Path tempDir) throws RocksDBException {
        try (TransactionDB db = openDb(tempDir);
             WriteOptions wo = new WriteOptions();
             Transaction seed = db.beginTransaction(wo)) {
            seed.put("k".getBytes(), "original".getBytes());
            seed.commit();
        }

        try (TransactionDB db = openDb(tempDir);
             WriteOptions wo = new WriteOptions();
             ReadOptions ro = new ReadOptions();
             Transaction txn = db.beginTransaction(wo)) {

            byte[] val = txn.getForUpdate(ro, "k".getBytes(), true);
            assertArrayEquals("original".getBytes(), val);
            txn.put("k".getBytes(), "updated".getBytes());
            txn.commit();

            assertArrayEquals("updated".getBytes(), db.get("k".getBytes()));
        }
    }

    @Test
    void deleteInsideTransaction(@TempDir Path tempDir) throws RocksDBException {
        try (TransactionDB db = openDb(tempDir);
             WriteOptions wo = new WriteOptions();
             Transaction seed = db.beginTransaction(wo)) {
            seed.put("k".getBytes(), "v".getBytes());
            seed.commit();
        }

        try (TransactionDB db = openDb(tempDir);
             WriteOptions wo = new WriteOptions();
             ReadOptions ro = new ReadOptions();
             Transaction txn = db.beginTransaction(wo)) {

            txn.delete("k".getBytes());
            assertNull(txn.get(ro, "k".getBytes()));
            txn.commit();

            assertNull(db.get("k".getBytes()));
        }
    }

    @Test
    void rollbackToSavePointRestoresPartialState(@TempDir Path tempDir) throws RocksDBException {
        try (TransactionDB db = openDb(tempDir);
             WriteOptions wo = new WriteOptions();
             ReadOptions ro = new ReadOptions();
             Transaction txn = db.beginTransaction(wo)) {

            txn.put("k1".getBytes(), "v1".getBytes());
            txn.setSavePoint();
            txn.put("k2".getBytes(), "v2".getBytes());
            txn.rollbackToSavePoint();

            assertArrayEquals("v1".getBytes(), txn.get(ro, "k1".getBytes()));
            assertNull(txn.get(ro, "k2".getBytes()));

            txn.commit();
        }
    }
}
