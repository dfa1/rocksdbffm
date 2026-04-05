package io.github.dfa1.rocksdbffm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class SnapshotTest {

    // -----------------------------------------------------------------------
    // Snapshot isolation
    // -----------------------------------------------------------------------

    @Test
    void snapshot_readsDoNotSeeWritesAfterSnapshot(@TempDir Path dir) {
        // Given
        try (var db = RocksDB.open(dir)) {
            db.put("key".getBytes(), "before".getBytes());

            // When — take snapshot, then overwrite the key
            try (Snapshot snap = db.getSnapshot();
                 ReadOptions ro = new ReadOptions().setSnapshot(snap)) {

                db.put("key".getBytes(), "after".getBytes());

                // Then — read via snapshot sees the pre-write value
                assertThat(db.get(ro, "key".getBytes())).isEqualTo("before".getBytes());
                // And a plain read sees the new value
                assertThat(db.get("key".getBytes())).isEqualTo("after".getBytes());
            }
        }
    }

    @Test
    void snapshot_readsDoNotSeeDeletesAfterSnapshot(@TempDir Path dir) {
        // Given
        try (var db = RocksDB.open(dir)) {
            db.put("key".getBytes(), "value".getBytes());

            // When
            try (Snapshot snap = db.getSnapshot();
                 ReadOptions ro = new ReadOptions().setSnapshot(snap)) {

                db.delete("key".getBytes());

                // Then — snapshot still sees the deleted key
                assertThat(db.get(ro, "key".getBytes())).isEqualTo("value".getBytes());
                assertThat(db.get("key".getBytes())).isNull();
            }
        }
    }

    @Test
    void snapshot_sequenceNumberIsNonNegative(@TempDir Path dir) {
        // Given / When
        try (var db = RocksDB.open(dir)) {
            db.put("k".getBytes(), "v".getBytes());
            try (Snapshot snap = db.getSnapshot()) {

                // Then
                assertThat(snap.sequenceNumber()).isGreaterThan(0);
            }
        }
    }

    @Test
    void snapshot_sequenceNumberIncreasesWithWrites(@TempDir Path dir) {
        // Given
        try (var db = RocksDB.open(dir)) {
            try (Snapshot snap1 = db.getSnapshot()) {
                db.put("k".getBytes(), "v".getBytes());
                try (Snapshot snap2 = db.getSnapshot()) {

                    // Then
                    assertThat(snap2.sequenceNumber()).isGreaterThan(snap1.sequenceNumber());
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // ReadOptions.setSnapshot(null) clears the snapshot
    // -----------------------------------------------------------------------

    @Test
    void setSnapshot_null_clearsSnapshot(@TempDir Path dir) {
        // Given
        try (var db = RocksDB.open(dir);
             ReadOptions ro = new ReadOptions()) {
            db.put("key".getBytes(), "v1".getBytes());

            try (Snapshot snap = db.getSnapshot()) {
                ro.setSnapshot(snap);
                db.put("key".getBytes(), "v2".getBytes());

                // Snapshot pinned — sees v1
                assertThat(db.get(ro, "key".getBytes())).isEqualTo("v1".getBytes());

                // When — clear snapshot
                ro.setSnapshot(null);

                // Then — sees latest value
                assertThat(db.get(ro, "key".getBytes())).isEqualTo("v2".getBytes());
            }
        }
    }

    // -----------------------------------------------------------------------
    // Snapshot + iterator
    // -----------------------------------------------------------------------

    @Test
    void snapshot_iteratorDoesNotSeeWritesAfterSnapshot(@TempDir Path dir) {
        // Given
        try (var db = RocksDB.open(dir)) {
            db.put("a".getBytes(), "1".getBytes());
            db.put("b".getBytes(), "2".getBytes());

            try (Snapshot snap = db.getSnapshot();
                 ReadOptions ro = new ReadOptions().setSnapshot(snap);
                 RocksIterator it = db.newIterator(ro)) {

                // Write after snapshot
                db.put("c".getBytes(), "3".getBytes());

                // When
                java.util.List<String> keys = new java.util.ArrayList<>();
                for (it.seekToFirst(); it.isValid(); it.next()) {
                    keys.add(new String(it.key()));
                }
                it.checkError();

                // Then — "c" was written after snapshot, must not appear
                assertThat(keys).containsExactly("a", "b");
            }
        }
    }

    // -----------------------------------------------------------------------
    // TransactionDB snapshot
    // -----------------------------------------------------------------------

    @Test
    void transactionDB_snapshot_isolation(@TempDir Path dir) {
        // Given
        try (var txnDbOpts = new TransactionDBOptions();
             var opts = new Options().setCreateIfMissing(true);
             var db = TransactionDB.open(opts, txnDbOpts, dir)) {

            db.put("key".getBytes(), "before".getBytes());

            // When
            try (Snapshot snap = db.getSnapshot();
                 ReadOptions ro = new ReadOptions().setSnapshot(snap);
                 WriteOptions wo = new WriteOptions();
                 Transaction txn = db.beginTransaction(wo)) {

                txn.put("key".getBytes(), "after".getBytes());
                txn.commit();

                // Then — snapshot read still sees the pre-commit value
                assertThat(db.get(ro, "key".getBytes())).isEqualTo("before".getBytes());
            }
        }
    }
}
