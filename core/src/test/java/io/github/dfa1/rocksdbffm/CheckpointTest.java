package io.github.dfa1.rocksdbffm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class CheckpointTest {

    // -----------------------------------------------------------------------
    // Basic export and read-only open
    // -----------------------------------------------------------------------

    @Test
    void exportTo_createsReadableSnapshot(@TempDir Path dir) {
        // Given
        var cpDir = dir.resolve("checkpoint");
        try (var db = RocksDB.open(dir.resolve("db"))) {
            db.put("k".getBytes(), "v".getBytes());

            // When
            try (var cp = Checkpoint.create(db)) {
                cp.exportTo(cpDir);
            }
        }

        // Then — checkpoint is a valid read-only database
        try (var snap = RocksDB.openReadOnly(cpDir)) {
            assertThat(snap.get("k".getBytes())).isEqualTo("v".getBytes());
        }
    }

    @Test
    void snapshot_isIsolatedFromSubsequentWrites(@TempDir Path dir) {
        // Given
        var cpDir = dir.resolve("checkpoint");
        try (var db = RocksDB.open(dir.resolve("db"))) {
            db.put("k".getBytes(), "before".getBytes());

            try (var cp = Checkpoint.create(db)) {
                cp.exportTo(cpDir);
            }

            // When — write after checkpoint
            db.put("k".getBytes(), "after".getBytes());
        }

        // Then — snapshot still sees the value at checkpoint time
        try (var snap = RocksDB.openReadOnly(cpDir)) {
            assertThat(snap.get("k".getBytes())).isEqualTo("before".getBytes());
        }
    }

    // -----------------------------------------------------------------------
    // Multiple checkpoints from one Checkpoint object
    // -----------------------------------------------------------------------

    @Test
    void multipleExports_captureProgressiveState(@TempDir Path dir) {
        // Given
        var cp1Dir = dir.resolve("checkpoint-1");
        var cp2Dir = dir.resolve("checkpoint-2");

        try (var db = RocksDB.open(dir.resolve("db"));
             var cp = Checkpoint.create(db)) {

            db.put("k".getBytes(), "v1".getBytes());
            cp.exportTo(cp1Dir);

            // When — mutate and take a second checkpoint with the same object
            db.put("k".getBytes(), "v2".getBytes());
            cp.exportTo(cp2Dir);
        }

        // Then — each snapshot reflects state at export time
        try (var snap1 = RocksDB.openReadOnly(cp1Dir)) {
            assertThat(snap1.get("k".getBytes())).isEqualTo("v1".getBytes());
        }
        try (var snap2 = RocksDB.openReadOnly(cp2Dir)) {
            assertThat(snap2.get("k".getBytes())).isEqualTo("v2".getBytes());
        }
    }

    // -----------------------------------------------------------------------
    // Absent key in snapshot
    // -----------------------------------------------------------------------

    @Test
    void snapshot_returnsNull_forKeyAbsentAtCheckpointTime(@TempDir Path dir) {
        // Given
        var cpDir = dir.resolve("checkpoint");
        try (var db = RocksDB.open(dir.resolve("db"))) {
            // checkpoint taken before any write
            try (var cp = Checkpoint.create(db)) {
                cp.exportTo(cpDir);
            }

            // When — write after checkpoint
            db.put("k".getBytes(), "v".getBytes());
        }

        // Then — snapshot does not see the post-checkpoint key
        try (var snap = RocksDB.openReadOnly(cpDir)) {
            assertThat(snap.get("k".getBytes())).isNull();
        }
    }

    // -----------------------------------------------------------------------
    // Delete visibility
    // -----------------------------------------------------------------------

    @Test
    void snapshot_preservesDeletedKey_thatWasDeletedAfterCheckpoint(@TempDir Path dir) {
        // Given
        var cpDir = dir.resolve("checkpoint");
        try (var db = RocksDB.open(dir.resolve("db"))) {
            db.put("k".getBytes(), "v".getBytes());

            try (var cp = Checkpoint.create(db)) {
                cp.exportTo(cpDir);
            }

            // When — delete after checkpoint
            db.delete("k".getBytes());
        }

        // Then — snapshot still has the key
        try (var snap = RocksDB.openReadOnly(cpDir)) {
            assertThat(snap.get("k".getBytes())).isEqualTo("v".getBytes());
        }
    }
}
