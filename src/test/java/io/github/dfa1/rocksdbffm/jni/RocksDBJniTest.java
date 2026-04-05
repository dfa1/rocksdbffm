package io.github.dfa1.rocksdbffm.jni;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.*;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RocksDBJniTest {

    static {
        RocksDB.loadLibrary();
    }

    @Test
    void get_returnsStoredValue(@TempDir Path dir) throws RocksDBException {
        // Given
        try (var opts = new Options().setCreateIfMissing(true);
             var db = RocksDB.open(opts, dir.toString())) {

            db.put("hello".getBytes(), "world".getBytes());

            // When
            var result = db.get("hello".getBytes());

            // Then
            assertThat(result).isEqualTo("world".getBytes());
        }
    }

    @Test
    void get_returnsNull_whenKeyAbsent(@TempDir Path dir) throws RocksDBException {
        // Given
        try (var opts = new Options().setCreateIfMissing(true);
             var db = RocksDB.open(opts, dir.toString())) {

            // When
            var result = db.get("nonexistent".getBytes());

            // Then
            assertThat(result).isNull();
        }
    }

    @Test
    void get_returnsNull_afterDelete(@TempDir Path dir) throws RocksDBException {
        // Given
        try (var opts = new Options().setCreateIfMissing(true);
             var db = RocksDB.open(opts, dir.toString())) {

            db.put("k".getBytes(), "v".getBytes());

            // When
            db.delete("k".getBytes());

            // Then
            assertThat(db.get("k".getBytes())).isNull();
        }
    }

    @Test
    void put_overwritesExistingKey(@TempDir Path dir) throws RocksDBException {
        // Given
        try (var opts = new Options().setCreateIfMissing(true);
             var db = RocksDB.open(opts, dir.toString())) {

            db.put("k".getBytes(), "v1".getBytes());

            // When
            db.put("k".getBytes(), "v2".getBytes());

            // Then
            assertThat(db.get("k".getBytes())).isEqualTo("v2".getBytes());
        }
    }

    @Test
    void write_commitsBatchedPuts(@TempDir Path dir) throws RocksDBException {
        // Given
        try (var opts = new Options().setCreateIfMissing(true);
             var db = RocksDB.open(opts, dir.toString());
             var wo = new WriteOptions();
             var batch = new WriteBatch()) {

            batch.put("k1".getBytes(), "v1".getBytes());
            batch.put("k2".getBytes(), "v2".getBytes());
            batch.put("k3".getBytes(), "v3".getBytes());

            // When
            db.write(wo, batch);

            // Then
            assertThat(db.get("k1".getBytes())).isEqualTo("v1".getBytes());
            assertThat(db.get("k2".getBytes())).isEqualTo("v2".getBytes());
            assertThat(db.get("k3".getBytes())).isEqualTo("v3".getBytes());
        }
    }

    @Test
    void write_commitsBatchedDeletes(@TempDir Path dir) throws RocksDBException {
        // Given
        try (var opts = new Options().setCreateIfMissing(true);
             var db = RocksDB.open(opts, dir.toString());
             var wo = new WriteOptions();
             var batch = new WriteBatch()) {

            db.put("k1".getBytes(), "v1".getBytes());
            db.put("k2".getBytes(), "v2".getBytes());
            batch.delete("k1".getBytes());
            batch.delete("k2".getBytes());

            // When
            db.write(wo, batch);

            // Then
            assertThat(db.get("k1".getBytes())).isNull();
            assertThat(db.get("k2".getBytes())).isNull();
        }
    }

    @Test
    void writeBatch_countReflectsQueuedOperations(@TempDir Path dir) throws RocksDBException {
        // Given
        try (var opts = new Options().setCreateIfMissing(true);
             var db = RocksDB.open(opts, dir.toString());
             var wo = new WriteOptions();
             var batch = new WriteBatch()) {

            for (int i = 0; i < 50; i++) {
                batch.put(("key-" + i).getBytes(), ("val-" + i).getBytes());
            }

            // When / Then
            assertThat(batch.count()).isEqualTo(50);
            db.write(wo, batch);
        }
    }

    @Test
    void open_fails_whenDbAbsentAndCreateIfMissingFalse(@TempDir Path dir) {
        // Given
        var dbPath = dir.resolve("nonexistent");

        try (var opts = new Options().setCreateIfMissing(false)) {
            // When / Then
            assertThatThrownBy(() -> RocksDB.open(opts, dbPath.toString()))
                .isInstanceOf(RocksDBException.class);
        }
    }

    @Test
    void open_createsDb_whenCreateIfMissingTrue(@TempDir Path dir) throws RocksDBException {
        // Given
        var dbPath = dir.resolve("newdb");

        try (var opts = new Options().setCreateIfMissing(true);
             var db = RocksDB.open(opts, dbPath.toString())) {

            // When
            db.put("k".getBytes(), "v".getBytes());

            // Then
            assertThat(db.get("k".getBytes())).isEqualTo("v".getBytes());
        }
    }

    @Test
    void openReadOnly_allowsReads(@TempDir Path dir) throws RocksDBException {
        // Given
        try (var opts = new Options().setCreateIfMissing(true);
             var rw = RocksDB.open(opts, dir.toString())) {
            rw.put("k".getBytes(), "v".getBytes());
        }

        try (var opts = new Options();
             var ro = RocksDB.openReadOnly(opts, dir.toString())) {

            // When
            var result = ro.get("k".getBytes());

            // Then
            assertThat(result).isEqualTo("v".getBytes());
        }
    }

    @Test
    void openReadOnly_rejectsPut(@TempDir Path dir) throws RocksDBException {
        // Given
        try (var opts = new Options().setCreateIfMissing(true);
             var rw = RocksDB.open(opts, dir.toString())) {
            rw.put("seed".getBytes(), "val".getBytes());
        }

        try (var opts = new Options();
             var ro = RocksDB.openReadOnly(opts, dir.toString())) {
            // When / Then
            assertThatThrownBy(() -> ro.put("k".getBytes(), "v".getBytes()))
                .isInstanceOf(RocksDBException.class);
        }
    }
}
