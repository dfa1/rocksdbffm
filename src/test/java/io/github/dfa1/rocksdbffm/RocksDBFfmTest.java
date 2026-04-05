package io.github.dfa1.rocksdbffm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RocksDBFfmTest {

    // -----------------------------------------------------------------------
    // put / get / delete
    // -----------------------------------------------------------------------

    @Test
    void get_returnsStoredValue(@TempDir Path dir) {
        // Given
        try (var db = RocksDB.open(dir)) {
            db.put("hello".getBytes(), "world".getBytes());

            // When
            var result = db.get("hello".getBytes());

            // Then
            assertThat(result).isEqualTo("world".getBytes());
        }
    }

    @Test
    void get_returnsNull_whenKeyAbsent(@TempDir Path dir) {
        // Given
        try (var db = RocksDB.open(dir)) {

            // When
            var result = db.get("nonexistent".getBytes());

            // Then
            assertThat(result).isNull();
        }
    }

    @Test
    void get_returnsNull_afterDelete(@TempDir Path dir) {
        // Given
        try (var db = RocksDB.open(dir)) {
            db.put("k".getBytes(), "v".getBytes());

            // When
            db.delete("k".getBytes());

            // Then
            assertThat(db.get("k".getBytes())).isNull();
        }
    }

    @Test
    void put_overwritesExistingKey(@TempDir Path dir) {
        // Given
        try (var db = RocksDB.open(dir)) {
            db.put("k".getBytes(), "v1".getBytes());

            // When
            db.put("k".getBytes(), "v2".getBytes());

            // Then
            assertThat(db.get("k".getBytes())).isEqualTo("v2".getBytes());
        }
    }

    @Test
    void put_handlesArbitraryBinaryKeys(@TempDir Path dir) {
        // Given
        var key   = new byte[]{0x00, 0x01, (byte) 0xFF};
        var value = new byte[]{0x42, 0x00, 0x43};

        try (var db = RocksDB.open(dir)) {
            // When
            db.put(key, value);

            // Then
            assertThat(db.get(key)).isEqualTo(value);
        }
    }

    // -----------------------------------------------------------------------
    // WriteBatch
    // -----------------------------------------------------------------------

    @Test
    void write_commitsBatchedPuts(@TempDir Path dir) {
        // Given
        try (var db = RocksDB.open(dir);
             var batch = WriteBatch.create()) {

            batch.put("k1".getBytes(), "v1".getBytes());
            batch.put("k2".getBytes(), "v2".getBytes());
            batch.put("k3".getBytes(), "v3".getBytes());

            // When
            db.write(batch);

            // Then
            assertThat(db.get("k1".getBytes())).isEqualTo("v1".getBytes());
            assertThat(db.get("k2".getBytes())).isEqualTo("v2".getBytes());
            assertThat(db.get("k3".getBytes())).isEqualTo("v3".getBytes());
        }
    }

    @Test
    void write_commitsBatchedDeletes(@TempDir Path dir) {
        // Given
        try (var db = RocksDB.open(dir);
             var batch = WriteBatch.create()) {

            db.put("k1".getBytes(), "v1".getBytes());
            db.put("k2".getBytes(), "v2".getBytes());
            batch.delete("k1".getBytes());
            batch.delete("k2".getBytes());

            // When
            db.write(batch);

            // Then
            assertThat(db.get("k1".getBytes())).isNull();
            assertThat(db.get("k2".getBytes())).isNull();
        }
    }

    @Test
    void writeBatch_countReflectsQueuedOperations(@TempDir Path dir) {
        // Given
        try (var db = RocksDB.open(dir);
             var batch = WriteBatch.create()) {

            for (int i = 0; i < 50; i++) {
                batch.put(("key-" + i).getBytes(), ("val-" + i).getBytes());
            }

            // When / Then (count is observable before commit)
            assertThat(batch.count()).isEqualTo(50);

            db.write(batch);
            for (int i = 0; i < 50; i++) {
                assertThat(db.get(("key-" + i).getBytes())).isEqualTo(("val-" + i).getBytes());
            }
        }
    }

    // -----------------------------------------------------------------------
    // Options — createIfMissing
    // -----------------------------------------------------------------------

    @Test
    void open_fails_whenDbAbsentAndCreateIfMissingFalse(@TempDir Path dir) {
        // Given
        var dbPath = dir.resolve("nonexistent");

        try (var opts = new Options().setCreateIfMissing(false)) {
            // When / Then
            assertThatThrownBy(() -> RocksDB.open(opts, dbPath))
                .isInstanceOf(RocksDBException.class);
        }
    }

    @Test
    void open_createsDb_whenCreateIfMissingTrue(@TempDir Path dir) {
        // Given
        var dbPath = dir.resolve("newdb");

        try (var opts = new Options().setCreateIfMissing(true);
             var db = RocksDB.open(opts, dbPath)) {

            // When
            db.put("k".getBytes(), "v".getBytes());

            // Then
            assertThat(db.get("k".getBytes())).isEqualTo("v".getBytes());
        }
    }

    @Test
    void options_createIfMissing_roundTrips() {
        // Given
        try (var opts = new Options()) {
            assertThat(opts.getCreateIfMissing()).isFalse();

            // When
            opts.setCreateIfMissing(true);
            // Then
            assertThat(opts.getCreateIfMissing()).isTrue();

            // When
            opts.setCreateIfMissing(false);
            // Then
            assertThat(opts.getCreateIfMissing()).isFalse();
        }
    }

    // -----------------------------------------------------------------------
    // readOnly
    // -----------------------------------------------------------------------

    @Test
    void openReadOnly_allowsReads(@TempDir Path dir) {
        // Given
        try (var rw = RocksDB.open(dir)) {
            rw.put("k".getBytes(), "v".getBytes());
        }

        // When
        try (var ro = RocksDB.openReadOnly(dir)) {
            var result = ro.get("k".getBytes());

            // Then
            assertThat(result).isEqualTo("v".getBytes());
        }
    }

    @Test
    void openReadOnly_rejectsPut(@TempDir Path dir) {
        // Given
        try (var rw = RocksDB.open(dir)) {
            rw.put("seed".getBytes(), "val".getBytes());
        }

        try (var ro = RocksDB.openReadOnly(dir)) {
            // When / Then
            assertThatThrownBy(() -> ro.put("k".getBytes(), "v".getBytes()))
                .isInstanceOf(RocksDBException.class);
        }
    }

    @Test
    void openReadOnly_rejectsDelete(@TempDir Path dir) {
        // Given
        try (var rw = RocksDB.open(dir)) {
            rw.put("k".getBytes(), "v".getBytes());
        }

        try (var ro = RocksDB.openReadOnly(dir)) {
            // When / Then
            assertThatThrownBy(() -> ro.delete("k".getBytes()))
                .isInstanceOf(RocksDBException.class);
        }
    }

    @Test
    void openReadOnly_withExplicitOptions(@TempDir Path dir) {
        // Given
        try (var rw = RocksDB.open(dir)) {
            rw.put("hello".getBytes(), "world".getBytes());
        }

        try (var opts = new Options();
             var ro = RocksDB.openReadOnly(opts, dir)) {

            // When
            var result = ro.get("hello".getBytes());

            // Then
            assertThat(result).isEqualTo("world".getBytes());
        }
    }
}
