package io.github.dfa1.rocksdbffm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class RocksDBFfmTest {

    @Test
    void putGetDelete(@TempDir Path tempDir) {
        try (RocksDB db = RocksDB.open(tempDir)) {
            byte[] key = "hello".getBytes();
            byte[] value = "world".getBytes();

            db.put(key, value);
            assertArrayEquals(value, db.get(key));

            db.delete(key);
            assertNull(db.get(key));
        }
    }

    @Test
    void getMissingKeyReturnsNull(@TempDir Path tempDir) {
        try (RocksDB db = RocksDB.open(tempDir)) {
            assertNull(db.get("nonexistent".getBytes()));
        }
    }

    @Test
    void overwriteKey(@TempDir Path tempDir) {
        try (RocksDB db = RocksDB.open(tempDir)) {
            byte[] key = "k".getBytes();
            db.put(key, "v1".getBytes());
            db.put(key, "v2".getBytes());
            assertArrayEquals("v2".getBytes(), db.get(key));
        }
    }

    @Test
    void binaryKeysAndValues(@TempDir Path tempDir) {
        try (RocksDB db = RocksDB.open(tempDir)) {
            byte[] key = new byte[]{0x00, 0x01, (byte) 0xFF};
            byte[] value = new byte[]{0x42, 0x00, 0x43};
            db.put(key, value);
            assertArrayEquals(value, db.get(key));
        }
    }

    @Test
    void batchPutsAreVisible(@TempDir Path tempDir) {
        try (RocksDB db = RocksDB.open(tempDir);
             WriteBatch batch = WriteBatch.create()) {

            batch.put("k1".getBytes(), "v1".getBytes());
            batch.put("k2".getBytes(), "v2".getBytes());
            batch.put("k3".getBytes(), "v3".getBytes());
            db.write(batch);

            assertArrayEquals("v1".getBytes(), db.get("k1".getBytes()));
            assertArrayEquals("v2".getBytes(), db.get("k2".getBytes()));
            assertArrayEquals("v3".getBytes(), db.get("k3".getBytes()));
        }
    }

    @Test
    void batchDeleteRemovesKeys(@TempDir Path tempDir) {
        try (RocksDB db = RocksDB.open(tempDir);
             WriteBatch batch = WriteBatch.create()) {

            db.put("k1".getBytes(), "v1".getBytes());
            db.put("k2".getBytes(), "v2".getBytes());

            batch.delete("k1".getBytes());
            batch.delete("k2".getBytes());
            db.write(batch);

            assertNull(db.get("k1".getBytes()));
            assertNull(db.get("k2".getBytes()));
        }
    }

    @Test
    void batchCountReflectsOperations(@TempDir Path tempDir) {
        try (RocksDB db = RocksDB.open(tempDir);
             WriteBatch batch = WriteBatch.create()) {

            for (int i = 0; i < 50; i++) {
                batch.put(("key-" + i).getBytes(), ("val-" + i).getBytes());
            }
            assertEquals(50, batch.count());
            db.write(batch);

            for (int i = 0; i < 50; i++) {
                assertArrayEquals(("val-" + i).getBytes(), db.get(("key-" + i).getBytes()));
            }
        }
    }

    // -----------------------------------------------------------------------
    // createIfMissing
    // -----------------------------------------------------------------------

    @Test
    void createIfMissingFalseFailsOnNewDb(@TempDir Path tempDir) {
        Path dbPath = tempDir.resolve("nonexistent");
        try (Options opts = new Options().setCreateIfMissing(false)) {
            assertThrows(RocksDBException.class, () -> RocksDB.open(opts, dbPath));
        }
    }

    @Test
    void createIfMissingTrueCreatesDb(@TempDir Path tempDir) {
        Path dbPath = tempDir.resolve("newdb");
        try (Options opts = new Options().setCreateIfMissing(true);
             RocksDB db = RocksDB.open(opts, dbPath)) {
            db.put("k".getBytes(), "v".getBytes());
            assertArrayEquals("v".getBytes(), db.get("k".getBytes()));
        }
    }

    @Test
    void optionsGetCreateIfMissingRoundTrips() {
        try (Options opts = new Options()) {
            assertFalse(opts.getCreateIfMissing());
            opts.setCreateIfMissing(true);
            assertTrue(opts.getCreateIfMissing());
            opts.setCreateIfMissing(false);
            assertFalse(opts.getCreateIfMissing());
        }
    }

    // -----------------------------------------------------------------------
    // readOnly
    // -----------------------------------------------------------------------

    @Test
    void readOnlyAllowsReads(@TempDir Path tempDir) {
        try (RocksDB rw = RocksDB.open(tempDir)) {
            rw.put("k".getBytes(), "v".getBytes());
        }
        try (RocksDB ro = RocksDB.openReadOnly(tempDir)) {
            assertArrayEquals("v".getBytes(), ro.get("k".getBytes()));
        }
    }

    @Test
    void readOnlyRejectsPut(@TempDir Path tempDir) {
        try (RocksDB rw = RocksDB.open(tempDir)) {
            rw.put("seed".getBytes(), "val".getBytes());
        }
        try (RocksDB ro = RocksDB.openReadOnly(tempDir)) {
            assertThrows(RocksDBException.class,
                () -> ro.put("k".getBytes(), "v".getBytes()));
        }
    }

    @Test
    void readOnlyRejectsDelete(@TempDir Path tempDir) {
        try (RocksDB rw = RocksDB.open(tempDir)) {
            rw.put("k".getBytes(), "v".getBytes());
        }
        try (RocksDB ro = RocksDB.openReadOnly(tempDir)) {
            assertThrows(RocksDBException.class,
                () -> ro.delete("k".getBytes()));
        }
    }

    @Test
    void readOnlyWithOptions(@TempDir Path tempDir) {
        try (RocksDB rw = RocksDB.open(tempDir)) {
            rw.put("hello".getBytes(), "world".getBytes());
        }
        try (Options opts = new Options();
             RocksDB ro = RocksDB.openReadOnly(opts, tempDir)) {
            assertArrayEquals("world".getBytes(), ro.get("hello".getBytes()));
        }
    }
}
