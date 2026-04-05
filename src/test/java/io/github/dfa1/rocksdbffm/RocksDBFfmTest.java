package io.github.dfa1.rocksdbffm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class RocksDBFfmTest {

    @Test
    void putGetDelete(@TempDir Path tempDir) {
        try (RocksDB db = RocksDB.open(tempDir.toString())) {
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
        try (RocksDB db = RocksDB.open(tempDir.toString())) {
            assertNull(db.get("nonexistent".getBytes()));
        }
    }

    @Test
    void overwriteKey(@TempDir Path tempDir) {
        try (RocksDB db = RocksDB.open(tempDir.toString())) {
            byte[] key = "k".getBytes();
            db.put(key, "v1".getBytes());
            db.put(key, "v2".getBytes());
            assertArrayEquals("v2".getBytes(), db.get(key));
        }
    }

    @Test
    void binaryKeysAndValues(@TempDir Path tempDir) {
        try (RocksDB db = RocksDB.open(tempDir.toString())) {
            byte[] key = new byte[]{0x00, 0x01, (byte) 0xFF};
            byte[] value = new byte[]{0x42, 0x00, 0x43};
            db.put(key, value);
            assertArrayEquals(value, db.get(key));
        }
    }

    @Test
    void batchPutsAreVisible(@TempDir Path tempDir) {
        try (RocksDB db = RocksDB.open(tempDir.toString());
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
        try (RocksDB db = RocksDB.open(tempDir.toString());
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
        try (RocksDB db = RocksDB.open(tempDir.toString());
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
}
