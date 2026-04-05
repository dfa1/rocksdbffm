package com.example.ffm;

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
}
