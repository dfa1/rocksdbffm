package com.example;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class RocksDBJniTest {

    static {
        RocksDB.loadLibrary();
    }

    @Test
    void putGetDelete(@TempDir Path tempDir) throws RocksDBException {
        try (Options opts = new Options().setCreateIfMissing(true);
             RocksDB db = RocksDB.open(opts, tempDir.toString())) {

            byte[] key = "hello".getBytes();
            byte[] value = "world".getBytes();

            db.put(key, value);
            assertArrayEquals(value, db.get(key));

            db.delete(key);
            assertNull(db.get(key));
        }
    }

    @Test
    void getMissingKeyReturnsNull(@TempDir Path tempDir) throws RocksDBException {
        try (Options opts = new Options().setCreateIfMissing(true);
             RocksDB db = RocksDB.open(opts, tempDir.toString())) {
            assertNull(db.get("nonexistent".getBytes()));
        }
    }

    @Test
    void overwriteKey(@TempDir Path tempDir) throws RocksDBException {
        try (Options opts = new Options().setCreateIfMissing(true);
             RocksDB db = RocksDB.open(opts, tempDir.toString())) {
            byte[] key = "k".getBytes();
            db.put(key, "v1".getBytes());
            db.put(key, "v2".getBytes());
            assertArrayEquals("v2".getBytes(), db.get(key));
        }
    }
}
