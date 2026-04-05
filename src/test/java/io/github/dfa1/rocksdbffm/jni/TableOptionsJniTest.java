package io.github.dfa1.rocksdbffm.jni;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.*;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * JNI parity tests for table options, mirroring TableOptionsFfmTest.
 */
class TableOptionsJniTest {

    static {
        RocksDB.loadLibrary();
    }

    @Test
    void lruCacheUsage() {
        try (Cache cache = new LRUCache(16 * 1024 * 1024)) {
            assertTrue(cache.getUsage() >= 0);
            assertTrue(cache.getPinnedUsage() >= 0);
        }
    }

    @Test
    void defaultTableConfigWorks(@TempDir Path tempDir) throws RocksDBException {
        BlockBasedTableConfig tbl = new BlockBasedTableConfig();
        try (Options opts = new Options().setCreateIfMissing(true).setTableFormatConfig(tbl);
             RocksDB db = RocksDB.open(opts, tempDir.toString())) {

            db.put("k".getBytes(), "v".getBytes());
            assertArrayEquals("v".getBytes(), db.get("k".getBytes()));
        }
    }

    @Test
    void bloomFilterWorks(@TempDir Path tempDir) throws RocksDBException {
        BloomFilter bloom = new BloomFilter(10);
        BlockBasedTableConfig tbl = new BlockBasedTableConfig().setFilterPolicy(bloom);
        try (Options opts = new Options().setCreateIfMissing(true).setTableFormatConfig(tbl);
             RocksDB db = RocksDB.open(opts, tempDir.toString())) {

            db.put("bloom-key".getBytes(), "bloom-value".getBytes());
            assertArrayEquals("bloom-value".getBytes(), db.get("bloom-key".getBytes()));
            assertNull(db.get("absent".getBytes()));
        }
    }

    @Test
    void sharedBlockCacheWorks(@TempDir Path tempDir) throws RocksDBException {
        try (Cache cache = new LRUCache(64 * 1024 * 1024)) {
            BlockBasedTableConfig tbl = new BlockBasedTableConfig()
                .setBlockCache(cache)
                .setCacheIndexAndFilterBlocks(true);
            try (Options opts = new Options().setCreateIfMissing(true).setTableFormatConfig(tbl);
                 RocksDB db = RocksDB.open(opts, tempDir.toString())) {

                for (int i = 0; i < 100; i++) {
                    db.put(("key-" + i).getBytes(), ("val-" + i).getBytes());
                }
                for (int i = 0; i < 100; i++) {
                    assertArrayEquals(("val-" + i).getBytes(), db.get(("key-" + i).getBytes()));
                }
            }
        }
    }

    @Test
    void twoLevelIndexType(@TempDir Path tempDir) throws RocksDBException {
        BlockBasedTableConfig tbl = new BlockBasedTableConfig()
            .setIndexType(IndexType.kTwoLevelIndexSearch)
            .setPartitionFilters(true)
            .setFilterPolicy(new BloomFilter(10));
        try (Options opts = new Options().setCreateIfMissing(true).setTableFormatConfig(tbl);
             RocksDB db = RocksDB.open(opts, tempDir.toString())) {

            db.put("k".getBytes(), "v".getBytes());
            assertArrayEquals("v".getBytes(), db.get("k".getBytes()));
        }
    }

    @Test
    void formatVersion5(@TempDir Path tempDir) throws RocksDBException {
        BlockBasedTableConfig tbl = new BlockBasedTableConfig().setFormatVersion(5);
        try (Options opts = new Options().setCreateIfMissing(true).setTableFormatConfig(tbl);
             RocksDB db = RocksDB.open(opts, tempDir.toString())) {

            db.put("k".getBytes(), "v".getBytes());
            assertArrayEquals("v".getBytes(), db.get("k".getBytes()));
        }
    }
}
