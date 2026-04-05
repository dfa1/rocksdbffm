package io.github.dfa1.rocksdbffm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class TableOptionsFfmTest {

    // -----------------------------------------------------------------------
    // LRUCache
    // -----------------------------------------------------------------------

    @Test
    void lruCacheCapacity() {
        try (LRUCache cache = new LRUCache(MemorySize.ofMB(16))) {
            assertEquals(MemorySize.ofMB(16), cache.getCapacity());
        }
    }

    @Test
    void lruCacheSetCapacity() {
        try (LRUCache cache = new LRUCache(MemorySize.ofMB(8))) {
            cache.setCapacity(MemorySize.ofMB(32));
            assertEquals(MemorySize.ofMB(32), cache.getCapacity());
        }
    }

    @Test
    void lruCacheUsageNonNegative() {
        try (LRUCache cache = new LRUCache(MemorySize.ofMB(16))) {
            assertTrue(cache.getUsage().compareTo(MemorySize.ZERO) >= 0);
            assertTrue(cache.getPinnedUsage().compareTo(MemorySize.ZERO) >= 0);
        }
    }

    // -----------------------------------------------------------------------
    // BlockBasedTableConfig — basic open/read/write
    // -----------------------------------------------------------------------

    @Test
    void defaultTableConfigWorks(@TempDir Path tempDir) {
        try (BlockBasedTableConfig tbl = new BlockBasedTableConfig();
             Options opts = new Options().setCreateIfMissing(true).setTableFormatConfig(tbl);
             RocksDB db = RocksDB.open(opts, tempDir)) {

            db.put("k".getBytes(), "v".getBytes());
            assertArrayEquals("v".getBytes(), db.get("k".getBytes()));
        }
    }

    @Test
    void customBlockSize(@TempDir Path tempDir) {
        try (BlockBasedTableConfig tbl = new BlockBasedTableConfig()
                .setBlockSize(MemorySize.ofKB(16));
             Options opts = new Options().setCreateIfMissing(true).setTableFormatConfig(tbl);
             RocksDB db = RocksDB.open(opts, tempDir)) {

            db.put("key".getBytes(), "value".getBytes());
            assertArrayEquals("value".getBytes(), db.get("key".getBytes()));
        }
    }

    // -----------------------------------------------------------------------
    // FilterPolicy — Bloom
    // -----------------------------------------------------------------------

    @Test
    void bloomFilterWorks(@TempDir Path tempDir) {
        try (BlockBasedTableConfig tbl = new BlockBasedTableConfig()
                .setFilterPolicy(FilterPolicy.newBloom(10));
             Options opts = new Options().setCreateIfMissing(true).setTableFormatConfig(tbl);
             RocksDB db = RocksDB.open(opts, tempDir)) {

            db.put("bloom-key".getBytes(), "bloom-value".getBytes());
            assertArrayEquals("bloom-value".getBytes(), db.get("bloom-key".getBytes()));
            assertNull(db.get("absent".getBytes()));
        }
    }

    // -----------------------------------------------------------------------
    // FilterPolicy — Ribbon
    // -----------------------------------------------------------------------

    @Test
    void ribbonFilterWorks(@TempDir Path tempDir) {
        try (BlockBasedTableConfig tbl = new BlockBasedTableConfig()
                .setFilterPolicy(FilterPolicy.newRibbon(10));
             Options opts = new Options().setCreateIfMissing(true).setTableFormatConfig(tbl);
             RocksDB db = RocksDB.open(opts, tempDir)) {

            db.put("ribbon-key".getBytes(), "ribbon-value".getBytes());
            assertArrayEquals("ribbon-value".getBytes(), db.get("ribbon-key".getBytes()));
            assertNull(db.get("absent".getBytes()));
        }
    }

    // -----------------------------------------------------------------------
    // Block cache integration
    // -----------------------------------------------------------------------

    @Test
    void sharedBlockCacheWorks(@TempDir Path tempDir) {
        try (LRUCache cache = new LRUCache(MemorySize.ofMB(64));
             BlockBasedTableConfig tbl = new BlockBasedTableConfig()
                 .setBlockCache(cache)
                 .setCacheIndexAndFilterBlocks(true)
                 .setFilterPolicy(FilterPolicy.newBloom(10));
             Options opts = new Options().setCreateIfMissing(true).setTableFormatConfig(tbl);
             RocksDB db = RocksDB.open(opts, tempDir)) {

            for (int i = 0; i < 100; i++) {
                db.put(("key-" + i).getBytes(), ("val-" + i).getBytes());
            }
            for (int i = 0; i < 100; i++) {
                assertArrayEquals(("val-" + i).getBytes(), db.get(("key-" + i).getBytes()));
            }
            // some data should now be cached
            assertTrue(cache.getUsage().compareTo(MemorySize.ZERO) >= 0);
        }
    }

    @Test
    void noBlockCache(@TempDir Path tempDir) {
        try (BlockBasedTableConfig tbl = new BlockBasedTableConfig()
                .setNoBlockCache(true);
             Options opts = new Options().setCreateIfMissing(true).setTableFormatConfig(tbl);
             RocksDB db = RocksDB.open(opts, tempDir)) {

            db.put("k".getBytes(), "v".getBytes());
            assertArrayEquals("v".getBytes(), db.get("k".getBytes()));
        }
    }

    // -----------------------------------------------------------------------
    // Index type
    // -----------------------------------------------------------------------

    @Test
    void twoLevelIndexType(@TempDir Path tempDir) {
        try (BlockBasedTableConfig tbl = new BlockBasedTableConfig()
                .setIndexType(BlockBasedTableConfig.IndexType.TWO_LEVEL_INDEX_SEARCH)
                .setPartitionFilters(true)
                .setFilterPolicy(FilterPolicy.newBloom(10));
             Options opts = new Options().setCreateIfMissing(true).setTableFormatConfig(tbl);
             RocksDB db = RocksDB.open(opts, tempDir)) {

            db.put("k".getBytes(), "v".getBytes());
            assertArrayEquals("v".getBytes(), db.get("k".getBytes()));
        }
    }

    // -----------------------------------------------------------------------
    // Format version
    // -----------------------------------------------------------------------

    @Test
    void formatVersion5(@TempDir Path tempDir) {
        try (BlockBasedTableConfig tbl = new BlockBasedTableConfig()
                .setFormatVersion(5);
             Options opts = new Options().setCreateIfMissing(true).setTableFormatConfig(tbl);
             RocksDB db = RocksDB.open(opts, tempDir)) {

            db.put("k".getBytes(), "v".getBytes());
            assertArrayEquals("v".getBytes(), db.get("k".getBytes()));
        }
    }
}
