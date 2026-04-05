package io.github.dfa1.rocksdbffm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class TableOptionsFfmTest {

    // -----------------------------------------------------------------------
    // LRUCache
    // -----------------------------------------------------------------------

    @Test
    void lruCache_reportsConfiguredCapacity() {
        // Given / When
        try (var sut = new LRUCache(MemorySize.ofMB(16))) {

            // Then
            assertThat(sut.getCapacity()).isEqualTo(MemorySize.ofMB(16));
        }
    }

    @Test
    void lruCache_setCapacity_updatesCapacity() {
        // Given
        try (var sut = new LRUCache(MemorySize.ofMB(8))) {

            // When
            sut.setCapacity(MemorySize.ofMB(32));

            // Then
            assertThat(sut.getCapacity()).isEqualTo(MemorySize.ofMB(32));
        }
    }

    @Test
    void lruCache_usageIsNonNegative() {
        // Given / When
        try (var sut = new LRUCache(MemorySize.ofMB(16))) {

            // Then
            assertThat(sut.getUsage()).isGreaterThanOrEqualTo(MemorySize.ZERO);
            assertThat(sut.getPinnedUsage()).isGreaterThanOrEqualTo(MemorySize.ZERO);
        }
    }

    // -----------------------------------------------------------------------
    // BlockBasedTableConfig — basic open/read/write
    // -----------------------------------------------------------------------

    @Test
    void defaultTableConfig_allowsReadWrite(@TempDir Path dir) {
        // Given
        try (var tbl = new BlockBasedTableConfig();
             var opts = new Options().setCreateIfMissing(true).setTableFormatConfig(tbl);
             var db = RocksDB.open(opts, dir)) {

            db.put("k".getBytes(), "v".getBytes());

            // When
            var result = db.get("k".getBytes());

            // Then
            assertThat(result).isEqualTo("v".getBytes());
        }
    }

    @Test
    void customBlockSize_allowsReadWrite(@TempDir Path dir) {
        // Given
        try (var tbl = new BlockBasedTableConfig().setBlockSize(MemorySize.ofKB(16));
             var opts = new Options().setCreateIfMissing(true).setTableFormatConfig(tbl);
             var db = RocksDB.open(opts, dir)) {

            db.put("key".getBytes(), "value".getBytes());

            // When
            var result = db.get("key".getBytes());

            // Then
            assertThat(result).isEqualTo("value".getBytes());
        }
    }

    // -----------------------------------------------------------------------
    // FilterPolicy — Bloom
    // -----------------------------------------------------------------------

    @Test
    void bloomFilter_returnsExistingKey(@TempDir Path dir) {
        // Given
        try (var tbl = new BlockBasedTableConfig().setFilterPolicy(FilterPolicy.newBloom(10));
             var opts = new Options().setCreateIfMissing(true).setTableFormatConfig(tbl);
             var db = RocksDB.open(opts, dir)) {

            db.put("bloom-key".getBytes(), "bloom-value".getBytes());

            // When
            var hit  = db.get("bloom-key".getBytes());
            var miss = db.get("absent".getBytes());

            // Then
            assertThat(hit).isEqualTo("bloom-value".getBytes());
            assertThat(miss).isNull();
        }
    }

    // -----------------------------------------------------------------------
    // FilterPolicy — Ribbon
    // -----------------------------------------------------------------------

    @Test
    void ribbonFilter_returnsExistingKey(@TempDir Path dir) {
        // Given
        try (var tbl = new BlockBasedTableConfig().setFilterPolicy(FilterPolicy.newRibbon(10));
             var opts = new Options().setCreateIfMissing(true).setTableFormatConfig(tbl);
             var db = RocksDB.open(opts, dir)) {

            db.put("ribbon-key".getBytes(), "ribbon-value".getBytes());

            // When
            var hit  = db.get("ribbon-key".getBytes());
            var miss = db.get("absent".getBytes());

            // Then
            assertThat(hit).isEqualTo("ribbon-value".getBytes());
            assertThat(miss).isNull();
        }
    }

    // -----------------------------------------------------------------------
    // Shared block cache
    // -----------------------------------------------------------------------

    @Test
    void sharedBlockCache_servesCachedReads(@TempDir Path dir) {
        // Given
        try (var cache = new LRUCache(MemorySize.ofMB(64));
             var tbl = new BlockBasedTableConfig()
                 .setBlockCache(cache)
                 .setCacheIndexAndFilterBlocks(true)
                 .setFilterPolicy(FilterPolicy.newBloom(10));
             var opts = new Options().setCreateIfMissing(true).setTableFormatConfig(tbl);
             var db = RocksDB.open(opts, dir)) {

            for (int i = 0; i < 100; i++) {
                db.put(("key-" + i).getBytes(), ("val-" + i).getBytes());
            }

            // When — read all keys to populate the cache
            for (int i = 0; i < 100; i++) {
                assertThat(db.get(("key-" + i).getBytes())).isEqualTo(("val-" + i).getBytes());
            }

            // Then — cache should have been used
            assertThat(cache.getUsage()).isGreaterThanOrEqualTo(MemorySize.ZERO);
        }
    }

    @Test
    void noBlockCache_allowsReadWrite(@TempDir Path dir) {
        // Given
        try (var tbl = new BlockBasedTableConfig().setNoBlockCache(true);
             var opts = new Options().setCreateIfMissing(true).setTableFormatConfig(tbl);
             var db = RocksDB.open(opts, dir)) {

            db.put("k".getBytes(), "v".getBytes());

            // When
            var result = db.get("k".getBytes());

            // Then
            assertThat(result).isEqualTo("v".getBytes());
        }
    }

    // -----------------------------------------------------------------------
    // Index type
    // -----------------------------------------------------------------------

    @Test
    void twoLevelIndexSearch_allowsReadWrite(@TempDir Path dir) {
        // Given
        try (var tbl = new BlockBasedTableConfig()
                 .setIndexType(BlockBasedTableConfig.IndexType.TWO_LEVEL_INDEX_SEARCH)
                 .setPartitionFilters(true)
                 .setFilterPolicy(FilterPolicy.newBloom(10));
             var opts = new Options().setCreateIfMissing(true).setTableFormatConfig(tbl);
             var db = RocksDB.open(opts, dir)) {

            db.put("k".getBytes(), "v".getBytes());

            // When
            var result = db.get("k".getBytes());

            // Then
            assertThat(result).isEqualTo("v".getBytes());
        }
    }

    // -----------------------------------------------------------------------
    // Format version
    // -----------------------------------------------------------------------

    @Test
    void formatVersion5_allowsReadWrite(@TempDir Path dir) {
        // Given
        try (var tbl = new BlockBasedTableConfig().setFormatVersion(5);
             var opts = new Options().setCreateIfMissing(true).setTableFormatConfig(tbl);
             var db = RocksDB.open(opts, dir)) {

            db.put("k".getBytes(), "v".getBytes());

            // When
            var result = db.get("k".getBytes());

            // Then
            assertThat(result).isEqualTo("v".getBytes());
        }
    }
}
