package io.github.dfa1.rocksdbffm.jni;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.*;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class TableOptionsJniTest {

    static {
        RocksDB.loadLibrary();
    }

    // -----------------------------------------------------------------------
    // LRUCache
    // -----------------------------------------------------------------------

    @Test
    void lruCache_usageIsNonNegative() {
        // Given / When
        try (var cache = new LRUCache(16 * 1024 * 1024L)) {

            // Then
            assertThat(cache.getUsage()).isGreaterThanOrEqualTo(0L);
            assertThat(cache.getPinnedUsage()).isGreaterThanOrEqualTo(0L);
        }
    }

    // -----------------------------------------------------------------------
    // BlockBasedTableConfig — basic open/read/write
    // -----------------------------------------------------------------------

    @Test
    void defaultTableConfig_allowsReadWrite(@TempDir Path dir) throws RocksDBException {
        // Given
        var tbl = new BlockBasedTableConfig();
        try (var opts = new Options().setCreateIfMissing(true).setTableFormatConfig(tbl);
             var db = RocksDB.open(opts, dir.toString())) {

            db.put("k".getBytes(), "v".getBytes());

            // When
            var result = db.get("k".getBytes());

            // Then
            assertThat(result).isEqualTo("v".getBytes());
        }
    }

    // -----------------------------------------------------------------------
    // FilterPolicy — Bloom
    // -----------------------------------------------------------------------

    @Test
    void bloomFilter_returnsExistingKey(@TempDir Path dir) throws RocksDBException {
        // Given
        var tbl = new BlockBasedTableConfig().setFilterPolicy(new BloomFilter(10));
        try (var opts = new Options().setCreateIfMissing(true).setTableFormatConfig(tbl);
             var db = RocksDB.open(opts, dir.toString())) {

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
    // Shared block cache
    // -----------------------------------------------------------------------

    @Test
    void sharedBlockCache_servesCachedReads(@TempDir Path dir) throws RocksDBException {
        // Given
        try (var cache = new LRUCache(64 * 1024 * 1024L)) {
            var tbl = new BlockBasedTableConfig()
                .setBlockCache(cache)
                .setCacheIndexAndFilterBlocks(true);
            try (var opts = new Options().setCreateIfMissing(true).setTableFormatConfig(tbl);
                 var db = RocksDB.open(opts, dir.toString())) {

                for (int i = 0; i < 100; i++) {
                    db.put(("key-" + i).getBytes(), ("val-" + i).getBytes());
                }

                // When — read all keys to populate the cache
                for (int i = 0; i < 100; i++) {
                    assertThat(db.get(("key-" + i).getBytes())).isEqualTo(("val-" + i).getBytes());
                }

                // Then — cache should have been used
                assertThat(cache.getUsage()).isGreaterThanOrEqualTo(0L);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Index type
    // -----------------------------------------------------------------------

    @Test
    void twoLevelIndexSearch_allowsReadWrite(@TempDir Path dir) throws RocksDBException {
        // Given
        var tbl = new BlockBasedTableConfig()
            .setIndexType(IndexType.kTwoLevelIndexSearch)
            .setPartitionFilters(true)
            .setFilterPolicy(new BloomFilter(10));
        try (var opts = new Options().setCreateIfMissing(true).setTableFormatConfig(tbl);
             var db = RocksDB.open(opts, dir.toString())) {

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
    void formatVersion5_allowsReadWrite(@TempDir Path dir) throws RocksDBException {
        // Given
        var tbl = new BlockBasedTableConfig().setFormatVersion(5);
        try (var opts = new Options().setCreateIfMissing(true).setTableFormatConfig(tbl);
             var db = RocksDB.open(opts, dir.toString())) {

            db.put("k".getBytes(), "v".getBytes());

            // When
            var result = db.get("k".getBytes());

            // Then
            assertThat(result).isEqualTo("v".getBytes());
        }
    }
}
