package io.github.dfa1.rocksdbffm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Optional;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;

class PropertyTest {

    // -----------------------------------------------------------------------
    // getProperty — string properties
    // -----------------------------------------------------------------------

    @Test
    void getProperty_stats_isPresent(@TempDir Path dir) {
        // Given / When
        try (var db = RocksDB.open(dir)) {
            Optional<String> stats = db.getProperty(Property.STATS);

            // Then
            assertThat(stats).isPresent();
            assertThat(stats.get()).contains("Compaction Stats");
        }
    }

    @Test
    void getProperty_levelStats_isPresent(@TempDir Path dir) {
        try (var db = RocksDB.open(dir)) {
            assertThat(db.getProperty(Property.LEVEL_STATS)).isPresent();
        }
    }

    @Test
    void getProperty_numFilesAtLevelL0_isPresent(@TempDir Path dir) {
        try (var db = RocksDB.open(dir)) {
            // Given
            db.put("k".getBytes(), "v".getBytes());

            // When
            Optional<String> result = db.getProperty(Property.NUM_FILES_AT_LEVEL_L0);

            // Then — value is a digit string like "0" or "1"
            assertThat(result).isPresent();
            assertThat(result.get()).matches("\\d+");
        }
    }

    // -----------------------------------------------------------------------
    // getLongProperty — numeric properties
    // -----------------------------------------------------------------------

    @Test
    void getLongProperty_estimateNumKeys_isPresent(@TempDir Path dir) {
        // Given
        try (var db = RocksDB.open(dir)) {
            db.put("k1".getBytes(), "v1".getBytes());
            db.put("k2".getBytes(), "v2".getBytes());

            // When
            OptionalLong keys = db.getLongProperty(Property.ESTIMATE_NUM_KEYS);

            // Then — estimate may be 0 before flush, but must be non-negative
            assertThat(keys).isPresent();
            assertThat(keys.getAsLong()).isGreaterThanOrEqualTo(0);
        }
    }

    @Test
    void getLongProperty_numRunningFlushes_isPresent(@TempDir Path dir) {
        try (var db = RocksDB.open(dir)) {
            OptionalLong flushes = db.getLongProperty(Property.NUM_RUNNING_FLUSHES);

            assertThat(flushes).isPresent();
            assertThat(flushes.getAsLong()).isGreaterThanOrEqualTo(0);
        }
    }

    @Test
    void getLongProperty_numRunningCompactions_isPresent(@TempDir Path dir) {
        try (var db = RocksDB.open(dir)) {
            assertThat(db.getLongProperty(Property.NUM_RUNNING_COMPACTIONS)).isPresent();
        }
    }

    @Test
    void getLongProperty_numSnapshots_incrementsWithSnapshot(@TempDir Path dir) {
        // Given
        try (var db = RocksDB.open(dir)) {
            long before = db.getLongProperty(Property.NUM_SNAPSHOTS).orElseThrow();

            // When
            try (Snapshot _ = db.getSnapshot()) {
                long during = db.getLongProperty(Property.NUM_SNAPSHOTS).orElseThrow();

                // Then
                assertThat(during).isEqualTo(before + 1);
            }

            // After close
            long after = db.getLongProperty(Property.NUM_SNAPSHOTS).orElseThrow();
            assertThat(after).isEqualTo(before);
        }
    }

    // -----------------------------------------------------------------------
    // TransactionDB
    // -----------------------------------------------------------------------

    @Test
    void transactionDB_getProperty_stats_isPresent(@TempDir Path dir) {
        // Given
        try (var txnDbOpts = new TransactionDBOptions();
             var opts = new Options().setCreateIfMissing(true);
             var db = TransactionDB.open(opts, txnDbOpts, dir)) {

            // When
            Optional<String> stats = db.getProperty(Property.STATS);

            // Then
            assertThat(stats).isPresent();
            assertThat(stats.get()).contains("Compaction Stats");
        }
    }

    @Test
    void transactionDB_getLongProperty_estimateNumKeys(@TempDir Path dir) {
        // Given
        try (var txnDbOpts = new TransactionDBOptions();
             var opts = new Options().setCreateIfMissing(true);
             var db = TransactionDB.open(opts, txnDbOpts, dir)) {

            db.put("k".getBytes(), "v".getBytes());

            // When / Then
            OptionalLong keys = db.getLongProperty(Property.ESTIMATE_NUM_KEYS);
            assertThat(keys).isPresent();
            assertThat(keys.getAsLong()).isGreaterThanOrEqualTo(0);
        }
    }
}
