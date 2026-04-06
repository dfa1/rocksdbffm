package io.github.dfa1.rocksdbffm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CompressionTypeTest {

    // -----------------------------------------------------------------------
    // getSupportedTypes
    // -----------------------------------------------------------------------

    @Test
    void getSupportedTypes_alwaysContainsNoCompression() {
        // Given / When
        Set<CompressionType> supported = CompressionType.getSupportedTypes();

        // Then
        assertThat(supported).contains(CompressionType.NO_COMPRESSION);
    }

    @Test
    void getSupportedTypes_isNonEmpty() {
        assertThat(CompressionType.getSupportedTypes()).isNotEmpty();
    }

    @Test
    void getSupportedTypes_isStable() {
        // Two calls return the same set (cached)
        assertThat(CompressionType.getSupportedTypes())
            .isEqualTo(CompressionType.getSupportedTypes());
    }

    @Test
    void getSupportedTypes_isUnmodifiable() {
        Set<CompressionType> supported = CompressionType.getSupportedTypes();
        assertThatThrownBy(() -> supported.add(CompressionType.SNAPPY))
            .isInstanceOf(UnsupportedOperationException.class);
    }

    // -----------------------------------------------------------------------
    // Options integration
    // -----------------------------------------------------------------------

    @Test
    void options_setCompression_roundTrips() {
        try (Options opts = new Options()) {
            // When
            opts.setCompression(CompressionType.NO_COMPRESSION);
            // Then
            assertThat(opts.getCompression()).isEqualTo(CompressionType.NO_COMPRESSION);
        }
    }

    @Test
    void options_setCompression_chaining() {
        try (Options opts = new Options()
                .setCreateIfMissing(true)
                .setCompression(CompressionType.NO_COMPRESSION)) {
            assertThat(opts.getCompression()).isEqualTo(CompressionType.NO_COMPRESSION);
        }
    }

    @Test
    void openDb_withSupportedCompression_writesAndReadsBack(@TempDir Path dir) {
        // Given — NO_COMPRESSION is always supported
        try (Options opts = new Options()
                .setCreateIfMissing(true)
                .setCompression(CompressionType.NO_COMPRESSION);
             RocksDB db = RocksDB.open(opts, dir)) {
            // When
            db.put("k".getBytes(), "v".getBytes());

            // Then
            assertThat(db.get("k".getBytes())).isEqualTo("v".getBytes());
        }
    }

    @Test
    void openDb_withEachSupportedCompression_writesAndReadsBack(@TempDir Path dir) {
        // For every compression type that the library was built with, open a DB,
        // write a key, and read it back.
        int i = 0;
        for (CompressionType type : CompressionType.getSupportedTypes()) {
            Path dbPath = dir.resolve("db-" + i++);
            try (Options opts = new Options()
                    .setCreateIfMissing(true)
                    .setCompression(type);
                 RocksDB db = RocksDB.open(opts, dbPath)) {
                db.put("key".getBytes(), "val".getBytes());
                assertThat(db.get("key".getBytes()))
                    .as("compression=%s", type)
                    .isEqualTo("val".getBytes());
            }
        }
    }
}
