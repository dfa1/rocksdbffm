package io.github.dfa1.rocksdbffm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CompressionTypeTest {

    // -----------------------------------------------------------------------
    // getSupportedCompressions
    // -----------------------------------------------------------------------

    @Test
    void getSupportedCompressions_alwaysContainsNoCompression(@TempDir Path dir) {
        try (RocksDB db = RocksDB.open(dir)) {
            assertThat(db.getSupportedCompressions()).contains(CompressionType.NO_COMPRESSION);
        }
    }

    @Test
    void getSupportedCompressions_isNonEmpty(@TempDir Path dir) {
        try (RocksDB db = RocksDB.open(dir)) {
            assertThat(db.getSupportedCompressions()).isNotEmpty();
        }
    }

    @Test
    void getSupportedCompressions_isUnmodifiable(@TempDir Path dir) {
        try (RocksDB db = RocksDB.open(dir)) {
            Set<CompressionType> supported = db.getSupportedCompressions();
            assertThatThrownBy(() -> supported.add(CompressionType.SNAPPY))
                .isInstanceOf(UnsupportedOperationException.class);
        }
    }

    // -----------------------------------------------------------------------
    // Options integration
    // -----------------------------------------------------------------------

    @Test
    void options_setCompression_roundTrips() {
        try (Options opts = new Options()) {
            opts.setCompression(CompressionType.NO_COMPRESSION);
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
        try (Options opts = new Options()
                .setCreateIfMissing(true)
                .setCompression(CompressionType.NO_COMPRESSION);
             RocksDB db = RocksDB.open(opts, dir)) {
            db.put("k".getBytes(), "v".getBytes());
            assertThat(db.get("k".getBytes())).isEqualTo("v".getBytes());
        }
    }

    @Test
    void openDb_withEachSupportedCompression_writesAndReadsBack(@TempDir Path dir) {
        // Given — use a reference DB just to probe support
        Set<CompressionType> supported;
        try (RocksDB probe = RocksDB.open(dir.resolve("probe"))) {
            supported = probe.getSupportedCompressions();
        }

        // When / Then — open a separate DB per compression type
        int i = 0;
        for (CompressionType type : supported) {
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
