package io.github.dfa1.rocksdbffm;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

/**
 * Compression algorithms supported by the RocksDB C API.
 *
 * <p>Integer values match the {@code rocksdb_*_compression} constants in {@code rocksdb/c.h}.
 *
 * <p>Not every algorithm is available in every RocksDB build — use
 * {@link #getSupportedTypes()} to query which types are compiled in at runtime.
 *
 * <pre>{@code
 * // Configure compression
 * try (Options opts = new Options().setCompression(CompressionType.LZ4)) { ... }
 *
 * // Discover what's available
 * Set<CompressionType> supported = CompressionType.getSupportedTypes();
 * }</pre>
 */
public enum CompressionType {

    /** No compression. Always supported. */
    NO_COMPRESSION(0),

    /** Snappy compression. */
    SNAPPY(1),

    /** zlib compression. */
    ZLIB(2),

    /** bzip2 compression. */
    BZLIB2(3),

    /** LZ4 compression. */
    LZ4(4),

    /** LZ4HC (high-compression) compression. */
    LZ4HC(5),

    /** Xpress compression (Windows only). */
    XPRESS(6),

    /** Zstandard compression. */
    ZSTD(7);

    /** C API integer constant — package-private for use by {@link Options}. */
    final int value;

    CompressionType(int value) {
        this.value = value;
    }

    static CompressionType fromValue(int value) {
        for (CompressionType t : values()) {
            if (t.value == value) return t;
        }
        throw new IllegalArgumentException("Unknown compression type value: " + value);
    }

    // -----------------------------------------------------------------------
    // Runtime support probe
    // -----------------------------------------------------------------------

    private static final Set<CompressionType> SUPPORTED = probeSupported();

    /**
     * Returns the set of compression types compiled into the loaded RocksDB library.
     *
     * <p>{@link #NO_COMPRESSION} is always included. The result is cached after the
     * first call (probing occurs once at class-load time).
     */
    public static Set<CompressionType> getSupportedTypes() {
        return SUPPORTED;
    }

    private static Set<CompressionType> probeSupported() {
        Set<CompressionType> result = EnumSet.of(NO_COMPRESSION);

        Path tmpDir = null;
        try {
            tmpDir = Files.createTempDirectory("rocksdbffm-compress-probe-");
            for (CompressionType type : values()) {
                if (type == NO_COMPRESSION) continue;
                if (probe(type, tmpDir)) {
                    result.add(type);
                }
            }
        } catch (IOException ignored) {
            // If we can't create a temp dir, return only NO_COMPRESSION.
        } finally {
            if (tmpDir != null) deleteQuietly(tmpDir);
        }

        return Collections.unmodifiableSet(result);
    }

    private static boolean probe(CompressionType type, Path tmpDir) {
        Path dbPath = tmpDir.resolve(type.name().toLowerCase());
        try (Options opts = new Options().setCreateIfMissing(true).setCompression(type);
             RocksDB db = RocksDB.open(opts, dbPath);
             FlushOptions fo = new FlushOptions().setWait(true)) {
            db.put(new byte[]{0}, new byte[]{0});
            db.flush(fo);
            return true;
        } catch (RocksDBException ignored) {
            return false;
        } finally {
            deleteQuietly(dbPath);
        }
    }

    private static void deleteQuietly(Path path) {
        try {
            if (Files.isDirectory(path)) {
                try (var stream = Files.list(path)) {
                    stream.forEach(CompressionType::deleteQuietly);
                }
            }
            Files.deleteIfExists(path);
        } catch (IOException ignored) {
            // best effort
        }
    }
}
