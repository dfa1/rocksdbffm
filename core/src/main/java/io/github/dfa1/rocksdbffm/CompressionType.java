package io.github.dfa1.rocksdbffm;

/**
 * Compression algorithms supported by the RocksDB C API.
 *
 * <p>Integer values match the {@code rocksdb_*_compression} constants in {@code rocksdb/c.h}.
 *
 * <p>Not every algorithm is available in every RocksDB build — use
 * {@link RocksDB#getSupportedCompressions()} to query which types are compiled in at runtime.
 *
 * <pre>{@code
 * try (Options opts = Options.newOptions().setCompression(CompressionType.LZ4)) { ... }
 * }</pre>
 */
public enum CompressionType {

	/**
	 * No compression. Always supported.
	 */
	NO_COMPRESSION(0),

	/**
	 * Snappy compression.
	 */
	SNAPPY(1),

	/**
	 * zlib compression.
	 */
	ZLIB(2),

	/**
	 * bzip2 compression.
	 */
	BZLIB2(3),

	/**
	 * LZ4 compression.
	 */
	LZ4(4),

	/**
	 * LZ4HC (high-compression) compression.
	 */
	LZ4HC(5),

	/**
	 * Xpress compression (Windows only).
	 */
	XPRESS(6),

	/**
	 * Zstandard compression.
	 */
	ZSTD(7);

	/**
	 * C API integer constant — package-private for use by {@link Options}.
	 */
	final int value;

	CompressionType(int value) {
		this.value = value;
	}

	static CompressionType fromValue(int value) {
		for (CompressionType t : values()) {
			if (t.value == value) {
				return t;
			}
		}
		throw new IllegalArgumentException("Unknown compression type value: " + value);
	}
}
