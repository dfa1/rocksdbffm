package io.github.dfa1.rocksdbffm;

/// Compression algorithms supported by the RocksDB C API.
///
/// Integer values match the `rocksdb_*_compression` constants in `rocksdb/c.h`.
///
/// Not every algorithm is available in every RocksDB build.
///
/// ```
/// try (Options opts = Options.newOptions().setCompression(CompressionType.LZ4)) { ... }
/// ```
public enum CompressionType {

	/// No compression. Always supported.
	NO_COMPRESSION(0),

	/// Snappy compression.
	/// TODO: not supported yet
	SNAPPY(1),

	/// zlib compression.
	/// TODO: not supported yet
	ZLIB(2),

	/// bzip2 compression.
	/// TODO: not supported yet
	BZLIB2(3),

	/// LZ4 compression.
	LZ4(4),

	/// LZ4HC (high-compression) compression.
	LZ4HC(5),

	/// Express compression (Windows only).
	/// TODO: not supported yet
	XPRESS(6),

	/// Zstandard compression.
	ZSTD(7);

	/// C API integer constant — package-private for use by [Options].
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
