package io.github.dfa1.rocksdbffm;

/// Controls whether blob values are pre-populated into the blob cache on write.
///
/// Matches the `rocksdb_prepopulate_blob_*` constants in `rocksdb/c.h`.
public enum PrepopulateBlobCache {

	/// Never pre-populate the blob cache (default).
	DISABLE(0),

	/// Pre-populate the blob cache only during flush (memtable → SST).
	FLUSH_ONLY(1);

	final int value;

	PrepopulateBlobCache(int value) {
		this.value = value;
	}

	static PrepopulateBlobCache fromValue(int value) {
		for (PrepopulateBlobCache v : values()) {
			if (v.value == value) {
				return v;
			}
		}
		throw new IllegalArgumentException("Unknown PrepopulateBlobCache value: " + value);
	}
}
