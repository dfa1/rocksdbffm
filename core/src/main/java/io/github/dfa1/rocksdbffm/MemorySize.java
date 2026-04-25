package io.github.dfa1.rocksdbffm;

/// Immutable value object representing a non-negative quantity of memory.
///
/// Use the named factory methods to make the unit explicit at the call site:
///
/// ```
/// new LRUCache(MemorySize.ofMB(64))          // unambiguous
/// tbl.setBlockSize(MemorySize.ofKB(16))      // no raw-long guesswork
/// ```
///
/// The constructor rejects negative values at construction time — an invalid
/// `MemorySize` cannot be created and therefore cannot be passed anywhere.
/// This follows the _parse, don't validate_ principle: constraints are
/// structural, not procedural.
public final class MemorySize implements Comparable<MemorySize> {

	private static final long KB = 1024L;
	private static final long MB = 1024L * KB;
	private static final long GB = 1024L * MB;

	/// Convenience constant representing zero bytes; pass to APIs that accept an optional size hint.
	public static final MemorySize ZERO = new MemorySize(0);

	private final long bytes;

	private MemorySize(long bytes) {
		if (bytes < 0) {
			throw new IllegalArgumentException("MemorySize cannot be negative: " + bytes);
		}
		this.bytes = bytes;
	}

	// -----------------------------------------------------------------------
	// Named factories — unit is always explicit
	// -----------------------------------------------------------------------

	/// Creates a [MemorySize] from an exact byte count.
	///
	/// @param bytes non-negative byte count
	/// @return a new [MemorySize] representing the given number of bytes
	public static MemorySize ofBytes(long bytes) {
		return new MemorySize(bytes);
	}

	/// Creates a [MemorySize] from a kilobyte count (1 KB = 1024 bytes).
	///
	/// @param kilobytes non-negative kilobyte count
	/// @return a new [MemorySize] representing the given number of kilobytes
	public static MemorySize ofKB(long kilobytes) {
		return new MemorySize(Math.multiplyExact(kilobytes, KB));
	}

	/// Creates a [MemorySize] from a megabyte count (1 MB = 1024 KB).
	///
	/// @param megabytes non-negative megabyte count
	/// @return a new [MemorySize] representing the given number of megabytes
	public static MemorySize ofMB(long megabytes) {
		return new MemorySize(Math.multiplyExact(megabytes, MB));
	}

	/// Creates a [MemorySize] from a gigabyte count (1 GB = 1024 MB).
	///
	/// @param gigabytes non-negative gigabyte count
	/// @return a new [MemorySize] representing the given number of gigabytes
	public static MemorySize ofGB(long gigabytes) {
		return new MemorySize(Math.multiplyExact(gigabytes, GB));
	}

	// -----------------------------------------------------------------------
	// Accessors
	// -----------------------------------------------------------------------

	/// Returns the value in bytes, for passing to native calls.
	///
	/// @return the size in bytes
	public long toBytes() {
		return bytes;
	}

	/// Returns the sum of this size and `other`.
	///
	/// @param other the size to add
	/// @return a new [MemorySize] representing the combined byte count
	public MemorySize add(MemorySize other) {
		return new MemorySize(Math.addExact(this.bytes, other.bytes));
	}

	// -----------------------------------------------------------------------
	// Value semantics
	// -----------------------------------------------------------------------

	@Override
	public boolean equals(Object obj) {
		return obj instanceof MemorySize other && this.bytes == other.bytes;
	}

	@Override
	public int hashCode() {
		return Long.hashCode(bytes);
	}

	/// Returns a human-readable string in the largest unit that divides evenly,
	/// e.g. `"64 MB"`, `"16 KB"`, `"1536 B"`.
	@Override
	public String toString() {
		if (bytes == 0) {
			return "0 B";
		}
		if (bytes % GB == 0) {
			return (bytes / GB) + " GB";
		}
		if (bytes % MB == 0) {
			return (bytes / MB) + " MB";
		}
		if (bytes % KB == 0) {
			return (bytes / KB) + " KB";
		}
		return bytes + " B";
	}

	@Override
	public int compareTo(MemorySize other) {
		return Long.compare(this.bytes, other.bytes);
	}
}
