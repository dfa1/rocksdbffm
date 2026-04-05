package io.github.dfa1.rocksdbffm;

/**
 * Immutable value object representing a non-negative quantity of memory.
 *
 * <p>Use the named factory methods to make the unit explicit at the call site:
 * <pre>{@code
 * new LRUCache(MemorySize.ofMB(64))          // unambiguous
 * tbl.setBlockSize(MemorySize.ofKB(16))      // no raw-long guesswork
 * }</pre>
 *
 * <p>The constructor rejects negative values at construction time — an invalid
 * {@code MemorySize} cannot be created and therefore cannot be passed anywhere.
 * This follows the <em>parse, don't validate</em> principle: constraints are
 * structural, not procedural.
 */
public final class MemorySize implements Comparable<MemorySize> {

    private static final long KB = 1024L;
    private static final long MB = 1024L * KB;
    private static final long GB = 1024L * MB;

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

    public static MemorySize ofBytes(long bytes) {
        return new MemorySize(bytes);
    }

    public static MemorySize ofKB(long kilobytes) {
        return new MemorySize(Math.multiplyExact(kilobytes, KB));
    }

    public static MemorySize ofMB(long megabytes) {
        return new MemorySize(Math.multiplyExact(megabytes, MB));
    }

    public static MemorySize ofGB(long gigabytes) {
        return new MemorySize(Math.multiplyExact(gigabytes, GB));
    }

    // -----------------------------------------------------------------------
    // Accessors
    // -----------------------------------------------------------------------

    /** Returns the value in bytes, for passing to native calls. */
    public long toBytes() {
        return bytes;
    }

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

    /**
     * Returns a human-readable string in the largest unit that divides evenly,
     * e.g. {@code "64 MB"}, {@code "16 KB"}, {@code "1536 B"}.
     */
    @Override
    public String toString() {
        if (bytes == 0) return "0 B";
        if (bytes % GB == 0) return (bytes / GB) + " GB";
        if (bytes % MB == 0) return (bytes / MB) + " MB";
        if (bytes % KB == 0) return (bytes / KB) + " KB";
        return bytes + " B";
    }

    @Override
    public int compareTo(MemorySize other) {
        return Long.compare(this.bytes, other.bytes);
    }
}
