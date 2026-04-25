package io.github.dfa1.rocksdbffm;

/// Immutable value object representing a RocksDB sequence number.
///
/// Every write to RocksDB is stamped with a monotonically increasing sequence
/// number. Snapshots expose their sequence number so callers can reason about
/// relative ordering of database states:
///
/// ```
/// try (Snapshot s1 = db.getSnapshot()) {
///     db.put(key, value);
///     try (Snapshot s2 = db.getSnapshot()) {
///         assert s2.sequenceNumber().isAfter(s1.sequenceNumber());
///     }
/// }
/// ```
public final class SequenceNumber implements Comparable<SequenceNumber> {

	private final long value;

	private SequenceNumber(long value) {
		if (value < 0) {
			throw new IllegalArgumentException("SequenceNumber cannot be negative: " + value);
		}
		this.value = value;
	}

	/// Wraps a raw sequence number returned by the native API.
	///
	/// @param value the raw `uint64_t` sequence number
	/// @return a new [SequenceNumber] wrapping the given value
	public static SequenceNumber of(long value) {
		return new SequenceNumber(value);
	}

	/// Returns the raw `uint64_t` value.
	///
	/// @return the raw sequence number as a signed `long` (interpret unsigned)
	public long toLong() {
		return value;
	}

	/// Returns true if this sequence number is strictly after `other`.
	///
	/// @param other the sequence number to compare against
	/// @return `true` if this sequence number is greater than `other`
	public boolean isAfter(SequenceNumber other) {
		return this.value > other.value;
	}

	/// Returns true if this sequence number is strictly before `other`.
	///
	/// @param other the sequence number to compare against
	/// @return `true` if this sequence number is less than `other`
	public boolean isBefore(SequenceNumber other) {
		return this.value < other.value;
	}

	@Override
	public int compareTo(SequenceNumber other) {
		return Long.compareUnsigned(this.value, other.value);
	}

	@Override
	public boolean equals(Object obj) {
		return obj instanceof SequenceNumber other && this.value == other.value;
	}

	@Override
	public int hashCode() {
		return Long.hashCode(value);
	}

	@Override
	public String toString() {
		return "SequenceNumber(" + value + ")";
	}
}
