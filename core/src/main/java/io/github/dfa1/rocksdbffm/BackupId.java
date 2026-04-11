package io.github.dfa1.rocksdbffm;

/// Immutable value object representing a RocksDB backup ID.
///
/// Backup IDs are monotonically increasing unsigned 32-bit integers assigned
/// by the backup engine. The value is stored as a `long` to avoid sign
/// confusion with Java's signed `int`.
///
/// ```
/// BackupEngine engine = BackupEngine.open(opts, backupPath);
/// engine.createNewBackup(db);
/// List<BackupInfo> infos = engine.getBackupInfo();
/// BackupId latest = infos.getLast().backupId();
/// engine.verifyBackup(latest);
/// ```
public final class BackupId implements Comparable<BackupId> {

	private final long value;

	private BackupId(long value) {
		if (value < 0 || value > 0xFFFFFFFFL) {
			throw new IllegalArgumentException("BackupId out of uint32_t range: " + value);
		}
		this.value = value;
	}

	/// Creates a [BackupId] from a Java `long`.
	///
	/// @throws IllegalArgumentException if `value` is negative or exceeds `0xFFFFFFFF`
	public static BackupId of(long value) {
		return new BackupId(value);
	}

	/// Creates a [BackupId] from a native `uint32_t` returned as a Java `int`.
	///
	/// Used internally when reading IDs from the native layer via FFM.
	static BackupId fromNative(int nativeValue) {
		return new BackupId(Integer.toUnsignedLong(nativeValue));
	}

	/// Returns the ID as a Java `int` for passing to native FFM calls.
	///
	/// Used internally; the bit pattern matches the original `uint32_t`.
	int toNativeInt() {
		return (int) value;
	}

	/// Returns the raw value as a `long`.
	public long toLong() {
		return value;
	}

	@Override
	public int compareTo(BackupId other) {
		return Long.compareUnsigned(this.value, other.value);
	}

	@Override
	public boolean equals(Object obj) {
		return obj instanceof BackupId other && this.value == other.value;
	}

	@Override
	public int hashCode() {
		return Long.hashCode(value);
	}

	@Override
	public String toString() {
		return "BackupId(" + value + ")";
	}
}
