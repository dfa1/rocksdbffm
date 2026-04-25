package io.github.dfa1.rocksdbffm;

/// Statistics levels for RocksDB.
public enum StatsLevel {
	/// Disable all statistics collection.
	DISABLE_ALL(0),
	/// Collect all statistics except ticker types.
	EXCEPT_TICKERS(0),
	/// Collect all statistics except histograms and timers.
	EXCEPT_HISTOGRAM_OR_TIMERS(1),
	/// Collect all statistics except timer metrics.
	EXCEPT_TIMERS(2),
	/// Collect all statistics except detailed timers.
	EXCEPT_DETAILED_TIMERS(3),
	/// Collect all statistics except mutex wait time.
	EXCEPT_TIME_FOR_MUTEX(4),
	/// Collect all available statistics.
	ALL(5);

	private final int value;

	StatsLevel(int value) {
		this.value = value;
	}

	// don't expose this
	int getValue() {
		return value;
	}
}
