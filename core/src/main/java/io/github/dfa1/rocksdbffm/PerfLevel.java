package io.github.dfa1.rocksdbffm;

/// Controls how much instrumentation [PerfContext] collects.
///
/// Set via [PerfContext#setPerfLevel(PerfLevel)]. Higher levels collect more
/// data at the cost of additional overhead.
///
/// ```
/// PerfContext.setPerfLevel(PerfLevel.ENABLE_COUNT);
/// try (PerfContext ctx = PerfContext.newPerfContext()) {
///     db.get("key".getBytes());
///     long hits = ctx.metric(PerfMetric.BLOCK_CACHE_HIT_COUNT);
/// }
/// ```
public enum PerfLevel {

	/// Not yet set — behaves like [#DISABLE].
	UNINITIALIZED(0),
	/// All instrumentation disabled. Default.
	DISABLE(1),
	/// Counts only (no timing). Very low overhead.
	ENABLE_COUNT(2),
	/// Counts + wall-clock timings, except for mutex waits.
	ENABLE_TIME_EXCEPT_FOR_MUTEX(3),
	/// Counts + all wall-clock timings including mutex waits.
	ENABLE_TIME(4);

	final int value;

	PerfLevel(int value) {
		this.value = value;
	}

	static PerfLevel fromValue(int value) {
		for (PerfLevel level : values()) {
			if (level.value == value) {
				return level;
			}
		}
		throw new IllegalArgumentException("Unknown PerfLevel value: " + value);
	}
}
