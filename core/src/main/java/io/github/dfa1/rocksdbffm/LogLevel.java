package io.github.dfa1.rocksdbffm;

/// Log verbosity levels, matching RocksDB's `InfoLogLevel` enum.
///
/// Passed to [Logger] factory methods and [Options#setInfoLogLevel].
public enum LogLevel {
	DEBUG(0),
	INFO(1),
	WARN(2),
	ERROR(3),
	FATAL(4),
	HEADER(5);

	final int value;

	LogLevel(int value) {
		this.value = value;
	}

	static LogLevel fromValue(int v) {
		return switch (v) {
			case 0 -> DEBUG;
			case 1 -> INFO;
			case 2 -> WARN;
			case 3 -> ERROR;
			case 4 -> FATAL;
			case 5 -> HEADER;
			default -> INFO;
		};
	}
}
