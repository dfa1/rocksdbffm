package io.github.dfa1.rocksdbffm;

/// Log verbosity levels, matching RocksDB's `InfoLogLevel` enum.
///
/// Passed to [Logger] factory methods and [Options#setInfoLogLevel].
public enum LogLevel {
	/// Verbose debug output; very high volume.
	DEBUG(0),
	/// Informational messages about normal operation.
	INFO(1),
	/// Potentially harmful situations that do not stop the DB.
	WARN(2),
	/// Error conditions that may affect correctness.
	ERROR(3),
	/// Fatal errors; the DB will likely be unusable after this.
	FATAL(4),
	/// Header lines emitted at startup that summarise configuration.
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
