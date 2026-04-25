package io.github.dfa1.rocksdbffm;

/// Unchecked exception thrown when a RocksDB native operation fails.
public class RocksDBException extends RuntimeException {

	/// Constructs an exception with the given message.
	///
	/// @param message description of the failure
	public RocksDBException(String message) {
		super(message);
	}

	/// Constructs an exception with the given message and cause.
	///
	/// @param message description of the failure
	/// @param cause   the underlying cause
	public RocksDBException(String message, Throwable cause) {
		super(message, cause);
	}

	/// Re-throws `t` as-is if it is already a [RocksDBException],
	/// otherwise wraps it. Use at the bottom of every `catch (Throwable t)` block:
	/// <pre>
	/// <code><jbr-internal-inline></jbr-internal-inline></code> catch (Throwable t) {
	///     throw RocksDBException.wrap("operation failed", t);
	/// }
	/// }
	/// </pre>
	///
	/// @param message description used if `t` is not already a [RocksDBException]
	/// @param t       the throwable to promote or wrap
	/// @return `t` cast to [RocksDBException], or a new one wrapping `t`
	public static RocksDBException wrap(String message, Throwable t) {
		return (t instanceof RocksDBException r) ? r : new RocksDBException(message, t);
	}
}
