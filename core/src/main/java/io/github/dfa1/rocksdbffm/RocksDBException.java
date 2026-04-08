package io.github.dfa1.rocksdbffm;

public class RocksDBException extends RuntimeException {

	public RocksDBException(String message) {
		super(message);
	}

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
	public static RocksDBException wrap(String message, Throwable t) {
		return (t instanceof RocksDBException r) ? r : new RocksDBException(message, t);
	}
}
