package io.github.dfa1.rocksdbffm;

public class RocksDBException extends RuntimeException {

	public RocksDBException(String message) {
		super(message);
	}

	public RocksDBException(String message, Throwable cause) {
		super(message, cause);
	}

	/**
	 * Re-throws {@code t} as-is if it is already a {@link RocksDBException},
	 * otherwise wraps it. Use at the bottom of every {@code catch (Throwable t)} block:
	 * <pre>{@code
	 * } catch (Throwable t) {
	 *     throw RocksDBException.wrap("operation failed", t);
	 * }
	 * }</pre>
	 */
	public static RocksDBException wrap(String message, Throwable t) {
		return (t instanceof RocksDBException r) ? r : new RocksDBException(message, t);
	}
}
