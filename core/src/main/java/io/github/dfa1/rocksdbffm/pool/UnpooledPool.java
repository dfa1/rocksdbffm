package io.github.dfa1.rocksdbffm.pool;

import java.util.function.Supplier;

/// A [Pool] implementation that creates a new instance on every [#acquire()] call.
/// [#release(Object)] is a no-op; instances are not reused.
///
/// @param <T> the type of pooled instances
public final class UnpooledPool<T> implements Pool<T> {

	private final Supplier<T> supplier;

	/// Creates an unpooled pool backed by the given supplier.
	///
	/// @param supplier factory called on every [#acquire()]
	public UnpooledPool(Supplier<T> supplier) {
		this.supplier = supplier;
	}

	@Override
	public T acquire() {
		return supplier.get();
	}

	@Override
	public void release(T slot) {
		// no-op: arena owns all segments, freed on close
	}

	@Override
	public int capacity() {
		return Integer.MAX_VALUE;
	}

	@Override
	public int available() {
		return Integer.MAX_VALUE;
	}

	@Override
	public void close() {
		// no-op
	}
}
