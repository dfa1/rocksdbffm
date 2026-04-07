package io.github.dfa1.rocksdbffm.pool;

import java.util.function.Supplier;

public final class UnpooledPool<T> implements Pool<T> {

	private final Supplier<T> supplier;

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
