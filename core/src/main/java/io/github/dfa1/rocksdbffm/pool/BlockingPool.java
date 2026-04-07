package io.github.dfa1.rocksdbffm.pool;

import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;

public final class BlockingPool<T> implements Pool<T> {

	private final int capacity;
	private final LinkedBlockingQueue<T> queue;

	public BlockingPool(int capacity, Supplier<T> factory) {
		this.capacity = capacity;
		this.queue = new LinkedBlockingQueue<>(capacity);
		for (int i = 0; i < capacity; i++) {
			queue.add(factory.get());
		}
	}

	@Override
	public T acquire() {
		T resource = queue.poll();
		if (resource == null) {
			throw new IllegalStateException("pool exhausted");
		}
		return resource;
	}

	@Override
	public void release(T resource) {
		Objects.requireNonNull(resource, "resource");
		boolean accepted = queue.offer(resource);
		if (!accepted) {
			throw new IllegalStateException("double release detected: pool already at capacity " + capacity);
		}
	}

	@Override
	public int capacity() {
		return capacity;
	}

	@Override
	public int available() {
		return queue.size();
	}

	@Override
	public void close() {
		// release memory
		queue.clear();
	}
}
