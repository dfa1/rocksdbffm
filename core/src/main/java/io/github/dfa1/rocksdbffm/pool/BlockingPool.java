package io.github.dfa1.rocksdbffm.pool;

import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;

/// Bounded pool backed by a [java.util.concurrent.LinkedBlockingQueue].
/// Throws [IllegalStateException] on over-acquire or double-release.
///
/// @param <T> the pooled resource type
public final class BlockingPool<T> implements Pool<T> {

	private final int capacity;
	private final LinkedBlockingQueue<T> queue;

	/// Creates a pool of `capacity` resources produced by `factory`.
	///
	/// @param capacity maximum number of pooled resources
	/// @param factory  supplier called once per slot to pre-populate the pool
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
