package io.github.dfa1.rocksdbffm.pool;

/// [Pool] wrapper that caches the most recently released resource in a [ThreadLocal].
///
/// Acquire/release cycles must happen on the same thread. Use with care.
///
/// @param <T> the pooled resource type
public final class CachedBlockingPool<T> implements Pool<T> {

	private final Pool<T> delegate;
	private final ThreadLocal<T> local;

	/// Wraps `delegate` with a per-thread cache.
	///
	/// @param delegate underlying pool to delegate to on cache miss
	public CachedBlockingPool(Pool<T> delegate) {
		this.delegate = delegate;
		this.local = new ThreadLocal<>();
	}

	@Override
	public T acquire() {
		T cached = local.get();
		if (cached != null) {
			local.remove();
			return cached;
		}
		return delegate.acquire();
	}

	@Override
	public void release(T resource) {
		if (local.get() == null) {
			local.set(resource);
		} else {
			delegate.release(resource);
		}
	}

	@Override
	public int capacity() {
		return delegate.capacity();
	}

	@Override
	public int available() {
		return delegate.available();
	}

	@Override
	public void close() {
		// TODO: ThreadLocal could leak
		delegate.close();
	}
}
