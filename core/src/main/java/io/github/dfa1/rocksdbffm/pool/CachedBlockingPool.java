package io.github.dfa1.rocksdbffm.pool;

// this will cache the most recent acquisition in a thread local
// use with care: the assumption is that acquire/release cycle happens on same thread
public final class CachedBlockingPool<T> implements Pool<T> {

    private final Pool<T> delegate;
    private final ThreadLocal<T> local;

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