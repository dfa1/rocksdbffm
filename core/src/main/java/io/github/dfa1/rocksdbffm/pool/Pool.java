package io.github.dfa1.rocksdbffm.pool;

public interface Pool<T> extends AutoCloseable {

    T acquire();

    void release(T resource);

    int capacity();

    int available();

    @Override
    void close();
}