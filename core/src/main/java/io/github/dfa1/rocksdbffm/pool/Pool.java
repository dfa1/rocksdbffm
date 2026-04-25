package io.github.dfa1.rocksdbffm.pool;

/// Generic resource pool.
///
/// @param <T> the pooled resource type
public interface Pool<T> extends AutoCloseable {

	/// Acquires a resource from the pool.
	///
	/// @return an available resource
	T acquire();

	/// Returns a previously acquired resource back to the pool.
	///
	/// @param resource the resource to release
	void release(T resource);

	/// Returns the maximum number of resources in the pool.
	///
	/// @return pool capacity
	int capacity();

	/// Returns the number of resources currently available for acquisition.
	///
	/// @return available resource count
	int available();

	@Override
	void close();
}
