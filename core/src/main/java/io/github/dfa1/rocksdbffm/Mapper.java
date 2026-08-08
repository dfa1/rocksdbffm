package io.github.dfa1.rocksdbffm;

import java.lang.foreign.MemorySegment;

/// Callback invoked by scoped zero-copy reads with a view of a pinned value.
///
/// The segment is only valid for the duration of [#map(MemorySegment)]; it is bound to
/// an arena that is closed as soon as this method returns, so it — and any view derived
/// from it — must not be retained past the call.
///
/// @param <R> the type produced from mapping the pinned value
@FunctionalInterface
public interface Mapper<R> {

	/// Maps `value` to a result. Every caller in this codebase rejects a `null` return
	/// with `NullPointerException` rather than accepting it silently.
	///
	/// @param value zero-copy, read-only view of the pinned value
	/// @return the value produced from `value`; must not be `null`
	R map(MemorySegment value);
}
