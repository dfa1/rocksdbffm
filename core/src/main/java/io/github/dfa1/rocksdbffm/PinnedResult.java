package io.github.dfa1.rocksdbffm;

import java.lang.foreign.MemorySegment;

/// Outcome of a pinned read: either the value was found, or the key is absent.
///
/// This replaces the `-1`-or-length encoding used by the copying `get` overloads.
/// Absence is a case of the type rather than a magic number, so it cannot be
/// mistaken for a length, and a `switch` over the result is checked by the compiler:
///
/// ```java
/// try (PinnedResult result = db.getPinned(key)) {
///     switch (result) {
///         case PinnedResult.Found found -> consume(found.value());
///         case PinnedResult.NotFound ignored -> handleMiss();
///     }
/// }
/// ```
///
/// A found result holds a block-cache entry pinned until it is closed, so the
/// result is [AutoCloseable] and belongs in a try-with-resources. Closing a
/// [NotFound] is harmless, which is why the whole result — not just the found
/// case — carries the close.
///
/// @see PinnableSlice
public sealed interface PinnedResult extends AutoCloseable {

	/// Releases the pinned block-cache entry, if any.
	///
	/// Unlike [AutoCloseable#close()] this throws no checked exception, and it is
	/// idempotent: closing twice is a no-op rather than a double free.
	@Override
	void close();

	/// The key was present; [#value()] borrows its value from the block cache.
	///
	/// @param slice the pinned slice holding the value; closed when this result is closed
	record Found(PinnableSlice slice) implements PinnedResult {

		/// Returns the value as a borrowed segment whose `byteSize()` is its length.
		///
		/// The segment is valid only until this result is closed; using it afterwards
		/// throws [IllegalStateException].
		///
		/// @return the pinned value, sized to the value's true length
		public MemorySegment value() {
			return slice.value();
		}

		/// Copies the value onto the Java heap.
		///
		/// @return a fresh array holding a copy of the value
		public byte[] toArray() {
			return slice.toArray();
		}

		@Override
		public void close() {
			slice.close();
		}
	}

	/// The key is absent from the database.
	///
	/// A singleton: it carries no state, and in particular no length, so it can
	/// never be read as an empty value. That distinction is what the segment-tier
	/// `get` overloads cannot express.
	enum NotFound implements PinnedResult {

		/// The sole instance.
		INSTANCE;

		@Override
		public void close() {
			// nothing was pinned
		}
	}
}
