package io.github.dfa1.rocksdbffm;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.List;

/// A merge operator, attached to a database or column family via [Options#setMergeOperator(MergeOperator)].
///
/// Two ways to obtain one:
///
/// - [#uint64Add()] — RocksDB's built-in operator: sums 8-byte little-endian `uint64` operands.
///   No callback involved; the cheapest option for a counter.
/// - [#custom(String, FullMergeFn)] — a merge operator implemented in Java, wired through
///   RocksDB's general callback-based `rocksdb_mergeoperator_create`. Use this for anything the
///   built-in doesn't cover (string concatenation, min/max, JSON patching, ...). `fn` receives
///   zero-copy [MemorySegment] views of the key, existing value, and operands rather than copied
///   `byte[]`s — benchmarking (`MergeOperatorBenchmark`, #94) showed `byte[]` copies dominating
///   once operands exceed ~1KB, so there is no separate copying tier to opt out of.
///
/// ```
/// try (var opts = Options.newOptions().setCreateIfMissing(true)
///             .setMergeOperator(MergeOperator.uint64Add());
///      var db = RocksDB.openReadWrite(opts, dbPath)) {
///     db.merge("views".getBytes(), encode(1));
///     db.merge("views".getBytes(), encode(1));
///     long total = decode(db.get("views".getBytes()));   // 2
/// }
/// ```
public sealed interface MergeOperator {

	/// Returns RocksDB's built-in merge operator that sums 8-byte little-endian `uint64` operands.
	///
	/// @return the uint64-add merge operator
	static MergeOperator uint64Add() {
		return new Uint64Add();
	}

	/// Wraps a Java-implemented merge function via RocksDB's general callback-based merge operator.
	///
	/// @param name stable identifier for this operator; RocksDB persists and checks it against the
	///             column family's stored options on every open, so it must not change across runs
	/// @param fn   folds merge operands into a value; see [FullMergeFn] for the threading and
	///             lifetime contract
	/// @return a new merge operator backed by `fn`; caller must pass it to
	/// [Options#setMergeOperator(MergeOperator)] or close it
	static MergeOperator custom(String name, FullMergeFn fn) {
		return Custom.create(name, fn);
	}

	/// Folds RocksDB merge operands into a value, in Java, from zero-copy native views.
	///
	/// Invoked whenever RocksDB needs the merged value for a key — on `get()`, and internally
	/// during flush/compaction — including from RocksDB's own background threads. Implementations
	/// must be thread-safe and must not throw: an exception here is caught and reported to
	/// RocksDB as a merge failure (surfaces as corruption to the caller) rather than crashing
	/// the JVM.
	///
	/// `key`, `existingValue`, and every view in `operands` are read-only and bound to an arena
	/// that is closed as soon as [#fullMerge(MemorySegment, MemorySegment, List)] returns; they
	/// — and any view derived from them — must not be retained past the call, per [Mapper]'s
	/// contract. Copy into a `byte[]` via `segment.toArray(ValueLayout.JAVA_BYTE)` if you need
	/// the data to outlive the call.
	@FunctionalInterface
	interface FullMergeFn {

		/// Folds `operands` (oldest first) into `existingValue`.
		///
		/// @param key           zero-copy view of the key being merged
		/// @param existingValue zero-copy view of the current value, or `null` if the key does
		///                      not exist yet
		/// @param operands      zero-copy views of merge operands queued for this key, oldest first
		/// @return the folded value to store; still copied into a malloc'd buffer once on the way
		/// out, since RocksDB takes ownership of the returned pointer
		byte[] fullMerge(MemorySegment key, MemorySegment existingValue, List<MemorySegment> operands);
	}

	/// RocksDB's built-in `uint64add` merge operator. Stateless — has no native handle of its own,
	/// since `rocksdb_options_set_uint64add_merge_operator` creates and attaches the operator to
	/// the [Options] in a single native call.
	record Uint64Add() implements MergeOperator {

		/// `void rocksdb_options_set_uint64add_merge_operator(rocksdb_options_t*);`
		private static final MethodHandle MH_SET_UINT64ADD_MERGE_OPERATOR = NativeLibrary.lookup(
				"rocksdb_options_set_uint64add_merge_operator",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

		void applyTo(MemorySegment optionsPtr) {
			try {
				MH_SET_UINT64ADD_MERGE_OPERATOR.invokeExact(optionsPtr);
			} catch (Throwable t) {
				throw RocksDB.wrapInvokeFailure("MergeOperator.uint64Add failed", t);
			}
		}
	}

	/// A Java-implemented merge operator, backed by a real `rocksdb_mergeoperator_t*` handle.
	final class Custom extends NativeObject implements MergeOperator {

		/// `rocksdb_mergeoperator_t* rocksdb_mergeoperator_create(void* state, void (*destructor)(void*), char* (*full_merge)(void*, const char* key, size_t key_length, const char* existing_value, size_t existing_value_length, const char* const* operands_list, const size_t* operands_list_length, int num_operands, unsigned char* success, size_t* new_value_length), char* (*partial_merge)(void*, const char* key, size_t key_length, const char* const* operands_list, const size_t* operands_list_length, int num_operands, unsigned char* success, size_t* new_value_length), void (*delete_value)(void*, const char* value, size_t value_length), const char* (*name)(void*));`
		private static final MethodHandle MH_MERGEOPERATOR_CREATE = NativeLibrary.lookup(
				"rocksdb_mergeoperator_create",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS,  // state
						ValueLayout.ADDRESS,  // destructor
						ValueLayout.ADDRESS,  // full_merge
						ValueLayout.ADDRESS,  // partial_merge
						ValueLayout.ADDRESS,  // delete_value
						ValueLayout.ADDRESS)); // name

		/// `void rocksdb_mergeoperator_destroy(rocksdb_mergeoperator_t*);`
		private static final MethodHandle MH_MERGEOPERATOR_DESTROY = NativeLibrary.lookup(
				"rocksdb_mergeoperator_destroy",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

		/// `void rocksdb_options_set_merge_operator(rocksdb_options_t*, rocksdb_mergeoperator_t*);`
		private static final MethodHandle MH_SET_MERGE_OPERATOR = NativeLibrary.lookup(
				"rocksdb_options_set_merge_operator",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		/// libc `void* malloc(size_t size)` — `full_merge`/`partial_merge` hand a malloc'd buffer
		/// back to RocksDB, which copies it out and frees it with plain `free()` (`delete_value` is
		/// passed as `NULL`, see [#create(String, FullMergeFn)]).
		private static final MethodHandle MH_MALLOC = Linker.nativeLinker().downcallHandle(
				Linker.nativeLinker().defaultLookup().find("malloc")
						.orElseThrow(() -> new UnsatisfiedLinkError("Symbol not found: malloc")),
				FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));

		private static final FunctionDescriptor FULL_MERGE_DESC = FunctionDescriptor.of(ValueLayout.ADDRESS,
				ValueLayout.ADDRESS,   // state
				ValueLayout.ADDRESS,   // key
				ValueLayout.JAVA_LONG, // key_length
				ValueLayout.ADDRESS,   // existing_value
				ValueLayout.JAVA_LONG, // existing_value_length
				ValueLayout.ADDRESS,   // operands_list
				ValueLayout.ADDRESS,   // operands_list_length
				ValueLayout.JAVA_INT,  // num_operands
				ValueLayout.ADDRESS,   // success
				ValueLayout.ADDRESS);  // new_value_length

		private static final FunctionDescriptor PARTIAL_MERGE_DESC = FunctionDescriptor.of(ValueLayout.ADDRESS,
				ValueLayout.ADDRESS,   // state
				ValueLayout.ADDRESS,   // key
				ValueLayout.JAVA_LONG, // key_length
				ValueLayout.ADDRESS,   // operands_list
				ValueLayout.ADDRESS,   // operands_list_length
				ValueLayout.JAVA_INT,  // num_operands
				ValueLayout.ADDRESS,   // success
				ValueLayout.ADDRESS);  // new_value_length

		private static final FunctionDescriptor DESTRUCTOR_DESC = FunctionDescriptor.ofVoid(ValueLayout.ADDRESS);

		private static final FunctionDescriptor NAME_DESC = FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS);

		// One global upcall stub per callback shape, shared by every Custom instance. Lives for the
		// JVM lifetime so the function pointers are always valid; dispatch is keyed off the `state`
		// pointer, which carries a registry ID rather than real memory (same trick as Logger).
		private static final MemorySegment FULL_MERGE_STUB;
		private static final MemorySegment PARTIAL_MERGE_STUB;
		private static final MemorySegment DESTRUCTOR_STUB;
		private static final MemorySegment NAME_STUB;

		static {
			try {
				MethodHandles.Lookup lookup = MethodHandles.lookup();
				FULL_MERGE_STUB = Linker.nativeLinker().upcallStub(
						lookup.findStatic(Custom.class, "fullMergeDispatch", MethodType.methodType(
								MemorySegment.class, MemorySegment.class, MemorySegment.class, long.class,
								MemorySegment.class, long.class, MemorySegment.class, MemorySegment.class,
								int.class, MemorySegment.class, MemorySegment.class)),
						FULL_MERGE_DESC, Arena.global());
				PARTIAL_MERGE_STUB = Linker.nativeLinker().upcallStub(
						lookup.findStatic(Custom.class, "partialMergeDispatch", MethodType.methodType(
								MemorySegment.class, MemorySegment.class, MemorySegment.class, long.class,
								MemorySegment.class, MemorySegment.class, int.class, MemorySegment.class,
								MemorySegment.class)),
						PARTIAL_MERGE_DESC, Arena.global());
				DESTRUCTOR_STUB = Linker.nativeLinker().upcallStub(
						lookup.findStatic(Custom.class, "destructorDispatch",
								MethodType.methodType(void.class, MemorySegment.class)),
						DESTRUCTOR_DESC, Arena.global());
				NAME_STUB = Linker.nativeLinker().upcallStub(
						lookup.findStatic(Custom.class, "nameDispatch",
								MethodType.methodType(MemorySegment.class, MemorySegment.class)),
						NAME_DESC, Arena.global());
			} catch (ReflectiveOperationException e) {
				throw new ExceptionInInitializerError(e);
			}
		}

		private record State(FullMergeFn fn, MemorySegment nameSeg) {
		}

		// Registry: id (smuggled through the `state` pointer) -> Java-side merge function.
		// Unregistered from destructorDispatch, not from tryClose: once ownership transfers to
		// Options via applyTo, the native shared_ptr controls this object's real lifetime, which
		// can outlive this Java wrapper.
		private static final UpcallRegistry<State> REGISTRY = new UpcallRegistry<>();
		// Fully-qualified because io.github.dfa1.rocksdbffm.Logger (this package's RocksDB
		// logger wrapper) would otherwise shadow the unqualified name.
		private static final System.Logger LOG = System.getLogger(Custom.class.getName());

		private Custom(MemorySegment ptr) {
			super(ptr);
		}

		static Custom create(String name, FullMergeFn fn) {
			BackgroundUpcallThreads.installShutdownDrain();
			MemorySegment nameSeg = Arena.global().allocateFrom(name);
			MemorySegment statePtr = REGISTRY.register(new State(fn, nameSeg));
			try {
				MemorySegment ptr = (MemorySegment) MH_MERGEOPERATOR_CREATE.invokeExact(
						statePtr, DESTRUCTOR_STUB, FULL_MERGE_STUB, PARTIAL_MERGE_STUB,
						MemorySegment.NULL, NAME_STUB);
				return new Custom(ptr);
			} catch (Throwable t) {
				REGISTRY.unregister(statePtr);
				throw RocksDB.wrapInvokeFailure("MergeOperator.custom failed", t);
			}
		}

		void applyTo(MemorySegment optionsPtr) {
			try {
				MH_SET_MERGE_OPERATOR.invokeExact(optionsPtr, ptr());
				transferOwnership();
			} catch (Throwable t) {
				throw RocksDB.wrapInvokeFailure("MergeOperator.custom applyTo failed", t);
			}
		}

		@Override
		protected void tryClose(MemorySegment ptr) throws Throwable {
			MH_MERGEOPERATOR_DESTROY.invokeExact(ptr);
		}

		private static MemorySegment mallocCopy(byte[] data) {
			try {
				int len = Math.max(1, data.length);
				MemorySegment buf = ((MemorySegment) MH_MALLOC.invokeExact((long) len)).reinterpret(len);
				MemorySegment.copy(data, 0, buf, ValueLayout.JAVA_BYTE, 0, data.length);
				return buf;
			} catch (Throwable t) {
				throw new AssertionError(t);
			}
		}

		/// Builds a zero-copy, read-only view of a borrowed native buffer, bound to `arena` so
		/// use past the call throws rather than reading freed/reused memory (same pattern as
		/// [PinnableHandle#map(Arena, Mapper, MemorySegment)]).
		private static MemorySegment view(MemorySegment ptr, long len, Arena arena) {
			if (MemorySegment.NULL.equals(ptr) || len <= 0) {
				return MemorySegment.ofArray(new byte[0]).asReadOnly();
			}
			return ptr.reinterpret(len, arena, null).asReadOnly();
		}

		private static void writeMergeFailure(MemorySegment successPtr, MemorySegment newValueLenPtr) {
			successPtr.set(ValueLayout.JAVA_BYTE, 0, (byte) 0);
			newValueLenPtr.set(ValueLayout.JAVA_LONG, 0, 0L);
		}

		private static List<MemorySegment> readOperandViews(MemorySegment operandsList, MemorySegment operandsLen,
				int numOperands, Arena arena) {
			List<MemorySegment> operands = new ArrayList<>(numOperands);
			if (numOperands == 0) {
				return operands;
			}
			MemorySegment listArr = operandsList.reinterpret(ValueLayout.ADDRESS.byteSize() * numOperands);
			MemorySegment lenArr = operandsLen.reinterpret(ValueLayout.JAVA_LONG.byteSize() * numOperands);
			for (int i = 0; i < numOperands; i++) {
				operands.add(view(listArr.getAtIndex(ValueLayout.ADDRESS, i), lenArr.getAtIndex(ValueLayout.JAVA_LONG, i), arena));
			}
			return operands;
		}

		/// Called from [#FULL_MERGE_STUB]. Must not throw.
		private static MemorySegment fullMergeDispatch(MemorySegment state, MemorySegment key, long keyLen,
				MemorySegment existingValue, long existingValueLen, MemorySegment operandsList,
				MemorySegment operandsLen, int numOperands, MemorySegment success, MemorySegment newValueLen) {
			BackgroundUpcallThreads.track();
			MemorySegment successPtr = success.reinterpret(ValueLayout.JAVA_BYTE.byteSize());
			MemorySegment newValueLenPtr = newValueLen.reinterpret(ValueLayout.JAVA_LONG.byteSize());
			try (Arena arena = Arena.ofConfined()) {
				State s = REGISTRY.get(state);
				MemorySegment keyView = view(key, keyLen, arena);
				MemorySegment existingView = MemorySegment.NULL.equals(existingValue) ? null : view(existingValue, existingValueLen, arena);
				List<MemorySegment> operandViews = readOperandViews(operandsList, operandsLen, numOperands, arena);
				byte[] result = s.fn().fullMerge(keyView, existingView, operandViews);
				successPtr.set(ValueLayout.JAVA_BYTE, 0, (byte) 1);
				newValueLenPtr.set(ValueLayout.JAVA_LONG, 0, result.length);
				return mallocCopy(result);
			} catch (Throwable t) {
				// must not throw across the upcall boundary — an escaping AssertionError here
				// (assertions are on by default under Surefire) would abort the JVM, not just
				// this call, so report failure to RocksDB instead of asserting.
				LOG.log(System.Logger.Level.ERROR, "fullMerge callback failed", t);
				writeMergeFailure(successPtr, newValueLenPtr);
				return mallocCopy(new byte[0]);
			}
		}

		/// Called from [#PARTIAL_MERGE_STUB]. Always declines: RocksDB then keeps accumulating
		/// operands and calls [#fullMergeDispatch] later instead. Must not throw.
		private static MemorySegment partialMergeDispatch(MemorySegment state, MemorySegment key, long keyLen,
				MemorySegment operandsList, MemorySegment operandsLen, int numOperands,
				MemorySegment success, MemorySegment newValueLen) {
			BackgroundUpcallThreads.track();
			MemorySegment successPtr = success.reinterpret(ValueLayout.JAVA_BYTE.byteSize());
			MemorySegment newValueLenPtr = newValueLen.reinterpret(ValueLayout.JAVA_LONG.byteSize());
			try {
				writeMergeFailure(successPtr, newValueLenPtr);
				return mallocCopy(new byte[0]);
			} catch (Throwable t) {
				// same "must not throw" contract as fullMergeDispatch.
				LOG.log(System.Logger.Level.ERROR, "partialMerge callback failed", t);
				writeMergeFailure(successPtr, newValueLenPtr);
				return mallocCopy(new byte[0]);
			}
		}

		/// Called from [#DESTRUCTOR_STUB] when RocksDB's internal `shared_ptr` refcount hits zero.
		/// This is the only reliable unregistration point: ownership transfer via [#applyTo] means
		/// [#tryClose(MemorySegment)] may never run for this instance. Must not throw.
		private static void destructorDispatch(MemorySegment state) {
			REGISTRY.unregister(state);
		}

		/// Called from [#NAME_STUB]. Must not throw.
		private static MemorySegment nameDispatch(MemorySegment state) {
			State s = REGISTRY.get(state);
			return s != null ? s.nameSeg() : MemorySegment.NULL;
		}
	}
}
