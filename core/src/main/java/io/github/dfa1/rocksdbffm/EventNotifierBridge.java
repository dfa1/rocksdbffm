package io.github.dfa1.rocksdbffm;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.function.Consumer;

/// FFM plumbing behind [Options#addEventListener(EventNotifier)]. Wraps
/// `rocksdb_eventlistener_create()`, whose 10 callbacks are dispatched to one Java
/// [EventNotifier] using the same registry-id-through-`state`-pointer trick as [Logger] and
/// [MergeOperator.Custom].
///
/// Unlike those, there is no public native handle here: `rocksdb_options_add_eventlistener()`
/// cannot fail (it is `void`, no `errptr`), so [#attach(MemorySegment, EventNotifier)] creates
/// the native listener and attaches it to the given `rocksdb_options_t*` in one step, leaving no
/// window where a caller would need to remember to close anything. `rocksdb_eventlistener_destroy`
/// is deliberately never called: once attached, RocksDB wraps the listener in a `shared_ptr` that
/// owns it from then on, and it deletes the listener itself, firing the destructor callback below,
/// when that `shared_ptr`'s refcount hits zero — the only reliable point to unregister.
final class EventNotifierBridge {

	private EventNotifierBridge() {
	}

	/// `rocksdb_eventlistener_t* rocksdb_eventlistener_create(void* state_, void (*destructor_)(void*), on_flush_begin_cb on_flush_begin, on_flush_completed_cb on_flush_completed, on_compaction_begin_cb on_compaction_begin, on_compaction_completed_cb on_compaction_completed, on_subcompaction_begin_cb on_subcompaction_begin, on_subcompaction_completed_cb on_subcompaction_completed, on_external_file_ingested_cb on_external_file_ingested, on_background_error_cb on_background_error, on_stall_conditions_changed_cb on_stall_conditions_changed, on_memtable_sealed_cb on_memtable_sealed);`
	private static final MethodHandle MH_EVENTLISTENER_CREATE = NativeLibrary.lookup(
			"rocksdb_eventlistener_create",
			FunctionDescriptor.of(ValueLayout.ADDRESS,
					ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS,
					ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS,
					ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));

	/// `void rocksdb_options_add_eventlistener(rocksdb_options_t*, rocksdb_eventlistener_t*);`
	private static final MethodHandle MH_OPTIONS_ADD_EVENTLISTENER = NativeLibrary.lookup(
			"rocksdb_options_add_eventlistener",
			FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS));

	/// `void rocksdb_status_ptr_get_error(rocksdb_status_ptr_t* status, char** errptr);`
	private static final MethodHandle MH_STATUS_PTR_GET_ERROR = NativeLibrary.lookup(
			"rocksdb_status_ptr_get_error", FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS));

	private static final FunctionDescriptor DB_SCOPED_INFO_DESC = FunctionDescriptor.ofVoid(
			ValueLayout.ADDRESS,  // state
			ValueLayout.ADDRESS,  // rocksdb_t* db -- deliberately unused, see EventNotifier
			ValueLayout.ADDRESS   // info
	);

	private static final FunctionDescriptor INFO_ONLY_DESC = FunctionDescriptor.ofVoid(
			ValueLayout.ADDRESS,  // state
			ValueLayout.ADDRESS   // info
	);

	private static final FunctionDescriptor BACKGROUND_ERROR_DESC = FunctionDescriptor.ofVoid(
			ValueLayout.ADDRESS,   // state
			ValueLayout.JAVA_INT,  // uint32_t reason
			ValueLayout.ADDRESS    // rocksdb_status_ptr_t* status
	);

	private static final FunctionDescriptor DESTRUCTOR_DESC = FunctionDescriptor.ofVoid(ValueLayout.ADDRESS);

	// One global upcall stub per callback, shared by every attached EventNotifier. Live for the
	// JVM lifetime so the function pointers stay valid; dispatch is keyed off the `state` pointer,
	// which carries a registry id rather than real memory (same trick as Logger/MergeOperator.Custom).
	private static final MemorySegment FLUSH_BEGIN_STUB;
	private static final MemorySegment FLUSH_COMPLETED_STUB;
	private static final MemorySegment COMPACTION_BEGIN_STUB;
	private static final MemorySegment COMPACTION_COMPLETED_STUB;
	// RocksDB's C struct calls on_subcompaction_begin/on_subcompaction_completed unconditionally
	// (db/c.cc has no null check), so both slots still need a valid function pointer even though
	// EventNotifier does not expose subcompaction callbacks; one shared no-op stub covers both.
	private static final MemorySegment SUBCOMPACTION_NOOP_STUB;
	private static final MemorySegment EXTERNAL_FILE_INGESTED_STUB;
	private static final MemorySegment BACKGROUND_ERROR_STUB;
	private static final MemorySegment STALL_CONDITIONS_CHANGED_STUB;
	private static final MemorySegment MEMTABLE_SEALED_STUB;
	private static final MemorySegment DESTRUCTOR_STUB;

	private static final UpcallRegistry<EventNotifier> REGISTRY = new UpcallRegistry<>();
	private static final System.Logger LOG = System.getLogger(EventNotifierBridge.class.getName());

	static {
		try {
			MethodHandles.Lookup lookup = MethodHandles.lookup();
			MethodType dbScopedType = MethodType.methodType(
					void.class, MemorySegment.class, MemorySegment.class, MemorySegment.class);
			MethodType infoOnlyType = MethodType.methodType(void.class, MemorySegment.class, MemorySegment.class);

			FLUSH_BEGIN_STUB = Linker.nativeLinker().upcallStub(
					lookup.findStatic(EventNotifierBridge.class, "flushBeginDispatch", dbScopedType),
					DB_SCOPED_INFO_DESC, Arena.global());
			FLUSH_COMPLETED_STUB = Linker.nativeLinker().upcallStub(
					lookup.findStatic(EventNotifierBridge.class, "flushCompletedDispatch", dbScopedType),
					DB_SCOPED_INFO_DESC, Arena.global());
			COMPACTION_BEGIN_STUB = Linker.nativeLinker().upcallStub(
					lookup.findStatic(EventNotifierBridge.class, "compactionBeginDispatch", dbScopedType),
					DB_SCOPED_INFO_DESC, Arena.global());
			COMPACTION_COMPLETED_STUB = Linker.nativeLinker().upcallStub(
					lookup.findStatic(EventNotifierBridge.class, "compactionCompletedDispatch", dbScopedType),
					DB_SCOPED_INFO_DESC, Arena.global());
			SUBCOMPACTION_NOOP_STUB = Linker.nativeLinker().upcallStub(
					lookup.findStatic(EventNotifierBridge.class, "subcompactionNoopDispatch", infoOnlyType),
					INFO_ONLY_DESC, Arena.global());
			EXTERNAL_FILE_INGESTED_STUB = Linker.nativeLinker().upcallStub(
					lookup.findStatic(EventNotifierBridge.class, "externalFileIngestedDispatch", dbScopedType),
					DB_SCOPED_INFO_DESC, Arena.global());
			BACKGROUND_ERROR_STUB = Linker.nativeLinker().upcallStub(
					lookup.findStatic(EventNotifierBridge.class, "backgroundErrorDispatch", MethodType.methodType(
							void.class, MemorySegment.class, int.class, MemorySegment.class)),
					BACKGROUND_ERROR_DESC, Arena.global());
			STALL_CONDITIONS_CHANGED_STUB = Linker.nativeLinker().upcallStub(
					lookup.findStatic(EventNotifierBridge.class, "stallConditionsChangedDispatch", infoOnlyType),
					INFO_ONLY_DESC, Arena.global());
			MEMTABLE_SEALED_STUB = Linker.nativeLinker().upcallStub(
					lookup.findStatic(EventNotifierBridge.class, "memTableSealedDispatch", infoOnlyType),
					INFO_ONLY_DESC, Arena.global());
			DESTRUCTOR_STUB = Linker.nativeLinker().upcallStub(
					lookup.findStatic(EventNotifierBridge.class, "destructorDispatch",
							MethodType.methodType(void.class, MemorySegment.class)),
					DESTRUCTOR_DESC, Arena.global());
		} catch (ReflectiveOperationException e) {
			throw new ExceptionInInitializerError(e);
		}
	}

	/// Creates a native `rocksdb_eventlistener_t*` dispatching to `notifier` and attaches it to
	/// `optionsPtr`, transferring ownership to RocksDB in the same step.
	///
	/// @param optionsPtr the `rocksdb_options_t*` to attach the listener to
	/// @param notifier   the Java callback to dispatch every event to
	static void attach(MemorySegment optionsPtr, EventNotifier notifier) {
		BackgroundUpcallThreads.installShutdownDrain();
		MemorySegment statePtr = REGISTRY.register(notifier);
		try {
			MemorySegment listenerPtr = (MemorySegment) MH_EVENTLISTENER_CREATE.invokeExact(
					statePtr, DESTRUCTOR_STUB, FLUSH_BEGIN_STUB, FLUSH_COMPLETED_STUB,
					COMPACTION_BEGIN_STUB, COMPACTION_COMPLETED_STUB,
					SUBCOMPACTION_NOOP_STUB, SUBCOMPACTION_NOOP_STUB,
					EXTERNAL_FILE_INGESTED_STUB, BACKGROUND_ERROR_STUB,
					STALL_CONDITIONS_CHANGED_STUB, MEMTABLE_SEALED_STUB);
			MH_OPTIONS_ADD_EVENTLISTENER.invokeExact(optionsPtr, listenerPtr);
		} catch (Throwable t) {
			REGISTRY.unregister(statePtr);
			throw RocksDB.wrapInvokeFailure("EventNotifier attach failed", t);
		}
	}

	/// Called from [#FLUSH_BEGIN_STUB]. Must not throw.
	private static void flushBeginDispatch(MemorySegment state, MemorySegment db, MemorySegment info) {
		dispatch(state, n -> n.onFlushBegin(new FlushJobInfo(info)), "onFlushBegin");
	}

	/// Called from [#FLUSH_COMPLETED_STUB]. Must not throw.
	private static void flushCompletedDispatch(MemorySegment state, MemorySegment db, MemorySegment info) {
		dispatch(state, n -> n.onFlushCompleted(new FlushJobInfo(info)), "onFlushCompleted");
	}

	/// Called from [#COMPACTION_BEGIN_STUB]. Must not throw.
	private static void compactionBeginDispatch(MemorySegment state, MemorySegment db, MemorySegment info) {
		dispatch(state, n -> n.onCompactionBegin(new CompactionJobInfo(info)), "onCompactionBegin");
	}

	/// Called from [#COMPACTION_COMPLETED_STUB]. Must not throw.
	private static void compactionCompletedDispatch(MemorySegment state, MemorySegment db, MemorySegment info) {
		dispatch(state, n -> n.onCompactionCompleted(new CompactionJobInfo(info)), "onCompactionCompleted");
	}

	/// Called from [#SUBCOMPACTION_NOOP_STUB]. Deliberately does nothing: subcompaction events are
	/// not exposed on [EventNotifier]. Must not throw.
	private static void subcompactionNoopDispatch(MemorySegment state, MemorySegment info) {
	}

	/// Called from [#EXTERNAL_FILE_INGESTED_STUB]. Must not throw.
	private static void externalFileIngestedDispatch(MemorySegment state, MemorySegment db, MemorySegment info) {
		dispatch(state, n -> n.onExternalFileIngested(new ExternalFileIngestionInfo(info)), "onExternalFileIngested");
	}

	/// Called from [#STALL_CONDITIONS_CHANGED_STUB]. Must not throw.
	private static void stallConditionsChangedDispatch(MemorySegment state, MemorySegment info) {
		dispatch(state, n -> n.onStallConditionsChanged(new WriteStallInfo(info)), "onStallConditionsChanged");
	}

	/// Called from [#MEMTABLE_SEALED_STUB]. Must not throw.
	private static void memTableSealedDispatch(MemorySegment state, MemorySegment info) {
		dispatch(state, n -> n.onMemTableSealed(new MemTableInfo(info)), "onMemTableSealed");
	}

	/// Called from [#BACKGROUND_ERROR_STUB]. Must not throw.
	private static void backgroundErrorDispatch(MemorySegment state, int reason, MemorySegment statusPtr) {
		dispatch(state, n -> {
			try (Arena arena = Arena.ofConfined()) {
				MemorySegment err = RocksDB.errHolder(arena);
				try {
					MH_STATUS_PTR_GET_ERROR.invokeExact(statusPtr, err);
				} catch (Throwable t) {
					throw new AssertionError(t);
				}
				RocksDBException error = null;
				try {
					RocksDB.checkError(err);
				} catch (RocksDBException e) {
					error = e;
				}
				n.onBackgroundError(BackgroundErrorReason.fromValue(reason), error);
			}
		}, "onBackgroundError");
	}

	/// Called from [#DESTRUCTOR_STUB] when RocksDB's internal `shared_ptr` refcount hits zero.
	/// This is the only reliable unregistration point, since [#attach(MemorySegment, EventNotifier)]
	/// transfers ownership immediately. Must not throw.
	private static void destructorDispatch(MemorySegment state) {
		REGISTRY.unregister(state);
	}

	private static void dispatch(MemorySegment state, Consumer<EventNotifier> call, String method) {
		BackgroundUpcallThreads.track();
		try {
			EventNotifier notifier = REGISTRY.get(state);
			if (notifier != null) {
				call.accept(notifier);
			}
		} catch (Throwable t) {
			// exceptions must not escape into native code, but they must be shown to the user somehow
			LOG.log(System.Logger.Level.ERROR, method + " callback failed", t);
		}
	}
}
