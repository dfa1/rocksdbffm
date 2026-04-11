package io.github.dfa1.rocksdbffm;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/// FFM wrapper for `rocksdb_logger_t`.
///
/// Two factory methods are provided:
///
/// - [#newStderrLogger] — hardcoded to write to stderr; simple, zero overhead
/// - [#newCallbackLogger] — fires a Java [LogCallback] on every log event, so you can
///   route RocksDB output into any logging framework (SLF4J, java.util.logging, a test
///   capture list, …) instead of being stuck with stderr
///
/// RocksDB holds a `shared_ptr` reference to the logger internally. Closing this
/// object decrements that refcount via `rocksdb_logger_destroy`. It is safe to close
/// before the DB is closed — any native log calls that arrive after [#close()] are
/// silently dropped.
///
/// ```
/// // Route RocksDB logs into SLF4J (or any other framework)
/// try (var logger = Logger.newCallbackLogger(LogLevel.INFO,
///             (level, msg) -> slf4jLogger.info("[rocksdb][{}] {}", level, msg));
///      var opts = Options.newOptions().setCreateIfMissing(true).setInfoLog(logger)) {
///     // open DB …
/// }
/// ```
public final class Logger extends NativeObject {

	/// Receives a log message from RocksDB. Must not throw.
	@FunctionalInterface
	public interface LogCallback {
		void log(LogLevel level, String message);
	}

	private static final int STDERR_CALLBACK_ID = -1;

	/// `rocksdb_logger_t* rocksdb_logger_create_stderr_logger(int log_level, const char* prefix);`
	private static final MethodHandle MH_CREATE_STDERR;
	/// `rocksdb_logger_t* rocksdb_logger_create_callback_logger(int log_level, void (*)(void* priv, unsigned lev, char* msg, size_t len), void* priv);`
	private static final MethodHandle MH_CREATE_CALLBACK;
	/// `void rocksdb_logger_destroy(rocksdb_logger_t* logger);`
	private static final MethodHandle MH_DESTROY;

	private static final FunctionDescriptor CALLBACK_DESC = FunctionDescriptor.ofVoid(
			ValueLayout.ADDRESS,   // void* priv  — carries the callback ID
			ValueLayout.JAVA_INT,  // unsigned lev
			ValueLayout.ADDRESS,   // char* msg
			ValueLayout.JAVA_LONG  // size_t len
	);

	// Registry: callback ID → LogCallback.
	// IDs are used as the priv pointer value; no real memory is accessed at that address.
	private static final ConcurrentHashMap<Long, LogCallback> REGISTRY = new ConcurrentHashMap<>();
	private static final AtomicLong NEXT_ID = new AtomicLong(1);

	// One global upcall stub shared by all callback loggers.
	// Lives for the JVM lifetime so the function pointer is always valid.
	private static final MemorySegment GLOBAL_STUB;

	static {
		MH_CREATE_STDERR = NativeLibrary.lookup("rocksdb_logger_create_stderr_logger",
				FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.JAVA_INT, ValueLayout.ADDRESS));

		MH_CREATE_CALLBACK = NativeLibrary.lookup("rocksdb_logger_create_callback_logger",
				FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_DESTROY = NativeLibrary.lookup("rocksdb_logger_destroy",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

		try {
			MethodHandle dispatchMH = MethodHandles.lookup().findStatic(Logger.class, "dispatch",
					MethodType.methodType(void.class, MemorySegment.class, int.class, MemorySegment.class, long.class));
			GLOBAL_STUB = Linker.nativeLinker().upcallStub(dispatchMH, CALLBACK_DESC, Arena.global());
		} catch (ReflectiveOperationException e) {
			throw new ExceptionInInitializerError(e);
		}
	}

	/// `-1` for stderr loggers; the registry key for callback loggers.
	private final long callbackId;

	private Logger(MemorySegment ptr, long callbackId) {
		super(ptr);
		this.callbackId = callbackId;
	}

	/// Creates a logger that writes to stderr.
	///
	/// @param level  minimum level to emit; messages below this are suppressed
	/// @param prefix optional string prepended to every log line, or `null` for none
	public static Logger newStderrLogger(LogLevel level, String prefix) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment prefixSeg = prefix != null ? arena.allocateFrom(prefix) : MemorySegment.NULL;
			MemorySegment ptr = (MemorySegment) MH_CREATE_STDERR.invokeExact(level.value, prefixSeg);
			return new Logger(ptr, STDERR_CALLBACK_ID);
		} catch (Throwable t) {
			throw new RocksDBException("Logger.newStderrLogger failed", t);
		}
	}

	/// Creates a logger that dispatches messages to the given Java callback.
	///
	/// The callback may be invoked on RocksDB's internal threads; implementations must
	/// be thread-safe and must not throw exceptions.
	/// After this Logger is closed, any in-flight or subsequent native calls are dropped.
	///
	/// @param minLevel minimum level to deliver; lower-level messages are suppressed by RocksDB
	/// @param callback receives each log message
	public static Logger newCallbackLogger(LogLevel minLevel, LogCallback callback) {
		long id = NEXT_ID.getAndIncrement();
		REGISTRY.put(id, callback);
		try {
			// Pass the ID as the priv pointer value (address, not dereferenced).
			MemorySegment privPtr = MemorySegment.ofAddress(id);
			MemorySegment ptr = (MemorySegment) MH_CREATE_CALLBACK.invokeExact(minLevel.value, GLOBAL_STUB, privPtr);
			return new Logger(ptr, id);
		} catch (Throwable t) {
			REGISTRY.remove(id);
			throw new RocksDBException("Logger.newCallbackLogger failed", t);
		}
	}

	@Override
	protected void tryClose(MemorySegment ptr) throws Throwable {
		if (callbackId != STDERR_CALLBACK_ID) {
			// Unregister first so any concurrent native invocation after destroy becomes a no-op.
			REGISTRY.remove(callbackId);
		}
		MH_DESTROY.invokeExact(ptr);
	}

	/// Called from the single global upcall stub. Must not throw.
	private static void dispatch(MemorySegment priv, int lev, MemorySegment msg, long len) {
		try {
			LogCallback cb = REGISTRY.get(priv.address());
			if (cb == null) {
				return;
			}
			LogLevel level = LogLevel.fromValue(lev);
			String message;
			if (msg.equals(MemorySegment.NULL) || len <= 0) {
				message = "";
			} else {
				// msg arrives as a zero-size segment; reinterpret to give it bounds
				// so it is not possible to use msg.getString(0), that works only for zero terminated strings
				byte[] bytes = new byte[(int) len];
				msg.reinterpret(len).asByteBuffer().get(bytes, 0, (int) len);
				message = new String(bytes, 0, (int) len, StandardCharsets.UTF_8);
			}
			message = dropTrailingNewline(message);
			cb.log(level, message);
		} catch (Throwable throwable) {
			// exceptions must not escape into native code, but they must be shown to the user somehow
			assert false; // fail at least the build
		}
	}

	// sometimes callback is invoked with a message without \n but most of the time, with it
	// normalize the message to always drop the last newline (so logging framework are happy)
	private static String dropTrailingNewline(String message) {
		if (message.isEmpty()) {
			return message;
		}
		final int lastCharIndex = message.length() - 1;
		if (message.charAt(lastCharIndex) == '\n') {
			message = message.substring(0, lastCharIndex);
		}
		return message;
	}
}
