package io.github.dfa1.rocksdbffm;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/**
 * Centralized utility for RocksDB native operations.
 * Handles symbol lookup, error checking, and common memory patterns.
 */
public final class Native {

    private static final MethodHandle MH_FREE = RocksDB.lookup("rocksdb_free",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

    private Native() {}

    /**
     * Creates a pre-zeroed error holder in the given arena.
     * Use this for RocksDB C calls that take {@code char** errptr}.
     */
    public static MemorySegment errHolder(Arena arena) {
        MemorySegment holder = arena.allocate(ValueLayout.ADDRESS);
        holder.set(ValueLayout.ADDRESS, 0, MemorySegment.NULL);
        return holder;
    }

    /**
     * Checks if the error holder contains a non-NULL pointer.
     * If so, throws a {@link RocksDBException} and frees the C string.
     */
    public static void checkError(MemorySegment errHolder) {
        MemorySegment errPtr = errHolder.get(ValueLayout.ADDRESS, 0);
        if (!MemorySegment.NULL.equals(errPtr)) {
            String msg = errPtr.reinterpret(Long.MAX_VALUE).getString(0);
            try {
                MH_FREE.invokeExact(errPtr);
            } catch (Throwable ignored) {
                // Best effort to free
            }
            throw new RocksDBException(msg);
        }
    }

    /**
     * Executes a native call that might return an error.
     * Allocates a temporary {@link Arena#ofConfined()} for the error holder.
     */
    public static void check(CheckedConsumer<MemorySegment> action) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment holder = errHolder(arena);
            action.accept(holder);
            checkError(holder);
        } catch (RocksDBException e) {
            throw e;
        } catch (Throwable t) {
            throw new RocksDBException("Native call failed", t);
        }
    }

    /**
     * Executes a native call that returns a value and might return an error.
     */
    public static <T> T check(CheckedFunction<MemorySegment, T> action) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment holder = errHolder(arena);
            T result = action.apply(holder);
            checkError(holder);
            return result;
        } catch (RocksDBException e) {
            throw e;
        } catch (Throwable t) {
            throw new RocksDBException("Native call failed", t);
        }
    }

    @FunctionalInterface
    public interface CheckedConsumer<T> {
        void accept(T t) throws Throwable;
    }

    @FunctionalInterface
    public interface CheckedFunction<T, R> {
        R apply(T t) throws Throwable;
    }
}
