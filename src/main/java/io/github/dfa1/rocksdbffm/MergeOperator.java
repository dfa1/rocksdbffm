package io.github.dfa1.rocksdbffm;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.List;

/**
 * FFM wrapper for {@code rocksdb_mergeoperator_t}.
 *
 * <p>Use {@link #create} to build a custom operator backed by Java lambdas, or
 * call {@link io.github.dfa1.rocksdbffm.Options#setUint64AddMergeOperator()} for the
 * built-in uint64 accumulator.
 *
 * <p>Pass the operator to {@link Options#setMergeOperator(MergeOperator)}, which
 * transfers ownership to the Options object. The operator should still be closed via
 * try-with-resources — {@link #close()} becomes a no-op after ownership transfer.
 *
 * <pre>{@code
 * try (var op  = MergeOperator.create("append", (key, existing, ops) -> {
 *         var sb = new StringBuilder(existing == null ? "" : new String(existing));
 *         ops.forEach(o -> sb.append(new String(o)));
 *         return sb.toString().getBytes();
 *     }, (key, ops) -> null);                    // defer partial merge
 *      var opts = new Options().setCreateIfMissing(true).setMergeOperator(op);
 *      var db = RocksDB.open(opts, path)) {
 *     db.merge("k".getBytes(), ",hello".getBytes());
 * }
 * }</pre>
 */
public final class MergeOperator implements AutoCloseable {

    /**
     * Combines an existing value with a list of accumulated operands.
     * Return {@code null} to signal failure (RocksDB will surface an error on read).
     */
    @FunctionalInterface
    public interface FullMerge {
        byte[] merge(byte[] key, byte[] existingValue, List<byte[]> operands);
    }

    /**
     * Combines two or more operands before a full merge is available.
     * Return {@code null} to defer (operands will be passed to {@link FullMerge} instead).
     */
    @FunctionalInterface
    public interface PartialMerge {
        byte[] merge(byte[] key, List<byte[]> operands);
    }

    private static final Linker LINKER = Linker.nativeLinker();

    private static final MethodHandle MH_CREATE;
    private static final MethodHandle MH_DESTROY;
    static final MethodHandle MH_MALLOC;
    static final MethodHandle MH_FREE;

    private static final FunctionDescriptor DESC_FULL_MERGE;
    private static final FunctionDescriptor DESC_PARTIAL_MERGE;
    private static final FunctionDescriptor DESC_DELETE_VALUE;
    private static final FunctionDescriptor DESC_DESTRUCTOR;
    private static final FunctionDescriptor DESC_NAME;

    static {
        MH_CREATE = RocksDB.lookup("rocksdb_mergeoperator_create",
            FunctionDescriptor.of(ValueLayout.ADDRESS,
                ValueLayout.ADDRESS,  // state
                ValueLayout.ADDRESS,  // destructor fn ptr
                ValueLayout.ADDRESS,  // full_merge fn ptr
                ValueLayout.ADDRESS,  // partial_merge fn ptr
                ValueLayout.ADDRESS,  // delete_value fn ptr
                ValueLayout.ADDRESS   // name fn ptr
            ));

        MH_DESTROY = RocksDB.lookup("rocksdb_mergeoperator_destroy",
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

        MH_MALLOC = LINKER.downcallHandle(
            LINKER.defaultLookup().find("malloc").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));

        MH_FREE = LINKER.downcallHandle(
            LINKER.defaultLookup().find("free").orElseThrow(),
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

        // char* full_merge(state, key, klen, existing, elen, ops_list, ops_len_list,
        //                  n_ops, success*, new_val_len*)
        DESC_FULL_MERGE = FunctionDescriptor.of(ValueLayout.ADDRESS,
            ValueLayout.ADDRESS,                          // state
            ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,   // key, klen
            ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,   // existing, elen (nullable)
            ValueLayout.ADDRESS, ValueLayout.ADDRESS,     // ops_list, ops_len_list
            ValueLayout.JAVA_INT,                         // n_ops
            ValueLayout.ADDRESS, ValueLayout.ADDRESS);    // success*, new_val_len*

        // char* partial_merge(state, key, klen, ops_list, ops_len_list, n_ops, success*, new_val_len*)
        DESC_PARTIAL_MERGE = FunctionDescriptor.of(ValueLayout.ADDRESS,
            ValueLayout.ADDRESS,                          // state
            ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,   // key, klen
            ValueLayout.ADDRESS, ValueLayout.ADDRESS,     // ops_list, ops_len_list
            ValueLayout.JAVA_INT,                         // n_ops
            ValueLayout.ADDRESS, ValueLayout.ADDRESS);    // success*, new_val_len*

        // void delete_value(state, value, vlen)
        DESC_DELETE_VALUE = FunctionDescriptor.ofVoid(
            ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG);

        // void destructor(state)
        DESC_DESTRUCTOR = FunctionDescriptor.ofVoid(ValueLayout.ADDRESS);

        // const char* name(state)
        DESC_NAME = FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS);
    }

    /** Package-private: read by {@link Options#setMergeOperator}. */
    final MemorySegment ptr;

    // Strong references prevent GC of upcall stubs; the Arena owns their native memory.
    private final Arena stubArena;

    private boolean transferred;

    private MergeOperator(MemorySegment ptr, Arena stubArena) {
        this.ptr = ptr;
        this.stubArena = stubArena;
    }

    /**
     * Creates a custom merge operator backed by Java callbacks.
     *
     * @param name         unique name stored in SST metadata (cannot change once DBs are created)
     * @param fullMerge    applies accumulated operands to an existing value
     * @param partialMerge pre-merges operands (return {@code null} to defer)
     */
    public static MergeOperator create(String name, FullMerge fullMerge, PartialMerge partialMerge) {
        Arena stubArena = Arena.ofShared(); // shared: callbacks may fire on compaction threads
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();

            MemorySegment nameSegment = stubArena.allocateFrom(name);

            MethodHandle destructorMh = lookup.findStatic(MergeOperator.class, "destructorImpl",
                MethodType.methodType(void.class, MemorySegment.class));

            MethodHandle fullMergeMh = MethodHandles.insertArguments(
                lookup.findStatic(MergeOperator.class, "fullMergeImpl",
                    MethodType.methodType(MemorySegment.class,
                        FullMerge.class,
                        MemorySegment.class,
                        MemorySegment.class, long.class,
                        MemorySegment.class, long.class,
                        MemorySegment.class, MemorySegment.class,
                        int.class,
                        MemorySegment.class, MemorySegment.class)),
                0, fullMerge);

            MethodHandle partialMergeMh = MethodHandles.insertArguments(
                lookup.findStatic(MergeOperator.class, "partialMergeImpl",
                    MethodType.methodType(MemorySegment.class,
                        PartialMerge.class,
                        MemorySegment.class,
                        MemorySegment.class, long.class,
                        MemorySegment.class, MemorySegment.class,
                        int.class,
                        MemorySegment.class, MemorySegment.class)),
                0, partialMerge);

            MethodHandle deleteValueMh = lookup.findStatic(MergeOperator.class, "deleteValueImpl",
                MethodType.methodType(void.class,
                    MemorySegment.class, MemorySegment.class, long.class));

            MethodHandle nameMh = MethodHandles.insertArguments(
                lookup.findStatic(MergeOperator.class, "nameImpl",
                    MethodType.methodType(MemorySegment.class,
                        MemorySegment.class, MemorySegment.class)),
                0, nameSegment);

            MemorySegment ptr = (MemorySegment) MH_CREATE.invokeExact(
                MemorySegment.NULL,
                LINKER.upcallStub(destructorMh,   DESC_DESTRUCTOR,   stubArena),
                LINKER.upcallStub(fullMergeMh,    DESC_FULL_MERGE,   stubArena),
                LINKER.upcallStub(partialMergeMh, DESC_PARTIAL_MERGE, stubArena),
                LINKER.upcallStub(deleteValueMh,  DESC_DELETE_VALUE, stubArena),
                LINKER.upcallStub(nameMh,         DESC_NAME,         stubArena));

            return new MergeOperator(ptr, stubArena);
        } catch (Throwable t) {
            stubArena.close();
            throw RocksDBException.wrap("MergeOperator.create failed", t);
        }
    }

    // -----------------------------------------------------------------------
    // Upcall implementations (called from native RocksDB threads)
    // -----------------------------------------------------------------------

    private static void destructorImpl(MemorySegment state) {
        // Java GC manages the callbacks; nothing to do here.
    }

    private static MemorySegment fullMergeImpl(
            FullMerge callback,
            MemorySegment state,
            MemorySegment keyPtr,  long keyLen,
            MemorySegment existingPtr, long existingLen,
            MemorySegment opsList, MemorySegment opsLenList,
            int numOps,
            MemorySegment successPtr, MemorySegment newLenPtr) {
        try {
            byte[] key      = keyPtr.reinterpret(keyLen).toArray(ValueLayout.JAVA_BYTE);
            byte[] existing = MemorySegment.NULL.equals(existingPtr) ? null
                : existingPtr.reinterpret(existingLen).toArray(ValueLayout.JAVA_BYTE);
            List<byte[]> operands = readOperands(opsList, opsLenList, numOps);
            return writeResult(callback.merge(key, existing, operands), successPtr, newLenPtr);
        } catch (Throwable t) {
            successPtr.reinterpret(1).set(ValueLayout.JAVA_BYTE, 0, (byte) 0);
            return MemorySegment.NULL;
        }
    }

    private static MemorySegment partialMergeImpl(
            PartialMerge callback,
            MemorySegment state,
            MemorySegment keyPtr, long keyLen,
            MemorySegment opsList, MemorySegment opsLenList,
            int numOps,
            MemorySegment successPtr, MemorySegment newLenPtr) {
        try {
            byte[] key = keyPtr.reinterpret(keyLen).toArray(ValueLayout.JAVA_BYTE);
            List<byte[]> operands = readOperands(opsList, opsLenList, numOps);
            return writeResult(callback.merge(key, operands), successPtr, newLenPtr);
        } catch (Throwable t) {
            successPtr.reinterpret(1).set(ValueLayout.JAVA_BYTE, 0, (byte) 0);
            return MemorySegment.NULL;
        }
    }

    private static void deleteValueImpl(MemorySegment state, MemorySegment value, long len) {
        try {
            MH_FREE.invokeExact(value);
        } catch (Throwable ignored) {}
    }

    private static MemorySegment nameImpl(MemorySegment nameSegment, MemorySegment state) {
        return nameSegment;
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static List<byte[]> readOperands(
            MemorySegment opsList, MemorySegment opsLenList, int n) {
        MemorySegment listView = opsList.reinterpret((long) n * ValueLayout.ADDRESS.byteSize());
        MemorySegment lenView  = opsLenList.reinterpret((long) n * ValueLayout.JAVA_LONG.byteSize());
        List<byte[]> result = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            MemorySegment op = listView.getAtIndex(ValueLayout.ADDRESS, i);
            long opLen       = lenView.getAtIndex(ValueLayout.JAVA_LONG, i);
            result.add(op.reinterpret(opLen).toArray(ValueLayout.JAVA_BYTE));
        }
        return result;
    }

    private static MemorySegment writeResult(byte[] result, MemorySegment successPtr, MemorySegment newLenPtr) {
        // successPtr and newLenPtr arrive from native with byteSize=0; reinterpret before writing.
        if (result == null) {
            successPtr.reinterpret(1).set(ValueLayout.JAVA_BYTE, 0, (byte) 0);
            return MemorySegment.NULL;
        }
        try {
            MemorySegment buf = (MemorySegment) MH_MALLOC.invokeExact((long) result.length);
            MemorySegment.copy(result, 0, buf.reinterpret(result.length), ValueLayout.JAVA_BYTE, 0, result.length);
            successPtr.reinterpret(1).set(ValueLayout.JAVA_BYTE, 0, (byte) 1);
            newLenPtr.reinterpret(Long.BYTES).set(ValueLayout.JAVA_LONG, 0, (long) result.length);
            return buf;
        } catch (Throwable t) {
            successPtr.reinterpret(1).set(ValueLayout.JAVA_BYTE, 0, (byte) 0);
            return MemorySegment.NULL;
        }
    }

    /** Called by {@link Options#setMergeOperator}. Subsequent {@link #close()} calls skip native destroy. */
    void transferOwnership() {
        this.transferred = true;
    }

    @Override
    public void close() {
        if (!transferred) {
            try {
                MH_DESTROY.invokeExact(ptr);
            } catch (Throwable t) {
                throw RocksDBException.wrap("MergeOperator destroy failed", t);
            }
        }
        stubArena.close();
    }
}
