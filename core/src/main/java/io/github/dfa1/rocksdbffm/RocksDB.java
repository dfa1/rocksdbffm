package io.github.dfa1.rocksdbffm;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

/// Entry point for opening RocksDB databases.
///
/// All factory methods return a strongly-typed instance:
///
/// | Method | Returns |
/// |---|---|
/// | [#open] | [ReadWriteDB] |
/// | [#openReadOnly] | [ReadOnlyDB] |
/// | [#openWithTtl] | [TtlDB] |
/// | [#openSecondary] | [SecondaryDB] |
/// | [#openTransaction] | [TransactionDB] |
/// | [#openOptimistic] | [OptimisticTransactionDB] |
///
/// `RocksDB` is non-instantiable; it also acts as the single holder of all
/// `rocksdb_t*` method handles, which are mapped exactly once and exposed
/// via package-private static helpers to sibling classes.
public final class RocksDB {

	// -----------------------------------------------------------------------
	// Open handles — used only inside factory methods
	// -----------------------------------------------------------------------

	/// `rocksdb_t* rocksdb_open(const rocksdb_options_t* options, const char* name, char** errptr);`
	private static final MethodHandle MH_OPEN;
	/// `rocksdb_t* rocksdb_open_with_ttl(const rocksdb_options_t* options, const char* name, int ttl, char** errptr);`
	private static final MethodHandle MH_OPEN_WITH_TTL;
	/// `rocksdb_t* rocksdb_open_for_read_only(const rocksdb_options_t* options, const char* name, unsigned char error_if_wal_file_exists, char** errptr);`
	private static final MethodHandle MH_OPEN_FOR_READ_ONLY;
	/// `rocksdb_t* rocksdb_open_as_secondary(const rocksdb_options_t* options, const char* name, const char* secondary_path, char** errptr);`
	private static final MethodHandle MH_OPEN_SECONDARY;
	/// `rocksdb_transactiondb_t* rocksdb_transactiondb_open(const rocksdb_options_t* options, const rocksdb_transactiondb_options_t* txn_db_options, const char* name, char** errptr);`
	private static final MethodHandle MH_OPEN_TRANSACTION;
	/// `rocksdb_optimistictransactiondb_t* rocksdb_optimistictransactiondb_open(const rocksdb_options_t* options, const char* name, char** errptr);`
	private static final MethodHandle MH_OPEN_OPTIMISTIC;
	/// `rocksdb_t* rocksdb_optimistictransactiondb_get_base_db(rocksdb_optimistictransactiondb_t* otxn_db);`
	private static final MethodHandle MH_GET_BASE_DB;
	// -----------------------------------------------------------------------
	// Shared rocksdb_t* method handles — private, accessed via static helpers
	// -----------------------------------------------------------------------

	/// `void rocksdb_close(rocksdb_t* db);`
	private static final MethodHandle MH_CLOSE;
	/// `rocksdb_pinnableslice_t* rocksdb_get_pinned(rocksdb_t* db, const rocksdb_readoptions_t* options, const char* key, size_t keylen, char** errptr);`
	private static final MethodHandle MH_GET_PINNED;
	/// `const char* rocksdb_pinnableslice_value(const rocksdb_pinnableslice_t* t, size_t* vlen);`
	private static final MethodHandle MH_PINNABLESLICE_VALUE;
	/// `void rocksdb_pinnableslice_destroy(rocksdb_pinnableslice_t* v);`
	private static final MethodHandle MH_PINNABLESLICE_DESTROY;
	/// `void rocksdb_put(rocksdb_t* db, const rocksdb_writeoptions_t* options, const char* key, size_t keylen, const char* val, size_t vallen, char** errptr);`
	private static final MethodHandle MH_PUT;
	/// `void rocksdb_delete(rocksdb_t* db, const rocksdb_writeoptions_t* options, const char* key, size_t keylen, char** errptr);`
	private static final MethodHandle MH_DELETE;
	/// `void rocksdb_flush(rocksdb_t* db, const rocksdb_flushoptions_t* options, char** errptr);`
	private static final MethodHandle MH_FLUSH;
	/// `void rocksdb_flush_wal(rocksdb_t* db, unsigned char sync, char** errptr);`
	private static final MethodHandle MH_FLUSH_WAL;
	/// `const rocksdb_snapshot_t* rocksdb_create_snapshot(rocksdb_t* db);`
	private static final MethodHandle MH_CREATE_SNAPSHOT;
	/// `char* rocksdb_property_value(rocksdb_t* db, const char* propname);`
	private static final MethodHandle MH_PROPERTY_VALUE;
	/// `int rocksdb_property_int(rocksdb_t* db, const char* propname, uint64_t* out_val);`
	private static final MethodHandle MH_PROPERTY_INT;
	/// `void rocksdb_delete_range_cf(rocksdb_t* db, const rocksdb_writeoptions_t* options, rocksdb_column_family_handle_t* column_family, const char* start_key, size_t start_key_len, const char* end_key, size_t end_key_len, char** errptr);`
	private static final MethodHandle MH_DELETE_RANGE_CF;
	/// `rocksdb_column_family_handle_t* rocksdb_get_default_column_family_handle(rocksdb_t* db);`
	private static final MethodHandle MH_GET_DEFAULT_CF;
	/// `void rocksdb_write(rocksdb_t* db, const rocksdb_writeoptions_t* options, rocksdb_writebatch_t* batch, char** errptr);`
	private static final MethodHandle MH_WRITE;
	/// `unsigned char rocksdb_key_may_exist(rocksdb_t* db, const rocksdb_readoptions_t* options, const char* key, size_t key_len, char** value, size_t* val_len, const char* timestamp, size_t timestamp_len, unsigned char* value_found);`
	private static final MethodHandle MH_KEY_MAY_EXIST;
	/// `void rocksdb_compact_range(rocksdb_t* db, const char* start_key, size_t start_key_len, const char* limit_key, size_t limit_key_len);`
	private static final MethodHandle MH_COMPACT_RANGE;
	/// `void rocksdb_compact_range_opt(rocksdb_t* db, rocksdb_compactoptions_t* opt, const char* start_key, size_t start_key_len, const char* limit_key, size_t limit_key_len);`
	private static final MethodHandle MH_COMPACT_RANGE_OPT;
	/// `void rocksdb_suggest_compact_range(rocksdb_t* db, const char* start_key, size_t start_key_len, const char* limit_key, size_t limit_key_len, char** errptr);`
	private static final MethodHandle MH_SUGGEST_COMPACT_RANGE;
	/// `void rocksdb_disable_file_deletions(rocksdb_t* db, char** errptr);`
	private static final MethodHandle MH_DISABLE_FILE_DELETIONS;
	/// `void rocksdb_enable_file_deletions(rocksdb_t* db, char** errptr);`
	private static final MethodHandle MH_ENABLE_FILE_DELETIONS;
	/// `void rocksdb_ingest_external_file(rocksdb_t* db, const char* const* file_list, const size_t list_len, const rocksdb_ingestexternalfileoptions_t* opt, char** errptr);`
	private static final MethodHandle MH_INGEST_EXTERNAL_FILE;
	/// `uint64_t rocksdb_get_latest_sequence_number(rocksdb_t* db);`
	private static final MethodHandle MH_GET_LATEST_SEQUENCE_NUMBER;
	/// `rocksdb_wal_iterator_t* rocksdb_get_updates_since(rocksdb_t* db, uint64_t seq_number, const rocksdb_wal_readoptions_t* options, char** errptr);`
	private static final MethodHandle MH_GET_UPDATES_SINCE;
	/// `void rocksdb_cancel_all_background_work(rocksdb_t* db, unsigned char wait);`
	private static final MethodHandle MH_CANCEL_ALL_BACKGROUND_WORK;
	/// `void rocksdb_disable_manual_compaction(rocksdb_t* db);`
	private static final MethodHandle MH_DISABLE_MANUAL_COMPACTION;
	/// `void rocksdb_enable_manual_compaction(rocksdb_t* db);`
	private static final MethodHandle MH_ENABLE_MANUAL_COMPACTION;
	/// `void rocksdb_wait_for_compact(rocksdb_t* db, rocksdb_wait_for_compact_options_t* options, char** errptr);`
	private static final MethodHandle MH_WAIT_FOR_COMPACT;

	// -----------------------------------------------------------------------
	// Column-family method handles
	// -----------------------------------------------------------------------

	/// `rocksdb_t* rocksdb_open_column_families(const rocksdb_options_t* options, const char* name, int num_column_families, const char* const* column_family_names, const rocksdb_options_t* const* column_family_options, rocksdb_column_family_handle_t** column_family_handles, char** errptr);`
	private static final MethodHandle MH_OPEN_CF;
	/// `char** rocksdb_list_column_families(const rocksdb_options_t* options, const char* name, size_t* lencf, char** errptr);`
	private static final MethodHandle MH_LIST_CF;
	/// `void rocksdb_list_column_families_destroy(char** list, size_t len);`
	private static final MethodHandle MH_LIST_CF_DESTROY;
	/// `rocksdb_column_family_handle_t* rocksdb_create_column_family(rocksdb_t* db, const rocksdb_options_t* column_family_options, const char* column_family_name, char** errptr);`
	private static final MethodHandle MH_CREATE_CF;
	/// `void rocksdb_drop_column_family(rocksdb_t* db, rocksdb_column_family_handle_t* handle, char** errptr);`
	private static final MethodHandle MH_DROP_CF;
	/// `void rocksdb_put_cf(rocksdb_t* db, const rocksdb_writeoptions_t* options, rocksdb_column_family_handle_t* column_family, const char* key, size_t keylen, const char* val, size_t vallen, char** errptr);`
	private static final MethodHandle MH_PUT_CF;
	/// `rocksdb_pinnableslice_t* rocksdb_get_pinned_cf(rocksdb_t* db, const rocksdb_readoptions_t* options, rocksdb_column_family_handle_t* column_family, const char* key, size_t keylen, char** errptr);`
	private static final MethodHandle MH_GET_PINNED_CF;
	/// `void rocksdb_delete_cf(rocksdb_t* db, const rocksdb_writeoptions_t* options, rocksdb_column_family_handle_t* column_family, const char* key, size_t keylen, char** errptr);`
	private static final MethodHandle MH_DELETE_CF;
	/// `unsigned char rocksdb_key_may_exist_cf(rocksdb_t* db, const rocksdb_readoptions_t* options, rocksdb_column_family_handle_t* column_family, const char* key, size_t key_len, char** value, size_t* val_len, const char* timestamp, size_t timestamp_len, unsigned char* value_found);`
	private static final MethodHandle MH_KEY_MAY_EXIST_CF;
	/// `rocksdb_iterator_t* rocksdb_create_iterator_cf(rocksdb_t* db, const rocksdb_readoptions_t* options, rocksdb_column_family_handle_t* column_family);`
	private static final MethodHandle MH_CREATE_ITERATOR_CF;
	/// `void rocksdb_flush_cf(rocksdb_t* db, const rocksdb_flushoptions_t* options, rocksdb_column_family_handle_t* column_family, char** errptr);`
	private static final MethodHandle MH_FLUSH_CF;
	/// `char* rocksdb_property_value_cf(rocksdb_t* db, rocksdb_column_family_handle_t* column_family, const char* propname);`
	private static final MethodHandle MH_PROPERTY_VALUE_CF;
	/// `int rocksdb_property_int_cf(rocksdb_t* db, rocksdb_column_family_handle_t* column_family, const char* propname, uint64_t* out_val);`
	private static final MethodHandle MH_PROPERTY_INT_CF;
	/// `rocksdb_t* rocksdb_open_for_read_only_column_families(const rocksdb_options_t* options, const char* name, int num_column_families, const char* const* column_family_names, const rocksdb_options_t* const* column_family_options, rocksdb_column_family_handle_t** column_family_handles, unsigned char error_if_wal_file_exists, char** errptr);`
	private static final MethodHandle MH_OPEN_FOR_READ_ONLY_CF;
	/// `rocksdb_t* rocksdb_open_column_families_with_ttl(const rocksdb_options_t* options, const char* name, int num_column_families, const char* const* column_family_names, const rocksdb_options_t* const* column_family_options, rocksdb_column_family_handle_t** column_family_handles, const int* ttls, char** errptr);`
	private static final MethodHandle MH_OPEN_CF_WITH_TTL;
	/// `rocksdb_transactiondb_t* rocksdb_transactiondb_open_column_families(const rocksdb_options_t* options, const rocksdb_transactiondb_options_t* txn_db_options, const char* name, int num_column_families, const char* const* column_family_names, const rocksdb_options_t* const* column_family_options, rocksdb_column_family_handle_t** column_family_handles, char** errptr);`
	private static final MethodHandle MH_OPEN_TRANSACTION_CF;
	/// `rocksdb_optimistictransactiondb_t* rocksdb_optimistictransactiondb_open_column_families(const rocksdb_options_t* options, const char* name, int num_column_families, const char* const* column_family_names, const rocksdb_options_t* const* column_family_options, rocksdb_column_family_handle_t** column_family_handles, char** errptr);`
	private static final MethodHandle MH_OPEN_OPTIMISTIC_CF;
	/// `rocksdb_t* rocksdb_transactiondb_get_base_db(rocksdb_transactiondb_t* txn_db);`
	private static final MethodHandle MH_TRANSACTION_GET_BASE_DB;
	/// `void rocksdb_free(void* ptr);`
	static final MethodHandle MH_FREE = NativeLibrary.lookup("rocksdb_free",
			FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

	static {
		MH_OPEN = NativeLibrary.lookup("rocksdb_open",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_OPEN_WITH_TTL = NativeLibrary.lookup("rocksdb_open_with_ttl",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.JAVA_INT, ValueLayout.ADDRESS));

		MH_OPEN_FOR_READ_ONLY = NativeLibrary.lookup("rocksdb_open_for_read_only",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.JAVA_BYTE, ValueLayout.ADDRESS));

		MH_OPEN_SECONDARY = NativeLibrary.lookup("rocksdb_open_as_secondary",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_OPEN_TRANSACTION = NativeLibrary.lookup("rocksdb_transactiondb_open",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_OPEN_OPTIMISTIC = NativeLibrary.lookup("rocksdb_optimistictransactiondb_open",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_GET_BASE_DB = NativeLibrary.lookup("rocksdb_optimistictransactiondb_get_base_db",
				FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_CLOSE = NativeLibrary.lookup("rocksdb_close",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

		MH_GET_PINNED = NativeLibrary.lookup("rocksdb_get_pinned",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS));

		MH_PINNABLESLICE_VALUE = NativeLibrary.lookup("rocksdb_pinnableslice_value",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_PINNABLESLICE_DESTROY = NativeLibrary.lookup("rocksdb_pinnableslice_destroy",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

		MH_PUT = NativeLibrary.lookup("rocksdb_put",
				FunctionDescriptor.ofVoid(
						ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS));

		MH_DELETE = NativeLibrary.lookup("rocksdb_delete",
				FunctionDescriptor.ofVoid(
						ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS));

		MH_FLUSH = NativeLibrary.lookup("rocksdb_flush",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_FLUSH_WAL = NativeLibrary.lookup("rocksdb_flush_wal",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_BYTE, ValueLayout.ADDRESS));

		MH_CREATE_SNAPSHOT = NativeLibrary.lookup("rocksdb_create_snapshot",
				FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_PROPERTY_VALUE = NativeLibrary.lookup("rocksdb_property_value",
				FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_PROPERTY_INT = NativeLibrary.lookup("rocksdb_property_int",
				FunctionDescriptor.of(ValueLayout.JAVA_INT,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_DELETE_RANGE_CF = NativeLibrary.lookup("rocksdb_delete_range_cf",
				FunctionDescriptor.ofVoid(
						ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS));

		MH_GET_DEFAULT_CF = NativeLibrary.lookup("rocksdb_get_default_column_family_handle",
				FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_WRITE = NativeLibrary.lookup("rocksdb_write",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_KEY_MAY_EXIST = NativeLibrary.lookup("rocksdb_key_may_exist",
				FunctionDescriptor.of(ValueLayout.JAVA_BYTE,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS));

		MH_COMPACT_RANGE = NativeLibrary.lookup("rocksdb_compact_range",
				FunctionDescriptor.ofVoid(
						ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));

		MH_COMPACT_RANGE_OPT = NativeLibrary.lookup("rocksdb_compact_range_opt",
				FunctionDescriptor.ofVoid(
						ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));

		MH_SUGGEST_COMPACT_RANGE = NativeLibrary.lookup("rocksdb_suggest_compact_range",
				FunctionDescriptor.ofVoid(
						ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS));

		MH_DISABLE_FILE_DELETIONS = NativeLibrary.lookup("rocksdb_disable_file_deletions",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_ENABLE_FILE_DELETIONS = NativeLibrary.lookup("rocksdb_enable_file_deletions",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_INGEST_EXTERNAL_FILE = NativeLibrary.lookup("rocksdb_ingest_external_file",
				FunctionDescriptor.ofVoid(
						ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_GET_LATEST_SEQUENCE_NUMBER = NativeLibrary.lookup("rocksdb_get_latest_sequence_number",
				FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));

		MH_GET_UPDATES_SINCE = NativeLibrary.lookup("rocksdb_get_updates_since",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_CANCEL_ALL_BACKGROUND_WORK = NativeLibrary.lookup("rocksdb_cancel_all_background_work",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_BYTE));

		MH_DISABLE_MANUAL_COMPACTION = NativeLibrary.lookup("rocksdb_disable_manual_compaction",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

		MH_ENABLE_MANUAL_COMPACTION = NativeLibrary.lookup("rocksdb_enable_manual_compaction",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

		MH_WAIT_FOR_COMPACT = NativeLibrary.lookup("rocksdb_wait_for_compact",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_OPEN_CF = NativeLibrary.lookup("rocksdb_open_column_families",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.JAVA_INT,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_LIST_CF = NativeLibrary.lookup("rocksdb_list_column_families",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_LIST_CF_DESTROY = NativeLibrary.lookup("rocksdb_list_column_families_destroy",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));

		MH_CREATE_CF = NativeLibrary.lookup("rocksdb_create_column_family",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_DROP_CF = NativeLibrary.lookup("rocksdb_drop_column_family",
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_PUT_CF = NativeLibrary.lookup("rocksdb_put_cf",
				FunctionDescriptor.ofVoid(
						ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS));

		MH_GET_PINNED_CF = NativeLibrary.lookup("rocksdb_get_pinned_cf",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS));

		MH_DELETE_CF = NativeLibrary.lookup("rocksdb_delete_cf",
				FunctionDescriptor.ofVoid(
						ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS));

		MH_KEY_MAY_EXIST_CF = NativeLibrary.lookup("rocksdb_key_may_exist_cf",
				FunctionDescriptor.of(ValueLayout.JAVA_BYTE,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS));

		MH_CREATE_ITERATOR_CF = NativeLibrary.lookup("rocksdb_create_iterator_cf",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_FLUSH_CF = NativeLibrary.lookup("rocksdb_flush_cf",
				FunctionDescriptor.ofVoid(
						ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_PROPERTY_VALUE_CF = NativeLibrary.lookup("rocksdb_property_value_cf",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_PROPERTY_INT_CF = NativeLibrary.lookup("rocksdb_property_int_cf",
				FunctionDescriptor.of(ValueLayout.JAVA_INT,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_OPEN_FOR_READ_ONLY_CF = NativeLibrary.lookup("rocksdb_open_for_read_only_column_families",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.JAVA_INT,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_BYTE, ValueLayout.ADDRESS));

		MH_OPEN_CF_WITH_TTL = NativeLibrary.lookup("rocksdb_open_column_families_with_ttl",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.JAVA_INT,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_OPEN_TRANSACTION_CF = NativeLibrary.lookup("rocksdb_transactiondb_open_column_families",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.JAVA_INT,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_OPEN_OPTIMISTIC_CF = NativeLibrary.lookup("rocksdb_optimistictransactiondb_open_column_families",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.JAVA_INT,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS));

		MH_TRANSACTION_GET_BASE_DB = NativeLibrary.lookup("rocksdb_transactiondb_get_base_db",
				FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS));
	}

	private RocksDB() {
		// no instances
	}

	// -----------------------------------------------------------------------
	// Factory — read-write
	// -----------------------------------------------------------------------

	/// Opens a read-write database at `path`.
	/// Use [Options#setCreateIfMissing(boolean)] to control behaviour when
	/// the path does not exist.
	///
	/// @param options the database options
	/// @param path directory where the database files are stored
	/// @return a new [ReadWriteDB] instance
	public static ReadWriteDB open(Options options, Path path) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MemorySegment pathSeg = arena.allocateFrom(path.toString());
			MemorySegment ptr = (MemorySegment) MH_OPEN.invokeExact(options.ptr(), pathSeg, err);
			checkError(err);
			return new ReadWriteDB(ptr, WriteOptions.newWriteOptions(), ReadOptions.newReadOptions());
		} catch (Throwable t) {
			throw RocksDBException.wrap("open failed", t);
		}
	}

	/// Equivalent to `open(options, path)` with `createIfMissing = true`.
	///
	/// @param path directory where the database files are stored
	/// @return a new [ReadWriteDB] instance
	public static ReadWriteDB open(Path path) {
		try (Options opts = Options.newOptions().setCreateIfMissing(true)) {
			return open(opts, path);
		}
	}

	/// Opens (or creates) a TTL-aware read-write database at `path`.
	///
	/// Keys are lazily expired during the next compaction that covers their
	/// range. A `ttl` of [Duration#ZERO] disables expiry entirely.
	///
	/// @param options the database options
	/// @param path directory where the database files are stored
	/// @param ttl time-to-live for keys; [Duration#ZERO] disables expiry
	/// @return a new [TtlDB] instance
	public static TtlDB openWithTtl(Options options, Path path, Duration ttl) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MemorySegment pathSeg = arena.allocateFrom(path.toString());
			MemorySegment ptr = (MemorySegment) MH_OPEN_WITH_TTL.invokeExact(
					options.ptr(), pathSeg, (int) ttl.toSeconds(), err);
			checkError(err);
			return new TtlDB(ptr, WriteOptions.newWriteOptions(), ReadOptions.newReadOptions(), ttl);
		} catch (Throwable t) {
			throw RocksDBException.wrap("openWithTtl failed", t);
		}
	}

	/// Equivalent to `openWithTtl(options, path, ttl)` with `createIfMissing = true`.
	///
	/// @param path directory where the database files are stored
	/// @param ttl time-to-live for keys; [Duration#ZERO] disables expiry
	/// @return a new [TtlDB] instance
	public static TtlDB openWithTtl(Path path, Duration ttl) {
		try (Options opts = Options.newOptions().setCreateIfMissing(true)) {
			return openWithTtl(opts, path, ttl);
		}
	}

	/// Opens (or creates) a blob-enabled read-write database at `path`.
	///
	/// BlobDB stores large values (≥ [Options#setMinBlobSize]) in separate blob files,
	/// reducing write amplification for value-heavy workloads.
	/// The caller is responsible for setting [Options#setEnableBlobFiles(boolean)] to `true`
	/// and any other blob options before calling this method.
	///
	/// @param options the database options (must have blob files enabled)
	/// @param path directory where the database files are stored
	/// @return a new [BlobDB] instance
	public static BlobDB openWithBlobFiles(Options options, Path path) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MemorySegment pathSeg = arena.allocateFrom(path.toString());
			MemorySegment ptr = (MemorySegment) MH_OPEN.invokeExact(options.ptr(), pathSeg, err);
			checkError(err);
			return new BlobDB(ptr, WriteOptions.newWriteOptions(), ReadOptions.newReadOptions());
		} catch (Throwable t) {
			throw RocksDBException.wrap("openWithBlobFiles failed", t);
		}
	}

	/// Equivalent to `openWithBlobFiles(options, path)` with `createIfMissing = true`
	/// and `enableBlobFiles = true`.
	///
	/// @param path directory where the database files are stored
	/// @return a new [BlobDB] instance
	public static BlobDB openWithBlobFiles(Path path) {
		try (Options opts = Options.newOptions().setCreateIfMissing(true).setEnableBlobFiles(true)) {
			return openWithBlobFiles(opts, path);
		}
	}

	// -----------------------------------------------------------------------
	// Factory — read-only
	// -----------------------------------------------------------------------

	/// Opens the database at `path` in read-only mode.
	///
	/// @param options the database options
	/// @param path directory where the database files are stored
	/// @param errorIfWalFileExists if `true`, fails when unrecovered WAL files are present
	/// @return a new [ReadOnlyDB] instance
	public static ReadOnlyDB openReadOnly(Options options, Path path, boolean errorIfWalFileExists) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MemorySegment pathSeg = arena.allocateFrom(path.toString());
			MemorySegment ptr = (MemorySegment) MH_OPEN_FOR_READ_ONLY.invokeExact(
					options.ptr(), pathSeg, errorIfWalFileExists ? (byte) 1 : (byte) 0, err);
			checkError(err);
			return new ReadOnlyDB(ptr, ReadOptions.newReadOptions());
		} catch (Throwable t) {
			throw RocksDBException.wrap("openReadOnly failed", t);
		}
	}

	/// Equivalent to `openReadOnly(options, path, false)`.
	///
	/// @param options the database options
	/// @param path directory where the database files are stored
	/// @return a new [ReadOnlyDB] instance
	public static ReadOnlyDB openReadOnly(Options options, Path path) {
		return openReadOnly(options, path, false);
	}

	/// Opens the database at `path` in read-only mode with default options.
	///
	/// @param path directory where the database files are stored
	/// @return a new [ReadOnlyDB] instance
	public static ReadOnlyDB openReadOnly(Path path) {
		try (Options opts = Options.newOptions()) {
			return openReadOnly(opts, path, false);
		}
	}

	// -----------------------------------------------------------------------
	// Factory — secondary
	// -----------------------------------------------------------------------

	/// Opens a secondary (read-only replica) instance of the database at `primaryPath`.
	///
	/// @param options the database options
	/// @param primaryPath directory of the primary database
	/// @param secondaryPath a dedicated directory for this secondary's own MANIFEST/WAL tails
	/// @return a new [SecondaryDB] instance
	public static SecondaryDB openSecondary(Options options, Path primaryPath, Path secondaryPath) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MemorySegment primary = arena.allocateFrom(primaryPath.toString());
			MemorySegment secondary = arena.allocateFrom(secondaryPath.toString());

			MemorySegment ptr = (MemorySegment) MH_OPEN_SECONDARY.invokeExact(
					options.ptr(), primary, secondary, err);
			checkError(err);

			return new SecondaryDB(ptr, ReadOptions.newReadOptions());
		} catch (Throwable t) {
			throw RocksDBException.wrap("openSecondary failed", t);
		}
	}

	// -----------------------------------------------------------------------
	// Factory — transactional
	// -----------------------------------------------------------------------

	/// Opens a [TransactionDB] (pessimistic / locking transactions) at `path`.
	///
	/// @param options the database options
	/// @param txnDbOptions the transaction DB options
	/// @param path directory where the database files are stored
	/// @return a new [TransactionDB] instance
	public static TransactionDB openTransaction(Options options, TransactionDBOptions txnDbOptions, Path path) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MemorySegment pathSeg = arena.allocateFrom(path.toString());

			MemorySegment ptr = (MemorySegment) MH_OPEN_TRANSACTION.invokeExact(
					options.ptr(), txnDbOptions.ptr(), pathSeg, err);
			checkError(err);

			MemorySegment baseDb = (MemorySegment) MH_TRANSACTION_GET_BASE_DB.invokeExact(ptr);
			return new TransactionDB(ptr, baseDb, WriteOptions.newWriteOptions(), ReadOptions.newReadOptions());
		} catch (Throwable t) {
			throw RocksDBException.wrap("openTransaction failed", t);
		}
	}

	/// Opens an [OptimisticTransactionDB] (conflict-detection-at-commit) at `path`.
	///
	/// @param options the database options
	/// @param path directory where the database files are stored
	/// @return a new [OptimisticTransactionDB] instance
	public static OptimisticTransactionDB openOptimistic(Options options, Path path) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MemorySegment pathSeg = arena.allocateFrom(path.toString());

			MemorySegment ptr = (MemorySegment) MH_OPEN_OPTIMISTIC.invokeExact(
					options.ptr(), pathSeg, err);
			checkError(err);

			MemorySegment baseDb = (MemorySegment) MH_GET_BASE_DB.invokeExact(ptr);
			return new OptimisticTransactionDB(ptr, baseDb, WriteOptions.newWriteOptions(), ReadOptions.newReadOptions());
		} catch (Throwable t) {
			throw RocksDBException.wrap("openOptimistic failed", t);
		}
	}

	// -----------------------------------------------------------------------
	// Package-private shared helpers — rocksdb_t* operations, mapped once
	// -----------------------------------------------------------------------

	/// byte[] get via PinnableSlice — zero-copy from block cache. Returns `null` if not found.
	static byte[] getBytes(MemorySegment db, MemorySegment readOpts, byte[] key) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MemorySegment k = toNative(arena, key);
			MemorySegment pin = (MemorySegment) MH_GET_PINNED.invokeExact(db, readOpts, k, (long) key.length, err);
			checkError(err);
			if (MemorySegment.NULL.equals(pin)) {
				return null;
			}
			MemorySegment valLenSeg = arena.allocate(ValueLayout.JAVA_LONG);
			MemorySegment valPtr = (MemorySegment) MH_PINNABLESLICE_VALUE.invokeExact(pin, valLenSeg);
			long valLen = valLenSeg.get(ValueLayout.JAVA_LONG, 0);
			byte[] result = valPtr.reinterpret(valLen).toArray(ValueLayout.JAVA_BYTE);
			MH_PINNABLESLICE_DESTROY.invokeExact(pin);
			return result;
		} catch (Throwable t) {
			throw RocksDBException.wrap("get failed", t);
		}
	}

	/// ByteBuffer get via PinnableSlice — copies once into the caller's buffer.
	/// Returns actual value length, or -1 if not found.
	static int getIntoBuffer(MemorySegment db, MemorySegment readOpts, MemorySegment key, long keyLen, ByteBuffer value) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MemorySegment pin = (MemorySegment) MH_GET_PINNED.invokeExact(db, readOpts, key, keyLen, err);
			checkError(err);
			if (MemorySegment.NULL.equals(pin)) {
				return -1;
			}
			MemorySegment valLenSeg = arena.allocate(ValueLayout.JAVA_LONG);
			MemorySegment valPtr = (MemorySegment) MH_PINNABLESLICE_VALUE.invokeExact(pin, valLenSeg);
			long valLen = valLenSeg.get(ValueLayout.JAVA_LONG, 0);
			int toCopy = (int) Math.min(valLen, value.remaining());
			MemorySegment.ofBuffer(value).copyFrom(valPtr.reinterpret(toCopy));
			value.position(value.position() + toCopy);
			MH_PINNABLESLICE_DESTROY.invokeExact(pin);
			return (int) valLen;
		} catch (Throwable t) {
			throw RocksDBException.wrap("get failed", t);
		}
	}

	/// MemorySegment get via PinnableSlice — copies once into the caller's segment.
	/// Returns actual value length.
	static long getIntoSegment(MemorySegment db, MemorySegment readOpts, MemorySegment key, long keyLen, MemorySegment value) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MemorySegment pin = (MemorySegment) MH_GET_PINNED.invokeExact(db, readOpts, key, keyLen, err);
			checkError(err);
			MemorySegment valLenSeg = arena.allocate(ValueLayout.JAVA_LONG);
			MemorySegment valPtr = (MemorySegment) MH_PINNABLESLICE_VALUE.invokeExact(pin, valLenSeg);
			long valLen = valLenSeg.get(ValueLayout.JAVA_LONG, 0);
			long toCopy = Math.min(valLen, value.byteSize());
			value.copyFrom(valPtr.reinterpret(toCopy));
			MH_PINNABLESLICE_DESTROY.invokeExact(pin);
			return valLen;
		} catch (Throwable t) {
			throw RocksDBException.wrap("get failed", t);
		}
	}

	/// byte[] put — slow path, allocates native memory.
	static void putBytes(MemorySegment db, MemorySegment writeOpts, byte[] key, byte[] value) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MemorySegment k = toNative(arena, key);
			MemorySegment v = toNative(arena, value);
			MH_PUT.invokeExact(db, writeOpts, k, (long) key.length, v, (long) value.length, err);
			checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("put failed", t);
		}
	}

	/// byte[] put using the caller's arena.
	static void putBytes(Arena arena, MemorySegment db, MemorySegment writeOpts, byte[] key, byte[] value) {
		try {
			MemorySegment err = errHolder(arena);
			MemorySegment k = toNative(arena, key);
			MemorySegment v = toNative(arena, value);
			MH_PUT.invokeExact(db, writeOpts, k, (long) key.length, v, (long) value.length, err);
			checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("put failed", t);
		}
	}

	/// MemorySegment put — zero-copy, caller supplies pre-allocated native segments.
	static void putSegment(MemorySegment db, MemorySegment writeOpts,
	                       MemorySegment key, long keyLen, MemorySegment val, long valLen) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MH_PUT.invokeExact(db, writeOpts, key, keyLen, val, valLen, err);
			checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("put failed", t);
		}
	}

	/// MemorySegment put using the caller's arena.
	static void putSegment(Arena arena, MemorySegment db, MemorySegment writeOpts,
	                       MemorySegment key, long keyLen, MemorySegment val, long valLen) {
		try {
			MemorySegment err = errHolder(arena);
			MH_PUT.invokeExact(db, writeOpts, key, keyLen, val, valLen, err);
			checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("put failed", t);
		}
	}

	/// byte[] delete — slow path.
	static void deleteBytes(MemorySegment db, MemorySegment writeOpts, byte[] key) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MemorySegment k = toNative(arena, key);
			MH_DELETE.invokeExact(db, writeOpts, k, (long) key.length, err);
			checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("delete failed", t);
		}
	}

	/// MemorySegment delete — zero-copy.
	static void deleteSegment(MemorySegment db, MemorySegment writeOpts, MemorySegment key, long keyLen) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MH_DELETE.invokeExact(db, writeOpts, key, keyLen, err);
			checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("delete failed", t);
		}
	}

	static void flush(MemorySegment db, FlushOptions flushOptions) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MH_FLUSH.invokeExact(db, flushOptions.ptr(), err);
			checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("flush failed", t);
		}
	}

	static void cancelAllBackgroundWork(MemorySegment db, boolean wait) {
		try {
			MH_CANCEL_ALL_BACKGROUND_WORK.invokeExact(db, wait ? (byte) 1 : (byte) 0);
		} catch (Throwable t) {
			throw RocksDBException.wrap("cancelAllBackgroundWork failed", t);
		}
	}

	static void disableManualCompaction(MemorySegment db) {
		try {
			MH_DISABLE_MANUAL_COMPACTION.invokeExact(db);
		} catch (Throwable t) {
			throw RocksDBException.wrap("disableManualCompaction failed", t);
		}
	}

	static void enableManualCompaction(MemorySegment db) {
		try {
			MH_ENABLE_MANUAL_COMPACTION.invokeExact(db);
		} catch (Throwable t) {
			throw RocksDBException.wrap("enableManualCompaction failed", t);
		}
	}

	static void waitForCompact(MemorySegment db, WaitForCompactOptions options) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MH_WAIT_FOR_COMPACT.invokeExact(db, options.ptr(), err);
			checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("waitForCompact failed", t);
		}
	}

	static SequenceNumber getLatestSequenceNumber(MemorySegment db) {
		try {
			long seq = (long) MH_GET_LATEST_SEQUENCE_NUMBER.invokeExact(db);
			return SequenceNumber.of(seq);
		} catch (Throwable t) {
			throw RocksDBException.wrap("getLatestSequenceNumber failed", t);
		}
	}

	static WalIterator getUpdatesSince(MemorySegment db, SequenceNumber sequenceNumber) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MemorySegment iterPtr = (MemorySegment) MH_GET_UPDATES_SINCE.invokeExact(
					db, sequenceNumber.toLong(), MemorySegment.NULL, err);
			checkError(err);
			return WalIterator.wrap(iterPtr);
		} catch (Throwable t) {
			throw RocksDBException.wrap("getUpdatesSince failed", t);
		}
	}

	static void flushWal(MemorySegment db, boolean sync) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MH_FLUSH_WAL.invokeExact(db, sync ? (byte) 1 : (byte) 0, err);
			checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("flushWal failed", t);
		}
	}

	static Snapshot createSnapshot(MemorySegment db) {
		try {
			MemorySegment snapPtr = (MemorySegment) MH_CREATE_SNAPSHOT.invokeExact(db);
			return new Snapshot(db, snapPtr);
		} catch (Throwable t) {
			throw RocksDBException.wrap("getSnapshot failed", t);
		}
	}

	static Optional<String> getProperty(MemorySegment db, Property property) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment propSeg = arena.allocateFrom(property.propertyName());
			MemorySegment result = (MemorySegment) MH_PROPERTY_VALUE.invokeExact(db, propSeg);
			if (MemorySegment.NULL.equals(result)) {
				return Optional.empty();
			}
			String value = result.reinterpret(Long.MAX_VALUE).getString(0);
			free(result);
			return Optional.of(value);
		} catch (Throwable t) {
			throw RocksDBException.wrap("getProperty failed", t);
		}
	}

	static OptionalLong getLongProperty(MemorySegment db, Property property) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment propSeg = arena.allocateFrom(property.propertyName());
			MemorySegment out = arena.allocate(ValueLayout.JAVA_LONG);
			int rc = (int) MH_PROPERTY_INT.invokeExact(db, propSeg, out);
			if (rc != 0) {
				return OptionalLong.empty();
			}
			return OptionalLong.of(out.get(ValueLayout.JAVA_LONG, 0));
		} catch (Throwable t) {
			throw RocksDBException.wrap("getLongProperty failed", t);
		}
	}

	static void close(MemorySegment db) throws Throwable {
		MH_CLOSE.invokeExact(db);
	}

	static void deleteRangeCfBytes(MemorySegment db, MemorySegment writeOpts, byte[] startKey, byte[] endKey) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MemorySegment cf = (MemorySegment) MH_GET_DEFAULT_CF.invokeExact(db);
			MH_DELETE_RANGE_CF.invokeExact(db, writeOpts, cf,
					toNative(arena, startKey), (long) startKey.length,
					toNative(arena, endKey), (long) endKey.length, err);
			checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("deleteRange failed", t);
		}
	}

	static void deleteRangeCfBuffer(MemorySegment db, MemorySegment writeOpts,
	                                ByteBuffer startKey, ByteBuffer endKey) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MemorySegment cf = (MemorySegment) MH_GET_DEFAULT_CF.invokeExact(db);
			MH_DELETE_RANGE_CF.invokeExact(db, writeOpts, cf,
					MemorySegment.ofBuffer(startKey), (long) startKey.remaining(),
					MemorySegment.ofBuffer(endKey), (long) endKey.remaining(), err);
			checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("deleteRange failed", t);
		}
	}

	static void deleteRangeCfSegment(MemorySegment db, MemorySegment writeOpts,
	                                 MemorySegment startKey, MemorySegment endKey) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MemorySegment cf = (MemorySegment) MH_GET_DEFAULT_CF.invokeExact(db);
			MH_DELETE_RANGE_CF.invokeExact(db, writeOpts, cf,
					startKey, startKey.byteSize(), endKey, endKey.byteSize(), err);
			checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("deleteRange failed", t);
		}
	}

	static void writeBatch(MemorySegment db, MemorySegment writeOpts, WriteBatch batch) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MH_WRITE.invokeExact(db, writeOpts, batch.ptr(), err);
			checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("write failed", t);
		}
	}

	static void writeBatch(Arena arena, MemorySegment db, MemorySegment writeOpts, WriteBatch batch) {
		try {
			MemorySegment err = errHolder(arena);
			MH_WRITE.invokeExact(db, writeOpts, batch.ptr(), err);
			checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("write failed", t);
		}
	}

	static boolean keyMayExistSegment(MemorySegment db, MemorySegment roOpts,
	                                  MemorySegment key, long keyLen) throws Throwable {
		return ((byte) MH_KEY_MAY_EXIST.invokeExact(db, roOpts, key, keyLen,
				MemorySegment.NULL, MemorySegment.NULL,
				MemorySegment.NULL, 0L, MemorySegment.NULL)) != 0;
	}

	static void compactRangeBytes(MemorySegment db, byte[] startKey, byte[] endKey) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment s = startKey == null ? MemorySegment.NULL : toNative(arena, startKey);
			MemorySegment e = endKey == null ? MemorySegment.NULL : toNative(arena, endKey);
			MH_COMPACT_RANGE.invokeExact(db,
					s, startKey == null ? 0L : (long) startKey.length,
					e, endKey == null ? 0L : (long) endKey.length);
		} catch (Throwable t) {
			throw RocksDBException.wrap("compactRange failed", t);
		}
	}

	static void compactRangeBuffer(MemorySegment db, ByteBuffer startKey, ByteBuffer endKey) {
		try {
			MemorySegment s = startKey == null ? MemorySegment.NULL : MemorySegment.ofBuffer(startKey);
			MemorySegment e = endKey == null ? MemorySegment.NULL : MemorySegment.ofBuffer(endKey);
			MH_COMPACT_RANGE.invokeExact(db,
					s, startKey == null ? 0L : (long) startKey.remaining(),
					e, endKey == null ? 0L : (long) endKey.remaining());
		} catch (Throwable t) {
			throw RocksDBException.wrap("compactRange failed", t);
		}
	}

	static void compactRangeSegment(MemorySegment db, MemorySegment startKey, MemorySegment endKey) {
		try {
			MemorySegment s = startKey == null ? MemorySegment.NULL : startKey;
			MemorySegment e = endKey == null ? MemorySegment.NULL : endKey;
			MH_COMPACT_RANGE.invokeExact(db,
					s, s == MemorySegment.NULL ? 0L : s.byteSize(),
					e, e == MemorySegment.NULL ? 0L : e.byteSize());
		} catch (Throwable t) {
			throw RocksDBException.wrap("compactRange failed", t);
		}
	}

	static void compactRangeOptBytes(MemorySegment db, CompactOptions opts, byte[] startKey, byte[] endKey) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment s = startKey == null ? MemorySegment.NULL : toNative(arena, startKey);
			MemorySegment e = endKey == null ? MemorySegment.NULL : toNative(arena, endKey);
			MH_COMPACT_RANGE_OPT.invokeExact(db, opts.ptr(),
					s, startKey == null ? 0L : (long) startKey.length,
					e, endKey == null ? 0L : (long) endKey.length);
		} catch (Throwable t) {
			throw RocksDBException.wrap("compactRange failed", t);
		}
	}

	static void suggestCompactRangeBytes(MemorySegment db, byte[] startKey, byte[] endKey) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MemorySegment s = startKey == null ? MemorySegment.NULL : toNative(arena, startKey);
			MemorySegment e = endKey == null ? MemorySegment.NULL : toNative(arena, endKey);
			MH_SUGGEST_COMPACT_RANGE.invokeExact(db,
					s, startKey == null ? 0L : (long) startKey.length,
					e, endKey == null ? 0L : (long) endKey.length, err);
			checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("suggestCompactRange failed", t);
		}
	}

	static void disableFileDeletions(MemorySegment db) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MH_DISABLE_FILE_DELETIONS.invokeExact(db, err);
			checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("disableFileDeletions failed", t);
		}
	}

	static void enableFileDeletions(MemorySegment db) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MH_ENABLE_FILE_DELETIONS.invokeExact(db, err);
			checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("enableFileDeletions failed", t);
		}
	}

	static void ingestExternalFile(MemorySegment db, List<Path> files, IngestExternalFileOptions options) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MemorySegment fileArray = arena.allocate(ValueLayout.ADDRESS, files.size());
			for (int i = 0; i < files.size(); i++) {
				fileArray.setAtIndex(ValueLayout.ADDRESS, i, arena.allocateFrom(files.get(i).toString()));
			}
			MH_INGEST_EXTERNAL_FILE.invokeExact(db, fileArray, (long) files.size(), options.ptr(), err);
			checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("ingestExternalFile failed", t);
		}
	}

	// -----------------------------------------------------------------------
	// Factory — column families
	// -----------------------------------------------------------------------

	/// Opens a read-write database at `path` with multiple column families.
	///
	/// The `descriptors` list must include a descriptor for every existing column family in the
	/// database, including the default column family (`"default"`). The `handles` list is cleared
	/// and populated with one [ColumnFamilyHandle] per descriptor, in the same order. The caller
	/// is responsible for closing each handle.
	///
	/// @param options the database options
	/// @param path directory where the database files are stored
	/// @param descriptors one descriptor per column family (must include `"default"`)
	/// @param handles output list populated with one handle per descriptor
	/// @return a new [ReadWriteDB] instance
	public static ReadWriteDB openWithColumnFamilies(Options options, Path path,
	                                                 List<ColumnFamilyDescriptor> descriptors,
	                                                 List<ColumnFamilyHandle> handles) {
		int n = descriptors.size();
		List<Options> tempOptions = new ArrayList<>();
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MemorySegment pathSeg = arena.allocateFrom(path.toString());

			MemorySegment namesArr = arena.allocate(ValueLayout.ADDRESS, n);
			MemorySegment optsArr = arena.allocate(ValueLayout.ADDRESS, n);
			MemorySegment handlesArr = arena.allocate(ValueLayout.ADDRESS, n);

			for (int i = 0; i < n; i++) {
				ColumnFamilyDescriptor desc = descriptors.get(i);
				namesArr.setAtIndex(ValueLayout.ADDRESS, i,
						arena.allocateFrom(new String(desc.name(), StandardCharsets.UTF_8)));
				Options cfOpts = desc.options();
				if (cfOpts == null) {
					cfOpts = Options.newOptions();
					tempOptions.add(cfOpts);
				}
				optsArr.setAtIndex(ValueLayout.ADDRESS, i, cfOpts.ptr());
			}

			MemorySegment ptr = (MemorySegment) MH_OPEN_CF.invokeExact(
					options.ptr(), pathSeg, n, namesArr, optsArr, handlesArr, err);
			checkError(err);

			handles.clear();
			for (int i = 0; i < n; i++) {
				handles.add(ColumnFamilyHandle.wrap(handlesArr.getAtIndex(ValueLayout.ADDRESS, i)));
			}

			return new ReadWriteDB(ptr, WriteOptions.newWriteOptions(), ReadOptions.newReadOptions());
		} catch (Throwable t) {
			throw RocksDBException.wrap("openWithColumnFamilies failed", t);
		} finally {
			for (Options o : tempOptions) {
				o.close();
			}
		}
	}

	/// Opens a read-only database at `path` with multiple column families.
	///
	/// The `handles` list is cleared and populated with one [ColumnFamilyHandle] per descriptor.
	///
	/// @param options the database options
	/// @param path directory where the database files are stored
	/// @param descriptors one descriptor per column family (must include `"default"`)
	/// @param handles output list populated with one handle per descriptor
	/// @return a new [ReadOnlyDB] instance
	public static ReadOnlyDB openReadOnlyWithColumnFamilies(Options options, Path path,
	                                                        List<ColumnFamilyDescriptor> descriptors,
	                                                        List<ColumnFamilyHandle> handles) {
		return openReadOnlyWithColumnFamilies(options, path, descriptors, handles, false);
	}

	/// Opens a read-only database at `path` with multiple column families.
	///
	/// @param options the database options
	/// @param path directory where the database files are stored
	/// @param descriptors one descriptor per column family (must include `"default"`)
	/// @param handles output list populated with one handle per descriptor
	/// @param errorIfWalFileExists if `true`, fails when unrecovered WAL files are present
	/// @return a new [ReadOnlyDB] instance
	public static ReadOnlyDB openReadOnlyWithColumnFamilies(Options options, Path path,
	                                                        List<ColumnFamilyDescriptor> descriptors,
	                                                        List<ColumnFamilyHandle> handles,
	                                                        boolean errorIfWalFileExists) {
		int n = descriptors.size();
		List<Options> tempOptions = new ArrayList<>();
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MemorySegment pathSeg = arena.allocateFrom(path.toString());
			MemorySegment namesArr = arena.allocate(ValueLayout.ADDRESS, n);
			MemorySegment optsArr = arena.allocate(ValueLayout.ADDRESS, n);
			MemorySegment handlesArr = arena.allocate(ValueLayout.ADDRESS, n);
			for (int i = 0; i < n; i++) {
				ColumnFamilyDescriptor desc = descriptors.get(i);
				namesArr.setAtIndex(ValueLayout.ADDRESS, i,
						arena.allocateFrom(new String(desc.name(), StandardCharsets.UTF_8)));
				Options cfOpts = desc.options();
				if (cfOpts == null) {
					cfOpts = Options.newOptions();
					tempOptions.add(cfOpts);
				}
				optsArr.setAtIndex(ValueLayout.ADDRESS, i, cfOpts.ptr());
			}
			MemorySegment ptr = (MemorySegment) MH_OPEN_FOR_READ_ONLY_CF.invokeExact(
					options.ptr(), pathSeg, n, namesArr, optsArr, handlesArr,
					errorIfWalFileExists ? (byte) 1 : (byte) 0, err);
			checkError(err);
			handles.clear();
			for (int i = 0; i < n; i++) {
				handles.add(ColumnFamilyHandle.wrap(handlesArr.getAtIndex(ValueLayout.ADDRESS, i)));
			}
			return new ReadOnlyDB(ptr, ReadOptions.newReadOptions());
		} catch (Throwable t) {
			throw RocksDBException.wrap("openReadOnlyWithColumnFamilies failed", t);
		} finally {
			for (Options o : tempOptions) {
				o.close();
			}
		}
	}

	/// Opens a TTL-aware read-write database at `path` with multiple column families.
	///
	/// Each column family is paired with its own TTL from `ttls` (index-aligned with `descriptors`).
	/// A TTL of [Duration#ZERO] disables expiry for that column family.
	/// The `handles` list is cleared and populated with one [ColumnFamilyHandle] per descriptor.
	///
	/// @param options     database-level options
	/// @param path        path to the database directory
	/// @param descriptors column family descriptors (name + optional per-CF options)
	/// @param ttls        per-column-family TTLs, index-aligned with `descriptors`
	/// @param handles     output list; cleared and filled with one handle per descriptor
	/// @return an open [TtlDB] instance; caller must close it
	public static TtlDB openWithColumnFamiliesAndTtl(Options options, Path path,
	                                                  List<ColumnFamilyDescriptor> descriptors,
	                                                  List<Duration> ttls,
	                                                  List<ColumnFamilyHandle> handles) {
		int n = descriptors.size();
		List<Options> tempOptions = new ArrayList<>();
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MemorySegment pathSeg = arena.allocateFrom(path.toString());
			MemorySegment namesArr = arena.allocate(ValueLayout.ADDRESS, n);
			MemorySegment optsArr = arena.allocate(ValueLayout.ADDRESS, n);
			MemorySegment handlesArr = arena.allocate(ValueLayout.ADDRESS, n);
			MemorySegment ttlsArr = arena.allocate(ValueLayout.JAVA_INT, n);
			for (int i = 0; i < n; i++) {
				ColumnFamilyDescriptor desc = descriptors.get(i);
				namesArr.setAtIndex(ValueLayout.ADDRESS, i,
						arena.allocateFrom(new String(desc.name(), StandardCharsets.UTF_8)));
				Options cfOpts = desc.options();
				if (cfOpts == null) {
					cfOpts = Options.newOptions();
					tempOptions.add(cfOpts);
				}
				optsArr.setAtIndex(ValueLayout.ADDRESS, i, cfOpts.ptr());
				ttlsArr.setAtIndex(ValueLayout.JAVA_INT, i, (int) ttls.get(i).toSeconds());
			}
			MemorySegment ptr = (MemorySegment) MH_OPEN_CF_WITH_TTL.invokeExact(
					options.ptr(), pathSeg, n, namesArr, optsArr, handlesArr, ttlsArr, err);
			checkError(err);
			handles.clear();
			for (int i = 0; i < n; i++) {
				handles.add(ColumnFamilyHandle.wrap(handlesArr.getAtIndex(ValueLayout.ADDRESS, i)));
			}
			Duration globalTtl = ttls.isEmpty() ? Duration.ZERO : ttls.get(0);
			return new TtlDB(ptr, WriteOptions.newWriteOptions(), ReadOptions.newReadOptions(), globalTtl);
		} catch (Throwable t) {
			throw RocksDBException.wrap("openWithColumnFamiliesAndTtl failed", t);
		} finally {
			for (Options o : tempOptions) {
				o.close();
			}
		}
	}

	/// Opens a [TransactionDB] at `path` with multiple column families.
	///
	/// The `handles` list is cleared and populated with one [ColumnFamilyHandle] per descriptor.
	///
	/// @param options       database-level options
	/// @param txnDbOptions  transaction database options
	/// @param path          path to the database directory
	/// @param descriptors   column family descriptors (name + optional per-CF options)
	/// @param handles       output list; cleared and filled with one handle per descriptor
	/// @return an open [TransactionDB] instance; caller must close it
	public static TransactionDB openTransactionWithColumnFamilies(Options options,
	                                                               TransactionDBOptions txnDbOptions,
	                                                               Path path,
	                                                               List<ColumnFamilyDescriptor> descriptors,
	                                                               List<ColumnFamilyHandle> handles) {
		int n = descriptors.size();
		List<Options> tempOptions = new ArrayList<>();
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MemorySegment pathSeg = arena.allocateFrom(path.toString());
			MemorySegment namesArr = arena.allocate(ValueLayout.ADDRESS, n);
			MemorySegment optsArr = arena.allocate(ValueLayout.ADDRESS, n);
			MemorySegment handlesArr = arena.allocate(ValueLayout.ADDRESS, n);
			for (int i = 0; i < n; i++) {
				ColumnFamilyDescriptor desc = descriptors.get(i);
				namesArr.setAtIndex(ValueLayout.ADDRESS, i,
						arena.allocateFrom(new String(desc.name(), StandardCharsets.UTF_8)));
				Options cfOpts = desc.options();
				if (cfOpts == null) {
					cfOpts = Options.newOptions();
					tempOptions.add(cfOpts);
				}
				optsArr.setAtIndex(ValueLayout.ADDRESS, i, cfOpts.ptr());
			}
			MemorySegment ptr = (MemorySegment) MH_OPEN_TRANSACTION_CF.invokeExact(
					options.ptr(), txnDbOptions.ptr(), pathSeg, n, namesArr, optsArr, handlesArr, err);
			checkError(err);
			handles.clear();
			for (int i = 0; i < n; i++) {
				handles.add(ColumnFamilyHandle.wrap(handlesArr.getAtIndex(ValueLayout.ADDRESS, i)));
			}
			MemorySegment baseDb = (MemorySegment) MH_TRANSACTION_GET_BASE_DB.invokeExact(ptr);
			return new TransactionDB(ptr, baseDb, WriteOptions.newWriteOptions(), ReadOptions.newReadOptions());
		} catch (Throwable t) {
			throw RocksDBException.wrap("openTransactionWithColumnFamilies failed", t);
		} finally {
			for (Options o : tempOptions) {
				o.close();
			}
		}
	}

	/// Opens an [OptimisticTransactionDB] at `path` with multiple column families.
	///
	/// The `handles` list is cleared and populated with one [ColumnFamilyHandle] per descriptor.
	///
	/// @param options     database-level options
	/// @param path        path to the database directory
	/// @param descriptors column family descriptors (name + optional per-CF options)
	/// @param handles     output list; cleared and filled with one handle per descriptor
	/// @return an open [OptimisticTransactionDB] instance; caller must close it
	public static OptimisticTransactionDB openOptimisticWithColumnFamilies(Options options, Path path,
	                                                                        List<ColumnFamilyDescriptor> descriptors,
	                                                                        List<ColumnFamilyHandle> handles) {
		int n = descriptors.size();
		List<Options> tempOptions = new ArrayList<>();
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MemorySegment pathSeg = arena.allocateFrom(path.toString());
			MemorySegment namesArr = arena.allocate(ValueLayout.ADDRESS, n);
			MemorySegment optsArr = arena.allocate(ValueLayout.ADDRESS, n);
			MemorySegment handlesArr = arena.allocate(ValueLayout.ADDRESS, n);
			for (int i = 0; i < n; i++) {
				ColumnFamilyDescriptor desc = descriptors.get(i);
				namesArr.setAtIndex(ValueLayout.ADDRESS, i,
						arena.allocateFrom(new String(desc.name(), StandardCharsets.UTF_8)));
				Options cfOpts = desc.options();
				if (cfOpts == null) {
					cfOpts = Options.newOptions();
					tempOptions.add(cfOpts);
				}
				optsArr.setAtIndex(ValueLayout.ADDRESS, i, cfOpts.ptr());
			}
			MemorySegment ptr = (MemorySegment) MH_OPEN_OPTIMISTIC_CF.invokeExact(
					options.ptr(), pathSeg, n, namesArr, optsArr, handlesArr, err);
			checkError(err);
			handles.clear();
			for (int i = 0; i < n; i++) {
				handles.add(ColumnFamilyHandle.wrap(handlesArr.getAtIndex(ValueLayout.ADDRESS, i)));
			}
			MemorySegment baseDb = (MemorySegment) MH_GET_BASE_DB.invokeExact(ptr);
			return new OptimisticTransactionDB(ptr, baseDb, WriteOptions.newWriteOptions(), ReadOptions.newReadOptions());
		} catch (Throwable t) {
			throw RocksDBException.wrap("openOptimisticWithColumnFamilies failed", t);
		} finally {
			for (Options o : tempOptions) {
				o.close();
			}
		}
	}

	/// Lists the names of all column families in the database at `path`.
	///
	/// @param options database-level options used to open the database metadata
	/// @param path    path to the database directory
	/// @return list of column family names as raw byte arrays
	public static List<byte[]> listColumnFamilies(Options options, Path path) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MemorySegment pathSeg = arena.allocateFrom(path.toString());
			MemorySegment lenSeg = arena.allocate(ValueLayout.JAVA_LONG);

			MemorySegment namesPtr = (MemorySegment) MH_LIST_CF.invokeExact(
					options.ptr(), pathSeg, lenSeg, err);
			checkError(err);

			long count = lenSeg.get(ValueLayout.JAVA_LONG, 0);
			List<byte[]> result = new ArrayList<>((int) count);
			MemorySegment namesArr = namesPtr.reinterpret(ValueLayout.ADDRESS.byteSize() * count);
			for (int i = 0; i < count; i++) {
				MemorySegment namePtr = namesArr.getAtIndex(ValueLayout.ADDRESS, i);
				result.add(namePtr.reinterpret(Long.MAX_VALUE).getString(0)
						.getBytes(StandardCharsets.UTF_8));
			}
			MH_LIST_CF_DESTROY.invokeExact(namesPtr, count);
			return result;
		} catch (Throwable t) {
			throw RocksDBException.wrap("listColumnFamilies failed", t);
		}
	}

	// -----------------------------------------------------------------------
	// Package-private CF helpers
	// -----------------------------------------------------------------------

	static ColumnFamilyHandle createCf(MemorySegment db, ColumnFamilyDescriptor descriptor) {
		List<Options> tempOptions = new ArrayList<>(1);
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			Options cfOpts = descriptor.options();
			if (cfOpts == null) {
				cfOpts = Options.newOptions();
				tempOptions.add(cfOpts);
			}
			MemorySegment nameSeg = arena.allocateFrom(
					new String(descriptor.name(), StandardCharsets.UTF_8));
			MemorySegment handle = (MemorySegment) MH_CREATE_CF.invokeExact(
					db, cfOpts.ptr(), nameSeg, err);
			checkError(err);
			return ColumnFamilyHandle.wrap(handle);
		} catch (Throwable t) {
			throw RocksDBException.wrap("createColumnFamily failed", t);
		} finally {
			for (Options o : tempOptions) {
				o.close();
			}
		}
	}

	static void dropCf(MemorySegment db, ColumnFamilyHandle handle) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MH_DROP_CF.invokeExact(db, handle.ptr(), err);
			checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("dropColumnFamily failed", t);
		}
	}

	/// byte[] put with explicit column family — slow path.
	static void putCfBytes(MemorySegment db, MemorySegment writeOpts, ColumnFamilyHandle cf,
	                       byte[] key, byte[] value) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MH_PUT_CF.invokeExact(db, writeOpts, cf.ptr(),
					toNative(arena, key), (long) key.length,
					toNative(arena, value), (long) value.length, err);
			checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("put failed", t);
		}
	}

	/// MemorySegment put with explicit column family — zero-copy.
	static void putCfSegment(MemorySegment db, MemorySegment writeOpts, ColumnFamilyHandle cf,
	                         MemorySegment key, long keyLen, MemorySegment val, long valLen) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MH_PUT_CF.invokeExact(db, writeOpts, cf.ptr(), key, keyLen, val, valLen, err);
			checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("put failed", t);
		}
	}

	/// byte[] get with explicit column family via PinnableSlice. Returns `null` if not found.
	static byte[] getCfBytes(MemorySegment db, MemorySegment readOpts, ColumnFamilyHandle cf,
	                         byte[] key) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MemorySegment pin = (MemorySegment) MH_GET_PINNED_CF.invokeExact(
					db, readOpts, cf.ptr(), toNative(arena, key), (long) key.length, err);
			checkError(err);
			if (MemorySegment.NULL.equals(pin)) {
				return null;
			}
			MemorySegment valLenSeg = arena.allocate(ValueLayout.JAVA_LONG);
			MemorySegment valPtr = (MemorySegment) MH_PINNABLESLICE_VALUE.invokeExact(pin, valLenSeg);
			long valLen = valLenSeg.get(ValueLayout.JAVA_LONG, 0);
			byte[] result = valPtr.reinterpret(valLen).toArray(ValueLayout.JAVA_BYTE);
			MH_PINNABLESLICE_DESTROY.invokeExact(pin);
			return result;
		} catch (Throwable t) {
			throw RocksDBException.wrap("get failed", t);
		}
	}

	/// ByteBuffer get with explicit column family via PinnableSlice.
	/// Returns actual value length, or -1 if not found.
	static int getCfIntoBuffer(MemorySegment db, MemorySegment readOpts, ColumnFamilyHandle cf,
	                           MemorySegment key, long keyLen, ByteBuffer value) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MemorySegment pin = (MemorySegment) MH_GET_PINNED_CF.invokeExact(
					db, readOpts, cf.ptr(), key, keyLen, err);
			checkError(err);
			if (MemorySegment.NULL.equals(pin)) {
				return -1;
			}
			MemorySegment valLenSeg = arena.allocate(ValueLayout.JAVA_LONG);
			MemorySegment valPtr = (MemorySegment) MH_PINNABLESLICE_VALUE.invokeExact(pin, valLenSeg);
			long valLen = valLenSeg.get(ValueLayout.JAVA_LONG, 0);
			int toCopy = (int) Math.min(valLen, value.remaining());
			MemorySegment.ofBuffer(value).copyFrom(valPtr.reinterpret(toCopy));
			value.position(value.position() + toCopy);
			MH_PINNABLESLICE_DESTROY.invokeExact(pin);
			return (int) valLen;
		} catch (Throwable t) {
			throw RocksDBException.wrap("get failed", t);
		}
	}

	/// MemorySegment get with explicit column family via PinnableSlice.
	/// Returns actual value length.
	static long getCfIntoSegment(MemorySegment db, MemorySegment readOpts, ColumnFamilyHandle cf,
	                             MemorySegment key, long keyLen, MemorySegment value) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MemorySegment pin = (MemorySegment) MH_GET_PINNED_CF.invokeExact(
					db, readOpts, cf.ptr(), key, keyLen, err);
			checkError(err);
			MemorySegment valLenSeg = arena.allocate(ValueLayout.JAVA_LONG);
			MemorySegment valPtr = (MemorySegment) MH_PINNABLESLICE_VALUE.invokeExact(pin, valLenSeg);
			long valLen = valLenSeg.get(ValueLayout.JAVA_LONG, 0);
			long toCopy = Math.min(valLen, value.byteSize());
			value.copyFrom(valPtr.reinterpret(toCopy));
			MH_PINNABLESLICE_DESTROY.invokeExact(pin);
			return valLen;
		} catch (Throwable t) {
			throw RocksDBException.wrap("get failed", t);
		}
	}

	/// byte[] delete with explicit column family — slow path.
	static void deleteCfBytes(MemorySegment db, MemorySegment writeOpts, ColumnFamilyHandle cf,
	                          byte[] key) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MH_DELETE_CF.invokeExact(db, writeOpts, cf.ptr(),
					toNative(arena, key), (long) key.length, err);
			checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("delete failed", t);
		}
	}

	/// MemorySegment delete with explicit column family — zero-copy.
	static void deleteCfSegment(MemorySegment db, MemorySegment writeOpts, ColumnFamilyHandle cf,
	                            MemorySegment key, long keyLen) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MH_DELETE_CF.invokeExact(db, writeOpts, cf.ptr(), key, keyLen, err);
			checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("delete failed", t);
		}
	}

	/// deleteRange with explicit column family — slow path.
	static void deleteRangeCfBytesExplicit(MemorySegment db, MemorySegment writeOpts,
	                                       ColumnFamilyHandle cf,
	                                       byte[] startKey, byte[] endKey) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MH_DELETE_RANGE_CF.invokeExact(db, writeOpts, cf.ptr(),
					toNative(arena, startKey), (long) startKey.length,
					toNative(arena, endKey), (long) endKey.length, err);
			checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("deleteRange failed", t);
		}
	}

	/// deleteRange with explicit column family — zero-copy for direct ByteBuffers.
	static void deleteRangeCfBufferExplicit(MemorySegment db, MemorySegment writeOpts,
	                                        ColumnFamilyHandle cf,
	                                        ByteBuffer startKey, ByteBuffer endKey) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MH_DELETE_RANGE_CF.invokeExact(db, writeOpts, cf.ptr(),
					MemorySegment.ofBuffer(startKey), (long) startKey.remaining(),
					MemorySegment.ofBuffer(endKey), (long) endKey.remaining(), err);
			checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("deleteRange failed", t);
		}
	}

	/// deleteRange with explicit column family — zero-copy for MemorySegments.
	static void deleteRangeCfSegmentExplicit(MemorySegment db, MemorySegment writeOpts,
	                                         ColumnFamilyHandle cf,
	                                         MemorySegment startKey, MemorySegment endKey) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MH_DELETE_RANGE_CF.invokeExact(db, writeOpts, cf.ptr(),
					startKey, startKey.byteSize(), endKey, endKey.byteSize(), err);
			checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("deleteRange failed", t);
		}
	}

	static boolean keyMayExistCfSegment(MemorySegment db, MemorySegment roOpts,
	                                    ColumnFamilyHandle cf,
	                                    MemorySegment key, long keyLen) throws Throwable {
		return ((byte) MH_KEY_MAY_EXIST_CF.invokeExact(db, roOpts, cf.ptr(), key, keyLen,
				MemorySegment.NULL, MemorySegment.NULL,
				MemorySegment.NULL, 0L, MemorySegment.NULL)) != 0;
	}

	static RocksIterator createIteratorCf(MemorySegment db, MemorySegment readOpts,
	                                      ColumnFamilyHandle cf) {
		try {
			MemorySegment iterPtr = (MemorySegment) MH_CREATE_ITERATOR_CF.invokeExact(
					db, readOpts, cf.ptr());
			return RocksIterator.create(iterPtr);
		} catch (Throwable t) {
			throw RocksDBException.wrap("newIterator failed", t);
		}
	}

	static void flushCf(MemorySegment db, FlushOptions flushOptions, ColumnFamilyHandle cf) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MH_FLUSH_CF.invokeExact(db, flushOptions.ptr(), cf.ptr(), err);
			checkError(err);
		} catch (Throwable t) {
			throw RocksDBException.wrap("flush failed", t);
		}
	}

	static Optional<String> getPropertyCf(MemorySegment db, ColumnFamilyHandle cf,
	                                       Property property) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment propSeg = arena.allocateFrom(property.propertyName());
			MemorySegment result = (MemorySegment) MH_PROPERTY_VALUE_CF.invokeExact(
					db, cf.ptr(), propSeg);
			if (MemorySegment.NULL.equals(result)) {
				return Optional.empty();
			}
			String value = result.reinterpret(Long.MAX_VALUE).getString(0);
			free(result);
			return Optional.of(value);
		} catch (Throwable t) {
			throw RocksDBException.wrap("getProperty failed", t);
		}
	}

	static OptionalLong getLongPropertyCf(MemorySegment db, ColumnFamilyHandle cf,
	                                      Property property) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment propSeg = arena.allocateFrom(property.propertyName());
			MemorySegment out = arena.allocate(ValueLayout.JAVA_LONG);
			int rc = (int) MH_PROPERTY_INT_CF.invokeExact(db, cf.ptr(), propSeg, out);
			if (rc != 0) {
				return OptionalLong.empty();
			}
			return OptionalLong.of(out.get(ValueLayout.JAVA_LONG, 0));
		} catch (Throwable t) {
			throw RocksDBException.wrap("getLongProperty failed", t);
		}
	}

	/// Creates a pre-zeroed error holder in the given arena.
	/// Use this for RocksDB C calls that take `char** errptr`.
	///
	/// @param arena arena to allocate the holder from
	/// @return a zeroed `char**` segment suitable for RocksDB error-out parameters
	public static MemorySegment errHolder(Arena arena) {
		MemorySegment holder = arena.allocate(ValueLayout.ADDRESS);
		holder.set(ValueLayout.ADDRESS, 0, MemorySegment.NULL);
		return holder;
	}

	/// Copies `bytes` into a new native memory segment allocated from `arena`.
	/// No null-terminator is appended; use the byte length when passing to C functions.
	///
	/// @param arena arena to allocate the segment from
	/// @param bytes source bytes to copy
	/// @return native segment containing a copy of `bytes`
	public static MemorySegment toNative(Arena arena, byte[] bytes) {
		MemorySegment seg = arena.allocate(bytes.length);
		// TODO: check if this is better seg.copyFrom(MemorySegment.ofArray(bytes));
		MemorySegment.copy(bytes, 0, seg, ValueLayout.JAVA_BYTE, 0, bytes.length);
		return seg;
	}

	/// Frees a malloc'd pointer returned by the RocksDB C API.
	///
	/// @param ptr pointer to free; must have been allocated by RocksDB
	public static void free(MemorySegment ptr) {
		try {
			MH_FREE.invokeExact(ptr);
		} catch (Throwable ignored) {
			// ignore errors as this is used in destructor-like code
		}
	}

	/// Checks if the error holder contains a non-NULL pointer.
	/// If so, throws a [RocksDBException] and frees the C string.
	///
	/// @param errHolder the `char**` segment previously passed to a RocksDB C call
	public static void checkError(MemorySegment errHolder) {
		MemorySegment errPtr = errHolder.get(ValueLayout.ADDRESS, 0);
		if (!MemorySegment.NULL.equals(errPtr)) {
			String msg = errPtr.reinterpret(Long.MAX_VALUE).getString(0);
			free(errPtr);
			throw new RocksDBException(msg);
		}
	}
}
