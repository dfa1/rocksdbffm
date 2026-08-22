package io.github.dfa1.rocksdbffm;

import java.io.IOException;
import java.io.UncheckedIOException;
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
/// | [#openReadWrite] | [ReadWriteDB] |
/// | [#openReadOnly] | [ReadOnlyDB] |
/// | [#openTtl] | [TtlDB] |
/// | [#openSecondary] | [SecondaryDB] |
/// | [#openBlob] | [BlobDB] |
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
	/// `unsigned char rocksdb_get_into_buffer(rocksdb_t* db, const rocksdb_readoptions_t* options, const char* key, size_t keylen, char* buffer, size_t buffer_size, size_t* vallen, unsigned char* found, char** errptr);`
	private static final MethodHandle MH_GET_INTO_BUFFER;
	/// `rocksdb_pinnable_handle_t* rocksdb_get_pinned_v2(rocksdb_t* db, const rocksdb_readoptions_t* options, const char* key, size_t keylen, char** errptr);`
	private static final MethodHandle MH_GET_PINNED_V2;
	/// `rocksdb_pinnable_handle_t* rocksdb_get_pinned_cf_v2(rocksdb_t* db, const rocksdb_readoptions_t* options, rocksdb_column_family_handle_t* column_family, const char* key, size_t keylen, char** errptr);`
	private static final MethodHandle MH_GET_PINNED_CF_V2;
	/// `void rocksdb_put(rocksdb_t* db, const rocksdb_writeoptions_t* options, const char* key, size_t keylen, const char* val, size_t vallen, char** errptr);`
	private static final MethodHandle MH_PUT;
	/// `void rocksdb_delete(rocksdb_t* db, const rocksdb_writeoptions_t* options, const char* key, size_t keylen, char** errptr);`
	private static final MethodHandle MH_DELETE;
	/// `void rocksdb_merge(rocksdb_t* db, const rocksdb_writeoptions_t* options, const char* key, size_t keylen, const char* val, size_t vallen, char** errptr);`
	private static final MethodHandle MH_MERGE;
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
	/// `unsigned char rocksdb_get_into_buffer_cf(rocksdb_t* db, const rocksdb_readoptions_t* options, rocksdb_column_family_handle_t* column_family, const char* key, size_t keylen, char* buffer, size_t buffer_size, size_t* vallen, unsigned char* found, char** errptr);`
	private static final MethodHandle MH_GET_INTO_BUFFER_CF;
	/// `void rocksdb_delete_cf(rocksdb_t* db, const rocksdb_writeoptions_t* options, rocksdb_column_family_handle_t* column_family, const char* key, size_t keylen, char** errptr);`
	private static final MethodHandle MH_DELETE_CF;
	/// `void rocksdb_merge_cf(rocksdb_t* db, const rocksdb_writeoptions_t* options, rocksdb_column_family_handle_t* column_family, const char* key, size_t keylen, const char* val, size_t vallen, char** errptr);`
	private static final MethodHandle MH_MERGE_CF;
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
	/// `rocksdb_t* rocksdb_open_as_secondary_column_families(const rocksdb_options_t* options, const char* name, const char* secondary_path, int num_column_families, const char* const* column_family_names, const rocksdb_options_t* const* column_family_options, rocksdb_column_family_handle_t** column_family_handles, char** errptr);`
	private static final MethodHandle MH_OPEN_SECONDARY_CF;
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

		MH_GET_INTO_BUFFER = NativeLibrary.lookup("rocksdb_get_into_buffer",
				FunctionDescriptor.of(ValueLayout.JAVA_BYTE,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS));


		// get_pinned_*_v2 can block on disk I/O (a Get), so it must NOT be marked
		// critical: a critical downcall stalls GC for its entire duration.
		MH_GET_PINNED_V2 = NativeLibrary.lookup("rocksdb_get_pinned_v2",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS));

		MH_GET_PINNED_CF_V2 = NativeLibrary.lookup("rocksdb_get_pinned_cf_v2",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS));

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

		MH_MERGE = NativeLibrary.lookup("rocksdb_merge",
				FunctionDescriptor.ofVoid(
						ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
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

		MH_GET_INTO_BUFFER_CF = NativeLibrary.lookup("rocksdb_get_into_buffer_cf",
				FunctionDescriptor.of(ValueLayout.JAVA_BYTE,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS));

		MH_DELETE_CF = NativeLibrary.lookup("rocksdb_delete_cf",
				FunctionDescriptor.ofVoid(
						ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS));

		MH_MERGE_CF = NativeLibrary.lookup("rocksdb_merge_cf",
				FunctionDescriptor.ofVoid(
						ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
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

		MH_OPEN_SECONDARY_CF = NativeLibrary.lookup("rocksdb_open_as_secondary_column_families",
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.JAVA_INT,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS,
						ValueLayout.ADDRESS, ValueLayout.ADDRESS));

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

	// A get/put call only reads the options struct it's given, never mutates it, so one
	// instance safely serves every open DB in the process. Never closed — closing it would
	// break every other DB still using it, so no DB's tryClose may call close() on these.
	static final WriteOptions DEFAULT_WRITE_OPTIONS = WriteOptions.newWriteOptions();
	static final ReadOptions DEFAULT_READ_OPTIONS = ReadOptions.newReadOptions();

	// -----------------------------------------------------------------------
	// Factory — read-write
	// -----------------------------------------------------------------------

	/// Opens a read-write database at `path`.
	/// Use [Options#setCreateIfMissing(boolean)] to control behavior when
	/// the path does not exist.
	///
	/// @param options the database options
	/// @param path directory where the database files are stored
	/// @return a new [ReadWriteDB] instance
	public static ReadWriteDB openReadWrite(Options options, Path path) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MemorySegment pathSeg = arena.allocateFrom(path.toString());
			MemorySegment ptr = (MemorySegment) MH_OPEN.invokeExact(options.ptr(), pathSeg, err);
			checkError(err);
			return new ReadWriteDB(ptr);
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("openReadWrite failed", t);
		}
	}

	/// Equivalent to `openReadWrite(options, path)` with `createIfMissing = true`.
	///
	/// @param path directory where the database files are stored
	/// @return a new [ReadWriteDB] instance
	public static ReadWriteDB openReadWrite(Path path) {
		try (Options opts = Options.newOptions().setCreateIfMissing(true)) {
			return openReadWrite(opts, path);
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
	public static TtlDB openTtl(Options options, Path path, Duration ttl) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MemorySegment pathSeg = arena.allocateFrom(path.toString());
			MemorySegment ptr = (MemorySegment) MH_OPEN_WITH_TTL.invokeExact(
					options.ptr(), pathSeg, (int) ttl.toSeconds(), err);
			checkError(err);
			return new TtlDB(ptr, ttl);
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("openTtl failed", t);
		}
	}

	/// Equivalent to `openTtl(options, path, ttl)` with `createIfMissing = true`.
	///
	/// @param path directory where the database files are stored
	/// @param ttl time-to-live for keys; [Duration#ZERO] disables expiry
	/// @return a new [TtlDB] instance
	public static TtlDB openTtl(Path path, Duration ttl) {
		try (Options opts = Options.newOptions().setCreateIfMissing(true)) {
			return openTtl(opts, path, ttl);
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
	public static BlobDB openBlob(Options options, Path path) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MemorySegment pathSeg = arena.allocateFrom(path.toString());
			MemorySegment ptr = (MemorySegment) MH_OPEN.invokeExact(options.ptr(), pathSeg, err);
			checkError(err);
			return new BlobDB(ptr);
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("openBlob failed", t);
		}
	}

	/// Equivalent to `openBlob(options, path)` with `createIfMissing = true`
	/// and `enableBlobFiles = true`.
	///
	/// @param path directory where the database files are stored
	/// @return a new [BlobDB] instance
	public static BlobDB openBlob(Path path) {
		try (Options opts = Options.newOptions().setCreateIfMissing(true).setEnableBlobFiles(true)) {
			return openBlob(opts, path);
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
					options.ptr(), pathSeg, toByte(errorIfWalFileExists), err);
			checkError(err);
			return new ReadOnlyDB(ptr);
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("openReadOnly failed", t);
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

			return new SecondaryDB(ptr);
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("openSecondary failed", t);
		}
	}

	/// Opens a secondary (read-only replica) instance at `primaryPath` with multiple column
	/// families. The `handles` list is cleared and populated with one [ColumnFamilyHandle]
	/// per descriptor, each legitimately scoped to this secondary instance — unlike a handle
	/// obtained from the primary or any other `rocksdb_t*`, safe to pass to this instance's
	/// column-family-scoped reads.
	///
	/// @param options      the database options
	/// @param primaryPath   directory of the primary database
	/// @param secondaryPath a dedicated directory for this secondary's own MANIFEST/WAL tails
	/// @param descriptors   one descriptor per column family (must include `"default"`)
	/// @param handles       output list populated with one handle per descriptor
	/// @return a new [SecondaryDB] instance
	public static SecondaryDB openSecondary(Options options, Path primaryPath, Path secondaryPath,
	                                        List<ColumnFamilyDescriptor> descriptors,
	                                        List<ColumnFamilyHandle> handles) {
		int n = descriptors.size();
		List<Options> tempOptions = new ArrayList<>();
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MemorySegment primary = arena.allocateFrom(primaryPath.toString());
			MemorySegment secondary = arena.allocateFrom(secondaryPath.toString());
			MemorySegment handlesArr = arena.allocate(ValueLayout.ADDRESS, n);
			CfNamesAndOptions cfArrays = buildCfArrays(arena, descriptors, tempOptions);

			MemorySegment ptr = (MemorySegment) MH_OPEN_SECONDARY_CF.invokeExact(
					options.ptr(), primary, secondary, n, cfArrays.names(), cfArrays.options(), handlesArr, err);
			checkError(err);

			collectCfHandles(handlesArr, n, handles);
			return new SecondaryDB(ptr);
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("openSecondary failed", t);
		} finally {
			closeTempOptions(tempOptions);
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
			return new TransactionDB(ptr, baseDb);
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("openTransaction failed", t);
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
			return new OptimisticTransactionDB(ptr, baseDb);
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("openOptimistic failed", t);
		}
	}

	// -----------------------------------------------------------------------
	// Package-private shared helpers — rocksdb_t* operations, mapped once
	// -----------------------------------------------------------------------

	/// Single-copy byte[] get: pins the value via `rocksdb_get_pinned_v2` and copies it out
	/// once. Not zero-copy — the returned array is a copy by definition — but cheaper than
	/// `rocksdb_get`, which per `c.h` returns "a malloc()ed array" the caller must free:
	/// that path copies the value into a fresh native buffer first, so producing a byte[]
	/// from it costs two copies plus a malloc/free round trip. Pinning skips the
	/// intermediate buffer entirely; `destroy` just drops the pin.
	///
	/// Uses the `_v2` handle rather than the older `rocksdb_get_pinned`, per `c.h`'s note
	/// on that family: "These functions avoid unnecessary memory allocations and copies.
	/// Bindings should migrate to these for better performance." [Transaction] and
	/// [TransactionDB] have no `_v2` equivalent in the C API and still go through
	/// [PinnableSlice].
	///
	/// Returns `null` if not found.
	static byte[] getBytes(RocksDBReadOperations db, ReadOptions readOpts, byte[] key) {
		// Single arena: it already needs one to marshal `key`, and withPinned would open
		// its own, paying for two Arena.ofConfined() per get instead of one.
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MemorySegment k = toNative(arena, key);
			MemorySegment handle = (MemorySegment) MH_GET_PINNED_V2.invokeExact(
					db.dbPtr(), readOpts.ptr(), k, (long) key.length, err);
			checkError(err);
			if (MemorySegment.NULL.equals(handle)) {
				return null;
			}
			try (PinnableHandle ph = PinnableHandle.wrap(handle)) {
				return ph.toByteArray(err);
			}
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("get failed", t);
		}
	}

	/// ByteBuffer get via `rocksdb_get_into_buffer` — copies directly into the caller's buffer,
	/// with no intermediate PinnableSlice. Copies nothing when the buffer is too small.
	static CopyResult getIntoBuffer(RocksDBReadOperations db, ReadOptions readOpts, MemorySegment key, ByteBuffer value) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MemorySegment valLenSeg = arena.allocate(ValueLayout.JAVA_LONG);
			MemorySegment foundSeg = arena.allocate(ValueLayout.JAVA_BYTE);
			byte fit = (byte) MH_GET_INTO_BUFFER.invokeExact(db.dbPtr(), readOpts.ptr(), key, key.byteSize(),
					MemorySegment.ofBuffer(value), (long) value.remaining(), valLenSeg, foundSeg, err);
			checkError(err);
			if (foundSeg.get(ValueLayout.JAVA_BYTE, 0) == 0) {
				return CopyResult.NotFound.INSTANCE;
			}
			long valLen = valLenSeg.get(ValueLayout.JAVA_LONG, 0);
			if (fit == 0) {
				return new CopyResult.NotEnoughCapacity(valLen);
			}
			value.position(value.position() + (int) valLen);
			return CopyResult.Copied.INSTANCE;
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("get failed", t);
		}
	}

	/// MemorySegment get via `rocksdb_get_into_buffer` — copies directly into the caller's
	/// segment, with no intermediate PinnableSlice. Copies nothing when `value` is too small.
	static CopyResult getIntoSegment(RocksDBReadOperations db, ReadOptions readOpts, MemorySegment key, MemorySegment value) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MemorySegment valLenSeg = arena.allocate(ValueLayout.JAVA_LONG);
			MemorySegment foundSeg = arena.allocate(ValueLayout.JAVA_BYTE);
			byte fit = (byte) MH_GET_INTO_BUFFER.invokeExact(db.dbPtr(), readOpts.ptr(), key, key.byteSize(),
					value, value.byteSize(), valLenSeg, foundSeg, err);
			checkError(err);
			if (foundSeg.get(ValueLayout.JAVA_BYTE, 0) == 0) {
				return CopyResult.NotFound.INSTANCE;
			}
			long valLen = valLenSeg.get(ValueLayout.JAVA_LONG, 0);
			if (fit == 0) {
				return new CopyResult.NotEnoughCapacity(valLen);
			}
			return CopyResult.Copied.INSTANCE;
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("get failed", t);
		}
	}

	// -----------------------------------------------------------------------
	// withPinned — scoped zero-copy get via rocksdb_pinnable_handle_t
	// -----------------------------------------------------------------------

	/// Scoped zero-copy get via `rocksdb_get_pinned_v2`. [PinnableHandle] owns the pinned
	/// value's lifetime and every way it gets consumed.
	static <R> Optional<R> withPinned(RocksDBReadOperations db, ReadOptions readOpts, MemorySegment key, Mapper<R> fn) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MemorySegment handle = (MemorySegment) MH_GET_PINNED_V2.invokeExact(db.dbPtr(), readOpts.ptr(), key, key.byteSize(), err);
			checkError(err);
			if (MemorySegment.NULL.equals(handle)) {
				return Optional.empty();
			}
			try (PinnableHandle ph = PinnableHandle.wrap(handle)) {
				return Optional.of(ph.map(arena, fn, err));
			}
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("get_pinned failed", t);
		}
	}

	/// Scoped zero-copy get from `cf` via `rocksdb_get_pinned_cf_v2`. See [#withPinned]
	/// for the lifetime contract.
	static <R> Optional<R> withPinnedCf(RocksDBReadOperations db, ReadOptions readOpts, ColumnFamilyHandle cf,
	                                     MemorySegment key, Mapper<R> fn) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MemorySegment handle = (MemorySegment) MH_GET_PINNED_CF_V2.invokeExact(
					db.dbPtr(), readOpts.ptr(), cf.ptr(), key, key.byteSize(), err);
			checkError(err);
			if (MemorySegment.NULL.equals(handle)) {
				return Optional.empty();
			}
			try (PinnableHandle ph = PinnableHandle.wrap(handle)) {
				return Optional.of(ph.map(arena, fn, err));
			}
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("get_pinned failed", t);
		}
	}

	/// byte[] put — slow path, allocates native memory.
	static void putBytes(RocksDBWriteOperations db, WriteOptions writeOpts, byte[] key, byte[] value) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MemorySegment k = toNative(arena, key);
			MemorySegment v = toNative(arena, value);
			MH_PUT.invokeExact(db.dbPtr(), writeOpts.ptr(), k, (long) key.length, v, (long) value.length, err);
			checkError(err);
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("put failed", t);
		}
	}

	/// byte[] put using the caller's arena.
	static void putBytes(Arena arena, RocksDBWriteOperations db, WriteOptions writeOpts, byte[] key, byte[] value) {
		try {
			MemorySegment err = errHolder(arena);
			MemorySegment k = toNative(arena, key);
			MemorySegment v = toNative(arena, value);
			MH_PUT.invokeExact(db.dbPtr(), writeOpts.ptr(), k, (long) key.length, v, (long) value.length, err);
			checkError(err);
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("put failed", t);
		}
	}

	/// MemorySegment put — zero-copy, caller supplies pre-allocated native segments.
	static void putSegment(RocksDBWriteOperations db, WriteOptions writeOpts,
	                       MemorySegment key, MemorySegment val) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MH_PUT.invokeExact(db.dbPtr(), writeOpts.ptr(), key, key.byteSize(), val, val.byteSize(), err);
			checkError(err);
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("put failed", t);
		}
	}

	/// MemorySegment put using the caller's arena.
	static void putSegment(Arena arena, RocksDBWriteOperations db, WriteOptions writeOpts,
	                       MemorySegment key, MemorySegment val) {
		try {
			MemorySegment err = errHolder(arena);
			MH_PUT.invokeExact(db.dbPtr(), writeOpts.ptr(), key, key.byteSize(), val, val.byteSize(), err);
			checkError(err);
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("put failed", t);
		}
	}

	/// byte[] merge — slow path, allocates native memory.
	static void mergeBytes(RocksDBWriteOperations db, WriteOptions writeOpts, byte[] key, byte[] value) {
		try (Arena arena = Arena.ofConfined()) {
			mergeBytes(arena, db, writeOpts, key, value);
		}
	}

	/// byte[] merge using the caller's arena.
	static void mergeBytes(Arena arena, RocksDBWriteOperations db, WriteOptions writeOpts, byte[] key, byte[] value) {
		MemorySegment k = toNative(arena, key);
		MemorySegment v = toNative(arena, value);
		mergeSegment(arena, db, writeOpts, k, v);
	}

	/// MemorySegment merge — zero-copy, caller supplies pre-allocated native segments.
	static void mergeSegment(RocksDBWriteOperations db, WriteOptions writeOpts,
	                         MemorySegment key, MemorySegment val) {
		try (Arena arena = Arena.ofConfined()) {
			mergeSegment(arena, db, writeOpts, key, val);
		}
	}

	/// MemorySegment merge using the caller's arena.
	static void mergeSegment(Arena arena, RocksDBWriteOperations db, WriteOptions writeOpts,
	                         MemorySegment key, MemorySegment val) {
		try {
			MemorySegment err = errHolder(arena);
			MH_MERGE.invokeExact(db.dbPtr(), writeOpts.ptr(), key, key.byteSize(), val, val.byteSize(), err);
			checkError(err);
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("merge failed", t);
		}
	}

	/// byte[] delete — slow path.
	static void deleteBytes(RocksDBWriteOperations db, WriteOptions writeOpts, byte[] key) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MemorySegment k = toNative(arena, key);
			MH_DELETE.invokeExact(db.dbPtr(), writeOpts.ptr(), k, (long) key.length, err);
			checkError(err);
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("delete failed", t);
		}
	}

	/// MemorySegment delete — zero-copy.
	static void deleteSegment(RocksDBWriteOperations db, WriteOptions writeOpts, MemorySegment key) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MH_DELETE.invokeExact(db.dbPtr(), writeOpts.ptr(), key, key.byteSize(), err);
			checkError(err);
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("delete failed", t);
		}
	}

	static void flush(RocksDBWriteOperations db, FlushOptions flushOptions) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MH_FLUSH.invokeExact(db.dbPtr(), flushOptions.ptr(), err);
			checkError(err);
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("flush failed", t);
		}
	}

	static void cancelAllBackgroundWork(RocksDBWriteOperations db, boolean wait) {
		try {
			MH_CANCEL_ALL_BACKGROUND_WORK.invokeExact(db.dbPtr(), toByte(wait));
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("cancelAllBackgroundWork failed", t);
		}
	}

	static void disableManualCompaction(RocksDBWriteOperations db) {
		try {
			MH_DISABLE_MANUAL_COMPACTION.invokeExact(db.dbPtr());
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("disableManualCompaction failed", t);
		}
	}

	static void enableManualCompaction(RocksDBWriteOperations db) {
		try {
			MH_ENABLE_MANUAL_COMPACTION.invokeExact(db.dbPtr());
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("enableManualCompaction failed", t);
		}
	}

	static void waitForCompact(RocksDBWriteOperations db, WaitForCompactOptions options) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MH_WAIT_FOR_COMPACT.invokeExact(db.dbPtr(), options.ptr(), err);
			checkError(err);
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("waitForCompact failed", t);
		}
	}

	static SequenceNumber getLatestSequenceNumber(RocksDBWriteOperations db) {
		try {
			long seq = (long) MH_GET_LATEST_SEQUENCE_NUMBER.invokeExact(db.dbPtr());
			return SequenceNumber.of(seq);
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("getLatestSequenceNumber failed", t);
		}
	}

	static WalIterator getUpdatesSince(RocksDBWriteOperations db, SequenceNumber sequenceNumber) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MemorySegment iterPtr = (MemorySegment) MH_GET_UPDATES_SINCE.invokeExact(
					db.dbPtr(), sequenceNumber.toLong(), MemorySegment.NULL, err);
			checkError(err);
			return WalIterator.wrap(iterPtr);
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("getUpdatesSince failed", t);
		}
	}

	static void flushWal(RocksDBWriteOperations db, boolean sync) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MH_FLUSH_WAL.invokeExact(db.dbPtr(), toByte(sync), err);
			checkError(err);
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("flushWal failed", t);
		}
	}

	static Snapshot createSnapshot(NativeObjectWithChildren owningDb, MemorySegment db) {
		try {
			MemorySegment snapPtr = (MemorySegment) MH_CREATE_SNAPSHOT.invokeExact(db);
			return new Snapshot(owningDb, db, snapPtr);
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("getSnapshot failed", t);
		}
	}

	static Optional<String> getProperty(RocksDBReadOperations db, Property property) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment propSeg = arena.allocateFrom(property.propertyName());
			MemorySegment result = (MemorySegment) MH_PROPERTY_VALUE.invokeExact(db.dbPtr(), propSeg);
			return toOptionalString(result);
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("getProperty failed", t);
		}
	}

	static OptionalLong getLongProperty(RocksDBReadOperations db, Property property) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment propSeg = arena.allocateFrom(property.propertyName());
			MemorySegment out = arena.allocate(ValueLayout.JAVA_LONG);
			int rc = (int) MH_PROPERTY_INT.invokeExact(db.dbPtr(), propSeg, out);
			if (rc != 0) {
				return OptionalLong.empty();
			}
			return OptionalLong.of(out.get(ValueLayout.JAVA_LONG, 0));
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("getLongProperty failed", t);
		}
	}

	static void closeDb(MemorySegment db) throws Throwable {
		MH_CLOSE.invokeExact(db);
	}

	static void deleteRangeCfBytes(RocksDBWriteOperations db, WriteOptions writeOpts, byte[] startKey, byte[] endKey) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MemorySegment cf = (MemorySegment) MH_GET_DEFAULT_CF.invokeExact(db.dbPtr());
			MH_DELETE_RANGE_CF.invokeExact(db.dbPtr(), writeOpts.ptr(), cf,
					toNative(arena, startKey), (long) startKey.length,
					toNative(arena, endKey), (long) endKey.length, err);
			checkError(err);
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("deleteRange failed", t);
		}
	}

	static void deleteRangeCfBuffer(RocksDBWriteOperations db, WriteOptions writeOpts,
	                                ByteBuffer startKey, ByteBuffer endKey) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MemorySegment cf = (MemorySegment) MH_GET_DEFAULT_CF.invokeExact(db.dbPtr());
			MH_DELETE_RANGE_CF.invokeExact(db.dbPtr(), writeOpts.ptr(), cf,
					MemorySegment.ofBuffer(startKey), (long) startKey.remaining(),
					MemorySegment.ofBuffer(endKey), (long) endKey.remaining(), err);
			checkError(err);
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("deleteRange failed", t);
		}
	}

	static void deleteRangeCfSegment(RocksDBWriteOperations db, WriteOptions writeOpts,
	                                 MemorySegment startKey, MemorySegment endKey) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MemorySegment cf = (MemorySegment) MH_GET_DEFAULT_CF.invokeExact(db.dbPtr());
			MH_DELETE_RANGE_CF.invokeExact(db.dbPtr(), writeOpts.ptr(), cf,
					startKey, startKey.byteSize(), endKey, endKey.byteSize(), err);
			checkError(err);
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("deleteRange failed", t);
		}
	}

	static void writeBatch(RocksDBWriteOperations db, WriteOptions writeOpts, WriteBatch batch) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MH_WRITE.invokeExact(db.dbPtr(), writeOpts.ptr(), batch.ptr(), err);
			checkError(err);
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("write failed", t);
		}
	}

	static void writeBatch(Arena arena, RocksDBWriteOperations db, WriteOptions writeOpts, WriteBatch batch) {
		try {
			MemorySegment err = errHolder(arena);
			MH_WRITE.invokeExact(db.dbPtr(), writeOpts.ptr(), batch.ptr(), err);
			checkError(err);
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("write failed", t);
		}
	}

	static boolean keyMayExistSegment(RocksDBReadOperations db, ReadOptions roOpts, MemorySegment key) {
		try {
			return fromByte((byte) MH_KEY_MAY_EXIST.invokeExact(db.dbPtr(), roOpts.ptr(), key, key.byteSize(),
					MemorySegment.NULL, MemorySegment.NULL,
					MemorySegment.NULL, 0L, MemorySegment.NULL));
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("keyMayExist failed", t);
		}
	}

	/// [#keyMayExistSegment] for a `byte[]` key: marshals `key` into a scratch [Arena]
	/// before delegating.
	static boolean keyMayExistBytes(RocksDBReadOperations db, ReadOptions roOpts, byte[] key) {
		try (Arena arena = Arena.ofConfined()) {
			return keyMayExistSegment(db, roOpts, toNative(arena, key));
		}
	}

	static void compactRangeBytes(RocksDBWriteOperations db, byte[] startKey, byte[] endKey) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment s = startKey == null ? MemorySegment.NULL : toNative(arena, startKey);
			MemorySegment e = endKey == null ? MemorySegment.NULL : toNative(arena, endKey);
			MH_COMPACT_RANGE.invokeExact(db.dbPtr(),
					s, startKey == null ? 0L : (long) startKey.length,
					e, endKey == null ? 0L : (long) endKey.length);
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("compactRange failed", t);
		}
	}

	static void compactRangeBuffer(RocksDBWriteOperations db, ByteBuffer startKey, ByteBuffer endKey) {
		try {
			MemorySegment s = startKey == null ? MemorySegment.NULL : MemorySegment.ofBuffer(startKey);
			MemorySegment e = endKey == null ? MemorySegment.NULL : MemorySegment.ofBuffer(endKey);
			MH_COMPACT_RANGE.invokeExact(db.dbPtr(),
					s, startKey == null ? 0L : (long) startKey.remaining(),
					e, endKey == null ? 0L : (long) endKey.remaining());
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("compactRange failed", t);
		}
	}

	static void compactRangeSegment(RocksDBWriteOperations db, MemorySegment startKey, MemorySegment endKey) {
		try {
			MemorySegment s = startKey == null ? MemorySegment.NULL : startKey;
			MemorySegment e = endKey == null ? MemorySegment.NULL : endKey;
			MH_COMPACT_RANGE.invokeExact(db.dbPtr(),
					s, s == MemorySegment.NULL ? 0L : s.byteSize(),
					e, e == MemorySegment.NULL ? 0L : e.byteSize());
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("compactRange failed", t);
		}
	}

	static void compactRangeOptBytes(RocksDBWriteOperations db, CompactOptions opts, byte[] startKey, byte[] endKey) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment s = startKey == null ? MemorySegment.NULL : toNative(arena, startKey);
			MemorySegment e = endKey == null ? MemorySegment.NULL : toNative(arena, endKey);
			MH_COMPACT_RANGE_OPT.invokeExact(db.dbPtr(), opts.ptr(),
					s, startKey == null ? 0L : (long) startKey.length,
					e, endKey == null ? 0L : (long) endKey.length);
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("compactRange failed", t);
		}
	}

	static void suggestCompactRangeBytes(RocksDBWriteOperations db, byte[] startKey, byte[] endKey) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MemorySegment s = startKey == null ? MemorySegment.NULL : toNative(arena, startKey);
			MemorySegment e = endKey == null ? MemorySegment.NULL : toNative(arena, endKey);
			MH_SUGGEST_COMPACT_RANGE.invokeExact(db.dbPtr(),
					s, startKey == null ? 0L : (long) startKey.length,
					e, endKey == null ? 0L : (long) endKey.length, err);
			checkError(err);
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("suggestCompactRange failed", t);
		}
	}

	static void disableFileDeletions(RocksDBWriteOperations db) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MH_DISABLE_FILE_DELETIONS.invokeExact(db.dbPtr(), err);
			checkError(err);
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("disableFileDeletions failed", t);
		}
	}

	static void enableFileDeletions(RocksDBWriteOperations db) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MH_ENABLE_FILE_DELETIONS.invokeExact(db.dbPtr(), err);
			checkError(err);
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("enableFileDeletions failed", t);
		}
	}

	static void ingestExternalFile(RocksDBWriteOperations db, List<Path> files, IngestExternalFileOptions options) {
		if (files.isEmpty()) {
			return;
		}
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MemorySegment fileArray = arena.allocate(ValueLayout.ADDRESS, files.size());
			for (int i = 0; i < files.size(); i++) {
				fileArray.setAtIndex(ValueLayout.ADDRESS, i, arena.allocateFrom(files.get(i).toString()));
			}
			MH_INGEST_EXTERNAL_FILE.invokeExact(db.dbPtr(), fileArray, (long) files.size(), options.ptr(), err);
			checkError(err);
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("ingestExternalFile failed", t);
		}
	}

	static void ingestExternalFileWithDefaults(RocksDBWriteOperations db, List<Path> files) {
		try (IngestExternalFileOptions opts = IngestExternalFileOptions.newIngestExternalFileOptions()) {
			ingestExternalFile(db, files, opts);
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
	public static ReadWriteDB openReadWrite(Options options, Path path,
	                                        List<ColumnFamilyDescriptor> descriptors,
	                                        List<ColumnFamilyHandle> handles) {
		int n = descriptors.size();
		List<Options> tempOptions = new ArrayList<>();
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MemorySegment pathSeg = arena.allocateFrom(path.toString());
			MemorySegment handlesArr = arena.allocate(ValueLayout.ADDRESS, n);
			CfNamesAndOptions cfArrays = buildCfArrays(arena, descriptors, tempOptions);

			MemorySegment ptr = (MemorySegment) MH_OPEN_CF.invokeExact(
					options.ptr(), pathSeg, n, cfArrays.names(), cfArrays.options(), handlesArr, err);
			checkError(err);

			collectCfHandles(handlesArr, n, handles);
			return new ReadWriteDB(ptr);
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("openReadWrite failed", t);
		} finally {
			closeTempOptions(tempOptions);
		}
	}

	/// Opens a blob-enabled read-write database at `path` with multiple column families.
	///
	/// The `handles` list is cleared and populated with one [ColumnFamilyHandle] per descriptor.
	/// Blob file options (min blob size, blob compression, ...) are set per column family via
	/// each descriptor's [Options], same as for [#openReadWrite(Options, Path, List, List)].
	///
	/// @param options the database options
	/// @param path directory where the database files are stored
	/// @param descriptors one descriptor per column family (must include `"default"`)
	/// @param handles output list populated with one handle per descriptor
	/// @return a new [BlobDB] instance
	public static BlobDB openBlob(Options options, Path path,
	                              List<ColumnFamilyDescriptor> descriptors,
	                              List<ColumnFamilyHandle> handles) {
		int n = descriptors.size();
		List<Options> tempOptions = new ArrayList<>();
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MemorySegment pathSeg = arena.allocateFrom(path.toString());
			MemorySegment handlesArr = arena.allocate(ValueLayout.ADDRESS, n);
			CfNamesAndOptions cfArrays = buildCfArrays(arena, descriptors, tempOptions);

			MemorySegment ptr = (MemorySegment) MH_OPEN_CF.invokeExact(
					options.ptr(), pathSeg, n, cfArrays.names(), cfArrays.options(), handlesArr, err);
			checkError(err);

			collectCfHandles(handlesArr, n, handles);
			return new BlobDB(ptr);
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("openBlob failed", t);
		} finally {
			closeTempOptions(tempOptions);
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
	public static ReadOnlyDB openReadOnly(Options options, Path path,
	                                      List<ColumnFamilyDescriptor> descriptors,
	                                      List<ColumnFamilyHandle> handles) {
		return openReadOnly(options, path, descriptors, handles, false);
	}

	/// Opens a read-only database at `path` with multiple column families.
	///
	/// @param options the database options
	/// @param path directory where the database files are stored
	/// @param descriptors one descriptor per column family (must include `"default"`)
	/// @param handles output list populated with one handle per descriptor
	/// @param errorIfWalFileExists if `true`, fails when unrecovered WAL files are present
	/// @return a new [ReadOnlyDB] instance
	public static ReadOnlyDB openReadOnly(Options options, Path path,
	                                      List<ColumnFamilyDescriptor> descriptors,
	                                      List<ColumnFamilyHandle> handles,
	                                      boolean errorIfWalFileExists) {
		int n = descriptors.size();
		List<Options> tempOptions = new ArrayList<>();
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MemorySegment pathSeg = arena.allocateFrom(path.toString());
			MemorySegment handlesArr = arena.allocate(ValueLayout.ADDRESS, n);
			CfNamesAndOptions cfArrays = buildCfArrays(arena, descriptors, tempOptions);
			MemorySegment ptr = (MemorySegment) MH_OPEN_FOR_READ_ONLY_CF.invokeExact(
					options.ptr(), pathSeg, n, cfArrays.names(), cfArrays.options(), handlesArr,
					toByte(errorIfWalFileExists), err);
			checkError(err);
			collectCfHandles(handlesArr, n, handles);
			return new ReadOnlyDB(ptr);
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("openReadOnly failed", t);
		} finally {
			closeTempOptions(tempOptions);
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
	public static TtlDB openTtl(Options options, Path path,
	                            List<ColumnFamilyDescriptor> descriptors,
	                            List<Duration> ttls,
	                            List<ColumnFamilyHandle> handles) {
		int n = descriptors.size();
		List<Options> tempOptions = new ArrayList<>();
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MemorySegment pathSeg = arena.allocateFrom(path.toString());
			MemorySegment handlesArr = arena.allocate(ValueLayout.ADDRESS, n);
			MemorySegment ttlsArr = arena.allocate(ValueLayout.JAVA_INT, n);
			CfNamesAndOptions cfArrays = buildCfArrays(arena, descriptors, tempOptions);
			for (int i = 0; i < n; i++) {
				ttlsArr.setAtIndex(ValueLayout.JAVA_INT, i, (int) ttls.get(i).toSeconds());
			}
			MemorySegment ptr = (MemorySegment) MH_OPEN_CF_WITH_TTL.invokeExact(
					options.ptr(), pathSeg, n, cfArrays.names(), cfArrays.options(), handlesArr, ttlsArr, err);
			checkError(err);
			collectCfHandles(handlesArr, n, handles);
			Duration globalTtl = ttls.isEmpty() ? Duration.ZERO : ttls.getFirst();
			return new TtlDB(ptr, globalTtl);
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("openTtl failed", t);
		} finally {
			closeTempOptions(tempOptions);
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
	public static TransactionDB openTransaction(Options options,
	                                            TransactionDBOptions txnDbOptions,
	                                            Path path,
	                                            List<ColumnFamilyDescriptor> descriptors,
	                                            List<ColumnFamilyHandle> handles) {
		int n = descriptors.size();
		List<Options> tempOptions = new ArrayList<>();
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MemorySegment pathSeg = arena.allocateFrom(path.toString());
			MemorySegment handlesArr = arena.allocate(ValueLayout.ADDRESS, n);
			CfNamesAndOptions cfArrays = buildCfArrays(arena, descriptors, tempOptions);
			MemorySegment ptr = (MemorySegment) MH_OPEN_TRANSACTION_CF.invokeExact(
					options.ptr(), txnDbOptions.ptr(), pathSeg, n, cfArrays.names(), cfArrays.options(), handlesArr, err);
			checkError(err);
			collectCfHandles(handlesArr, n, handles);
			MemorySegment baseDb = (MemorySegment) MH_TRANSACTION_GET_BASE_DB.invokeExact(ptr);
			return new TransactionDB(ptr, baseDb);
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("openTransaction failed", t);
		} finally {
			closeTempOptions(tempOptions);
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
	public static OptimisticTransactionDB openOptimistic(Options options, Path path,
	                                                     List<ColumnFamilyDescriptor> descriptors,
	                                                     List<ColumnFamilyHandle> handles) {
		int n = descriptors.size();
		List<Options> tempOptions = new ArrayList<>();
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MemorySegment pathSeg = arena.allocateFrom(path.toString());
			MemorySegment handlesArr = arena.allocate(ValueLayout.ADDRESS, n);
			CfNamesAndOptions cfArrays = buildCfArrays(arena, descriptors, tempOptions);
			MemorySegment ptr = (MemorySegment) MH_OPEN_OPTIMISTIC_CF.invokeExact(
					options.ptr(), pathSeg, n, cfArrays.names(), cfArrays.options(), handlesArr, err);
			checkError(err);
			collectCfHandles(handlesArr, n, handles);
			MemorySegment baseDb = (MemorySegment) MH_GET_BASE_DB.invokeExact(ptr);
			return new OptimisticTransactionDB(ptr, baseDb);
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("openOptimistic failed", t);
		} finally {
			closeTempOptions(tempOptions);
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
			throw RocksDB.wrapInvokeFailure("listColumnFamilies failed", t);
		}
	}

	// -----------------------------------------------------------------------
	// Package-private CF helpers
	// -----------------------------------------------------------------------

	/// The parallel `char* names[]` / `rocksdb_options_t* options[]` arrays a
	/// `*_column_families` C API call expects, one entry per descriptor.
	private record CfNamesAndOptions(MemorySegment names, MemorySegment options) {
	}

	/// Allocates and fills the parallel names/options arrays every `*_column_families` open
	/// call marshals, one entry per `descriptors[i]`. A descriptor with no explicit
	/// [ColumnFamilyDescriptor#options()] gets a fresh, disposable [Options] instance,
	/// appended to `tempOptions` so the caller can close it once the native call returns.
	///
	/// @param arena       arena backing the returned native arrays (and any allocated names)
	/// @param descriptors one descriptor per column family
	/// @param tempOptions appended with any default [Options] created here; caller must close them
	/// @return the names and options arrays, both length `descriptors.size()`
	private static CfNamesAndOptions buildCfArrays(Arena arena, List<ColumnFamilyDescriptor> descriptors,
	                                               List<Options> tempOptions) {
		int n = descriptors.size();
		MemorySegment namesArr = arena.allocate(ValueLayout.ADDRESS, n);
		MemorySegment optsArr = arena.allocate(ValueLayout.ADDRESS, n);
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
		return new CfNamesAndOptions(namesArr, optsArr);
	}

	/// Reads a native `rocksdb_column_family_handle_t*[]` array populated by a
	/// `*_column_families` open call back into `handles`, wrapping each entry.
	///
	/// @param handlesArr native array of `n` column family handle pointers
	/// @param n          number of handles
	/// @param handles    output list; cleared then populated with one handle per entry
	private static void collectCfHandles(MemorySegment handlesArr, int n, List<ColumnFamilyHandle> handles) {
		handles.clear();
		for (int i = 0; i < n; i++) {
			handles.add(ColumnFamilyHandle.wrap(handlesArr.getAtIndex(ValueLayout.ADDRESS, i)));
		}
	}

	/// Closes every [Options] a [#buildCfArrays] call appended to `tempOptions`.
	///
	/// @param tempOptions options to close, as populated by [#buildCfArrays]
	private static void closeTempOptions(List<Options> tempOptions) {
		for (Options o : tempOptions) {
			o.close();
		}
	}

	static ColumnFamilyHandle createCf(RocksDBWriteOperations db, ColumnFamilyDescriptor descriptor) {
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
					db.dbPtr(), cfOpts.ptr(), nameSeg, err);
			checkError(err);
			return ColumnFamilyHandle.wrap(handle);
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("createColumnFamily failed", t);
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
			throw RocksDB.wrapInvokeFailure("dropColumnFamily failed", t);
		}
	}

	/// byte[] put with explicit column family — slow path.
	static void putCfBytes(RocksDBWriteOperations db, WriteOptions writeOpts, ColumnFamilyHandle cf,
	                       byte[] key, byte[] value) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MH_PUT_CF.invokeExact(db.dbPtr(), writeOpts.ptr(), cf.ptr(),
					toNative(arena, key), (long) key.length,
					toNative(arena, value), (long) value.length, err);
			checkError(err);
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("put failed", t);
		}
	}

	/// MemorySegment put with explicit column family — zero-copy.
	static void putCfSegment(RocksDBWriteOperations db, WriteOptions writeOpts, ColumnFamilyHandle cf,
	                         MemorySegment key, MemorySegment val) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MH_PUT_CF.invokeExact(db.dbPtr(), writeOpts.ptr(), cf.ptr(), key, key.byteSize(), val, val.byteSize(), err);
			checkError(err);
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("put failed", t);
		}
	}

	/// byte[] merge with explicit column family — slow path.
	static void mergeCfBytes(RocksDBWriteOperations db, WriteOptions writeOpts, ColumnFamilyHandle cf,
	                         byte[] key, byte[] value) {
		try (Arena arena = Arena.ofConfined()) {
			mergeCfBytes(arena, db, writeOpts, cf, key, value);
		}
	}

	/// byte[] merge with explicit column family using the caller's arena.
	static void mergeCfBytes(Arena arena, RocksDBWriteOperations db, WriteOptions writeOpts, ColumnFamilyHandle cf,
	                         byte[] key, byte[] value) {
		MemorySegment k = toNative(arena, key);
		MemorySegment v = toNative(arena, value);
		mergeCfSegment(arena, db, writeOpts, cf, k, v);
	}

	/// MemorySegment merge with explicit column family — zero-copy.
	static void mergeCfSegment(RocksDBWriteOperations db, WriteOptions writeOpts, ColumnFamilyHandle cf,
	                           MemorySegment key, MemorySegment val) {
		try (Arena arena = Arena.ofConfined()) {
			mergeCfSegment(arena, db, writeOpts, cf, key, val);
		}
	}

	/// MemorySegment merge with explicit column family using the caller's arena.
	static void mergeCfSegment(Arena arena, RocksDBWriteOperations db, WriteOptions writeOpts, ColumnFamilyHandle cf,
	                           MemorySegment key, MemorySegment val) {
		try {
			MemorySegment err = errHolder(arena);
			MH_MERGE_CF.invokeExact(db.dbPtr(), writeOpts.ptr(), cf.ptr(), key, key.byteSize(), val, val.byteSize(), err);
			checkError(err);
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("merge failed", t);
		}
	}

	/// Single-copy byte[] get from `cf` via `rocksdb_get_pinned_cf_v2`. See [#getBytes] for
	/// why this pins rather than calling `rocksdb_get`, and why it uses the `_v2` handle.
	/// Returns `null` if not found.
	static byte[] getCfBytes(RocksDBReadOperations db, ReadOptions readOpts, ColumnFamilyHandle cf,
	                         byte[] key) {
		// Single arena, same reasoning as getBytes above.
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MemorySegment k = toNative(arena, key);
			MemorySegment handle = (MemorySegment) MH_GET_PINNED_CF_V2.invokeExact(
					db.dbPtr(), readOpts.ptr(), cf.ptr(), k, (long) key.length, err);
			checkError(err);
			if (MemorySegment.NULL.equals(handle)) {
				return null;
			}
			try (PinnableHandle ph = PinnableHandle.wrap(handle)) {
				return ph.toByteArray(err);
			}
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("get failed", t);
		}
	}

	/// ByteBuffer get with explicit column family via `rocksdb_get_into_buffer_cf`.
	/// Copies nothing when the buffer is too small.
	static CopyResult getCfIntoBuffer(RocksDBReadOperations db, ReadOptions readOpts, ColumnFamilyHandle cf,
	                                  MemorySegment key, ByteBuffer value) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MemorySegment valLenSeg = arena.allocate(ValueLayout.JAVA_LONG);
			MemorySegment foundSeg = arena.allocate(ValueLayout.JAVA_BYTE);
			byte fit = (byte) MH_GET_INTO_BUFFER_CF.invokeExact(db.dbPtr(), readOpts.ptr(), cf.ptr(), key, key.byteSize(),
					MemorySegment.ofBuffer(value), (long) value.remaining(), valLenSeg, foundSeg, err);
			checkError(err);
			if (foundSeg.get(ValueLayout.JAVA_BYTE, 0) == 0) {
				return CopyResult.NotFound.INSTANCE;
			}
			long valLen = valLenSeg.get(ValueLayout.JAVA_LONG, 0);
			if (fit == 0) {
				return new CopyResult.NotEnoughCapacity(valLen);
			}
			value.position(value.position() + (int) valLen);
			return CopyResult.Copied.INSTANCE;
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("get failed", t);
		}
	}

	/// MemorySegment get with explicit column family via `rocksdb_get_into_buffer_cf` —
	/// copies directly into the caller's segment. Copies nothing when `value` is too small.
	static CopyResult getCfIntoSegment(RocksDBReadOperations db, ReadOptions readOpts, ColumnFamilyHandle cf,
	                                   MemorySegment key, MemorySegment value) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MemorySegment valLenSeg = arena.allocate(ValueLayout.JAVA_LONG);
			MemorySegment foundSeg = arena.allocate(ValueLayout.JAVA_BYTE);
			byte fit = (byte) MH_GET_INTO_BUFFER_CF.invokeExact(db.dbPtr(), readOpts.ptr(), cf.ptr(), key, key.byteSize(),
					value, value.byteSize(), valLenSeg, foundSeg, err);
			checkError(err);
			if (foundSeg.get(ValueLayout.JAVA_BYTE, 0) == 0) {
				return CopyResult.NotFound.INSTANCE;
			}
			long valLen = valLenSeg.get(ValueLayout.JAVA_LONG, 0);
			if (fit == 0) {
				return new CopyResult.NotEnoughCapacity(valLen);
			}
			return CopyResult.Copied.INSTANCE;
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("get failed", t);
		}
	}

	/// byte[] delete with explicit column family — slow path.
	static void deleteCfBytes(RocksDBWriteOperations db, WriteOptions writeOpts, ColumnFamilyHandle cf,
	                          byte[] key) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MH_DELETE_CF.invokeExact(db.dbPtr(), writeOpts.ptr(), cf.ptr(),
					toNative(arena, key), (long) key.length, err);
			checkError(err);
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("delete failed", t);
		}
	}

	/// MemorySegment delete with explicit column family — zero-copy.
	static void deleteCfSegment(RocksDBWriteOperations db, WriteOptions writeOpts, ColumnFamilyHandle cf,
	                            MemorySegment key) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MH_DELETE_CF.invokeExact(db.dbPtr(), writeOpts.ptr(), cf.ptr(), key, key.byteSize(), err);
			checkError(err);
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("delete failed", t);
		}
	}

	/// deleteRange with explicit column family — slow path.
	static void deleteRangeCfBytesExplicit(RocksDBWriteOperations db, WriteOptions writeOpts,
	                                       ColumnFamilyHandle cf,
	                                       byte[] startKey, byte[] endKey) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MH_DELETE_RANGE_CF.invokeExact(db.dbPtr(), writeOpts.ptr(), cf.ptr(),
					toNative(arena, startKey), (long) startKey.length,
					toNative(arena, endKey), (long) endKey.length, err);
			checkError(err);
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("deleteRange failed", t);
		}
	}

	/// deleteRange with explicit column family — zero-copy for direct ByteBuffers.
	static void deleteRangeCfBufferExplicit(RocksDBWriteOperations db, WriteOptions writeOpts,
	                                        ColumnFamilyHandle cf,
	                                        ByteBuffer startKey, ByteBuffer endKey) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MH_DELETE_RANGE_CF.invokeExact(db.dbPtr(), writeOpts.ptr(), cf.ptr(),
					MemorySegment.ofBuffer(startKey), (long) startKey.remaining(),
					MemorySegment.ofBuffer(endKey), (long) endKey.remaining(), err);
			checkError(err);
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("deleteRange failed", t);
		}
	}

	/// deleteRange with explicit column family — zero-copy for MemorySegments.
	static void deleteRangeCfSegmentExplicit(RocksDBWriteOperations db, WriteOptions writeOpts,
	                                         ColumnFamilyHandle cf,
	                                         MemorySegment startKey, MemorySegment endKey) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MH_DELETE_RANGE_CF.invokeExact(db.dbPtr(), writeOpts.ptr(), cf.ptr(),
					startKey, startKey.byteSize(), endKey, endKey.byteSize(), err);
			checkError(err);
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("deleteRange failed", t);
		}
	}

	static boolean keyMayExistCfSegment(RocksDBReadOperations db, ReadOptions roOpts,
	                                    ColumnFamilyHandle cf, MemorySegment key) {
		try {
			return fromByte((byte) MH_KEY_MAY_EXIST_CF.invokeExact(db.dbPtr(), roOpts.ptr(), cf.ptr(), key, key.byteSize(),
					MemorySegment.NULL, MemorySegment.NULL,
					MemorySegment.NULL, 0L, MemorySegment.NULL));
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("keyMayExist failed", t);
		}
	}

	/// [#keyMayExistCfSegment] for a `byte[]` key: marshals `key` into a scratch [Arena]
	/// before delegating.
	static boolean keyMayExistCfBytes(RocksDBReadOperations db, ReadOptions roOpts,
	                                  ColumnFamilyHandle cf, byte[] key) {
		try (Arena arena = Arena.ofConfined()) {
			return keyMayExistCfSegment(db, roOpts, cf, toNative(arena, key));
		}
	}

	static RocksIterator createIteratorCf(RocksDBReadOperations db, ReadOptions readOpts,
	                                      ColumnFamilyHandle cf) {
		try {
			MemorySegment iterPtr = (MemorySegment) MH_CREATE_ITERATOR_CF.invokeExact(
					db.dbPtr(), readOpts.ptr(), cf.ptr());
			return RocksIterator.create(iterPtr);
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("newIterator failed", t);
		}
	}

	static void flushCf(RocksDBWriteOperations db, FlushOptions flushOptions, ColumnFamilyHandle cf) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment err = errHolder(arena);
			MH_FLUSH_CF.invokeExact(db.dbPtr(), flushOptions.ptr(), cf.ptr(), err);
			checkError(err);
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("flush failed", t);
		}
	}

	static Optional<String> getPropertyCf(MemorySegment db, ColumnFamilyHandle cf,
	                                       Property property) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment propSeg = arena.allocateFrom(property.propertyName());
			MemorySegment result = (MemorySegment) MH_PROPERTY_VALUE_CF.invokeExact(
					db, cf.ptr(), propSeg);
			return toOptionalString(result);
		} catch (Throwable t) {
			throw RocksDB.wrapInvokeFailure("getProperty failed", t);
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
			throw RocksDB.wrapInvokeFailure("getLongProperty failed", t);
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
			String msg = toJavaString(errPtr);
			throw new RocksDBException(msg);
		}
	}

	/// Classifies a `Throwable` caught from a `catch (Throwable t)` block wrapping an
	/// `invokeExact` call on a downcall [MethodHandle]. RocksDB itself reports operational
	/// failures via `errptr`, checked separately by [#checkError(MemorySegment)] -- typically
	/// called right after `invokeExact` inside the same `try`, so a genuine [RocksDBException]
	/// it throws reaches this method too, alongside whatever `invokeExact` itself might throw.
	/// See [ADR 0004](https://github.com/dfa1/rocksdbffm/blob/main/docs/adr/0004-error-handling.md).
	///
	/// Every `RuntimeException` -- a [RocksDBException] from `checkError`, or one of the small,
	/// fixed set `invokeExact` itself throws in practice for a downcall handle
	/// (`NullPointerException`, `IllegalStateException` including `WrongThreadException`,
	/// `WrongMethodTypeException`, `ClassCastException`, each indicating a concrete binding bug:
	/// wrong argument, closed/wrong-thread arena, mismatched `FunctionDescriptor`, bad cast) --
	/// propagates unwrapped, with its original type preserved. An [IOException] (not from
	/// `invokeExact` itself, which never throws a checked exception, but possible from other
	/// code sharing the same `try` block, e.g. file access) becomes an [UncheckedIOException],
	/// the standard idiom for surfacing it as unchecked. Anything else reaching this method
	/// should never actually happen for a correctly configured downcall handle, and becomes an
	/// [AssertionError].
	///
	/// @param message description used for the [UncheckedIOException]/[AssertionError] fallbacks
	/// @param t       the throwable caught from the `invokeExact` call's `try` block
	/// @return never returns; declared non-void so callers can write `throw wrapInvokeFailure(...)`
	static RuntimeException wrapInvokeFailure(String message, Throwable t) {
		if (t instanceof RuntimeException e) {
			throw e;
		}
		if (t instanceof IOException e) {
			throw new UncheckedIOException(message, e);
		}
		throw new AssertionError(message, t);
	}

	/// Copies `len` bytes out of a length-prefixed, non-owned native pointer (e.g. a `const
	/// char*` + separate `size_t*` out-param) into a new Java array. Unlike [#toJavaString],
	/// this does not free `ptr` -- use it for borrowed views the C API still owns, such as a
	/// pointer into an internal `std::string` that stays alive only as long as its parent object.
	///
	/// @param ptr non-NULL native pointer to a borrowed buffer
	/// @param len number of bytes to copy
	/// @return a new array containing a copy of the bytes
	public static byte[] toByteArray(MemorySegment ptr, long len) {
		return ptr.reinterpret(len).toArray(ValueLayout.JAVA_BYTE);
	}

	/// Decodes a borrowed, non-owned `const char*` + separate `size_t*` out-param pair as a
	/// UTF-8 [String], without freeing `ptr`. Same ownership contract as
	/// [#toByteArray(MemorySegment, long)]; use it for read-only accessors that hand back a view
	/// into a native `std::string` (e.g. event-listener job-info column family names and paths).
	///
	/// @param ptr native pointer to a borrowed buffer
	/// @param len number of bytes to decode
	/// @return the decoded string
	public static String toJavaString(MemorySegment ptr, long len) {
		return new String(toByteArray(ptr, len), StandardCharsets.UTF_8);
	}

	/// Converts a malloc'd, NUL-terminated `char*` returned by the RocksDB C API into a
	/// Java [String], then frees it.
	///
	/// @param ptr non-NULL `char*` allocated by RocksDB
	/// @return the decoded string
	public static String toJavaString(MemorySegment ptr) {
		String s = ptr.reinterpret(Long.MAX_VALUE).getString(0);
		free(ptr);
		return s;
	}

	/// [#toJavaString(MemorySegment)] for C APIs that return NULL instead of a value.
	///
	/// @param ptr `char*` allocated by RocksDB, or `MemorySegment.NULL`
	/// @return the decoded string, or [Optional#empty()] if `ptr` is NULL
	public static Optional<String> toOptionalString(MemorySegment ptr) {
		if (MemorySegment.NULL.equals(ptr)) {
			return Optional.empty();
		}
		return Optional.of(toJavaString(ptr));
	}

	/// Converts a Java `boolean` to the `unsigned char` (0 or 1) the C API expects.
	///
	/// @param value the boolean to convert
	/// @return `(byte) 1` if `value` is `true`, `(byte) 0` otherwise
	public static byte toByte(boolean value) {
		return value ? (byte) 1 : (byte) 0;
	}

	/// [#toByte(boolean)] in reverse: converts a C API `unsigned char` result back to a Java `boolean`.
	///
	/// @param value the native byte to convert
	/// @return `false` if `value` is `0`, `true` otherwise
	public static boolean fromByte(byte value) {
		return value != 0;
	}
}
