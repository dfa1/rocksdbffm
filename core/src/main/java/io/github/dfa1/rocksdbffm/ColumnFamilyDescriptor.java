package io.github.dfa1.rocksdbffm;

import java.nio.charset.StandardCharsets;

/// Describes a column family by name and optional per-family options.
///
/// Used when opening a database with multiple column families or creating a new one.
/// If no [Options] are supplied the DB-level options are used for the column family.
///
/// The caller retains ownership of any [Options] passed to this descriptor.
public final class ColumnFamilyDescriptor {

	private final byte[] name;
	private final Options options;

	private ColumnFamilyDescriptor(byte[] name, Options options) {
		this.name = name;
		this.options = options;
	}

	/// Creates a descriptor with the given raw-byte name and no explicit options.
	public static ColumnFamilyDescriptor of(byte[] name) {
		return new ColumnFamilyDescriptor(name, null);
	}

	/// Creates a descriptor with the given raw-byte name and per-family options.
	public static ColumnFamilyDescriptor of(byte[] name, Options options) {
		return new ColumnFamilyDescriptor(name, options);
	}

	/// Creates a descriptor with the given UTF-8 name and no explicit options.
	public static ColumnFamilyDescriptor of(String name) {
		return new ColumnFamilyDescriptor(name.getBytes(StandardCharsets.UTF_8), null);
	}

	/// Creates a descriptor with the given UTF-8 name and per-family options.
	public static ColumnFamilyDescriptor of(String name, Options options) {
		return new ColumnFamilyDescriptor(name.getBytes(StandardCharsets.UTF_8), options);
	}

	/// Returns the column family name as raw bytes.
	public byte[] name() {
		return name;
	}

	/// Returns the name as a UTF-8 string.
	public String nameAsString() {
		return new String(name, StandardCharsets.UTF_8);
	}

	/// Returns the per-family options, or `null` if none were specified.
	Options options() {
		return options;
	}
}
