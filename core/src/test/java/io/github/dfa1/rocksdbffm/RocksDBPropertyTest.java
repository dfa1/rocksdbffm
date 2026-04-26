package io.github.dfa1.rocksdbffm;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.NotBlank;
import net.jqwik.api.constraints.StringLength;
import net.jqwik.api.lifecycle.AfterProperty;
import net.jqwik.api.lifecycle.BeforeProperty;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class RocksDBPropertyTest {

	private Path tempDir;
	private ReadWriteDB db;

	@BeforeProperty
	void setUp() throws IOException {
		tempDir = Files.createTempDirectory("rocksdb-pbt-");
		db = RocksDB.open(tempDir);
	}

	@AfterProperty
	void tearDown() throws IOException {
		db.close();
		Files.walk(tempDir)
				.sorted(Comparator.reverseOrder())
				.map(Path::toFile)
				.forEach(File::delete);
	}

	// -----------------------------------------------------------------------
	// put / get / delete
	// -----------------------------------------------------------------------

	@Property(tries = 100)
	void put_getIsRoundTrip(
			@ForAll @NotBlank @StringLength(max = 32) String key,
			@ForAll @NotBlank @StringLength(max = 32) String value) {
		// Given - db shared across tries via @BeforeProperty
		db.put(key.getBytes(), value.getBytes());

		// When
		var result = db.get(key.getBytes());

		// Then
		assertThat(result).isEqualTo(value.getBytes());
	}

	@Property(tries = 100)
	void delete_removesKey(
			@ForAll @NotBlank @StringLength(max = 32) String key,
			@ForAll @NotBlank @StringLength(max = 32) String value) {
		// Given - db shared across tries via @BeforeProperty
		db.put(key.getBytes(), value.getBytes());
		db.delete(key.getBytes());

		// When
		var result = db.get(key.getBytes());

		// Then
		assertThat(result).isNull();
	}

	@Property(tries = 100)
	void put_overwritesExistingKey(
			@ForAll @NotBlank @StringLength(max = 32) String key,
			@ForAll @NotBlank @StringLength(max = 32) String value1,
			@ForAll @NotBlank @StringLength(max = 32) String value2) {
		// Given - db shared across tries via @BeforeProperty
		db.put(key.getBytes(), value1.getBytes());
		db.put(key.getBytes(), value2.getBytes());

		// When
		var result = db.get(key.getBytes());

		// Then
		assertThat(result).isEqualTo(value2.getBytes());
	}

	// -----------------------------------------------------------------------
	// Iterator
	// -----------------------------------------------------------------------

	@Property(tries = 100)
	void iterator_visitsKeysInLexicographicOrder(
			@ForAll List<@NotBlank @StringLength(max = 16) String> keys) {
		// Given - db shared across tries via @BeforeProperty
		for (String key : keys) {
			db.put(key.getBytes(), "v".getBytes());
		}

		// When
		List<byte[]> observed = new ArrayList<>();
		try (RocksIterator it = db.newIterator()) {
			for (it.seekToFirst(); it.isValid(); it.next()) {
				observed.add(it.key());
			}
			it.checkError();
		}

		// Then
		for (int i = 1; i < observed.size(); i++) {
			assertThat(Arrays.compareUnsigned(observed.get(i - 1), observed.get(i)))
					.as("key[%d] must be strictly less than key[%d]", i - 1, i)
					.isNegative();
		}
	}
}
