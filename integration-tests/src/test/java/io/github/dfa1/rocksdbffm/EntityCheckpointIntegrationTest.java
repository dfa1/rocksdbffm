package io.github.dfa1.rocksdbffm;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class EntityCheckpointIntegrationTest {

	private static final int ENTITY_COUNT = 1_000;

	@TempDir
	static Path sharedDir;

	private static Path cpDir;

	enum Status { ACTIVE, INACTIVE, PENDING }

	record Entity(long id, String name, Status status) {

		byte[] key() {
			return String.format("%010d", id).getBytes();
		}

		byte[] value() {
			return (name + ":" + status.name()).getBytes();
		}

		static byte[] keyFor(long id) {
			return String.format("%010d", id).getBytes();
		}

		static Entity decode(byte[] key, byte[] value) {
			long id = Long.parseLong(new String(key));
			String raw = new String(value);
			int sep = raw.lastIndexOf(':');
			return new Entity(id, raw.substring(0, sep), Status.valueOf(raw.substring(sep + 1)));
		}
	}

	@BeforeAll
	static void setup() {
		var dbDir = sharedDir.resolve("db");
		cpDir = sharedDir.resolve("checkpoint");
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = RocksDB.open(opts, dbDir);
		     var batch = WriteBatch.create()) {
			for (int i = 0; i < ENTITY_COUNT; i++) {
				var e = new Entity(i, "entity-" + i, Status.values()[i % Status.values().length]);
				batch.put(e.key(), e.value());
			}
			db.write(batch);
			try (var cp = Checkpoint.newCheckpoint(db)) {
				cp.exportTo(cpDir);
			}
		}
	}

	// -----------------------------------------------------------------------
	// Point lookups
	// -----------------------------------------------------------------------

	@Test
	void pointLookup_findsExistingEntity() {
		// Given — database seeded in @BeforeAll

		try (var db = RocksDB.openReadOnly(cpDir)) {
			// When
			var value = db.get(Entity.keyFor(42));

			// Then
			assertThat(value).isNotNull();
			var entity = Entity.decode(Entity.keyFor(42), value);
			assertThat(entity.id()).isEqualTo(42);
			assertThat(entity.name()).isEqualTo("entity-42");
			assertThat(entity.status()).isEqualTo(Status.values()[42 % Status.values().length]);
		}
	}

	@Test
	void pointLookup_returnsNull_forAbsentId() {
		// Given — database seeded in @BeforeAll

		try (var db = RocksDB.openReadOnly(cpDir)) {
			// When
			var value = db.get(Entity.keyFor(ENTITY_COUNT));

			// Then
			assertThat(value).isNull();
		}
	}

	// -----------------------------------------------------------------------
	// Range queries
	// -----------------------------------------------------------------------

	@Test
	void rangeQuery_returnsEntitiesInBounds() {
		// Given — database seeded in @BeforeAll
		long from = 100;
		long to = 200;

		try (var db = RocksDB.openReadOnly(cpDir)) {
			// When
			List<Entity> result = new ArrayList<>();
			try (var it = db.newIterator()) {
				for (it.seek(Entity.keyFor(from)); it.isValid(); it.next()) {
					var e = Entity.decode(it.key(), it.value());
					if (e.id() >= to) {
						break;
					}
					result.add(e);
				}
			}

			// Then
			assertThat(result).hasSize((int) (to - from));
			assertThat(result.getFirst().id()).isEqualTo(from);
			assertThat(result.getLast().id()).isEqualTo(to - 1);
		}
	}

	@Test
	void rangeQuery_filteredByStatus() {
		// Given — database seeded in @BeforeAll

		try (var db = RocksDB.openReadOnly(cpDir)) {
			// When
			List<Entity> active = new ArrayList<>();
			try (var it = db.newIterator()) {
				for (it.seekToFirst(); it.isValid(); it.next()) {
					var e = Entity.decode(it.key(), it.value());
					if (e.status() == Status.ACTIVE) {
						active.add(e);
					}
				}
			}

			// Then
			assertThat(active).isNotEmpty();
			assertThat(active).allMatch(e -> e.status() == Status.ACTIVE);
		}
	}

	// -----------------------------------------------------------------------
	// Checkpoint isolation — needs its own DB, cannot share
	// -----------------------------------------------------------------------

	@Test
	void checkpoint_doesNotReflectWritesAfterExport(@TempDir Path dir) {
		// Given
		var dbDir = dir.resolve("db");
		var isolatedCpDir = dir.resolve("checkpoint");
		try (var opts = Options.newOptions().setCreateIfMissing(true);
		     var db = RocksDB.open(opts, dbDir);
		     var batch = WriteBatch.create()) {
			for (int i = 0; i < ENTITY_COUNT; i++) {
				var e = new Entity(i, "entity-" + i, Status.values()[i % Status.values().length]);
				batch.put(e.key(), e.value());
			}
			db.write(batch);
			try (var cp = Checkpoint.newCheckpoint(db)) {
				cp.exportTo(isolatedCpDir);
			}
			// write after checkpoint
			db.put(Entity.keyFor(ENTITY_COUNT), new Entity(ENTITY_COUNT, "extra", Status.ACTIVE).value());
		}

		try (var db = RocksDB.openReadOnly(isolatedCpDir)) {
			// When
			var value = db.get(Entity.keyFor(ENTITY_COUNT));

			// Then
			assertThat(value).isNull();
		}
	}
}
