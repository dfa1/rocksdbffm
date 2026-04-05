package com.example;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import java.nio.file.Files;
import java.nio.file.Path;

public class Main {

    static {
        RocksDB.loadLibrary();
    }

    static void main() throws Exception {
        Path dbPath = Path.of("/tmp/ffm-rocksdb-example");
        Files.createDirectories(dbPath);

        try (Options options = new Options().setCreateIfMissing(true);
             RocksDB db = RocksDB.open(options, dbPath.toString())) {

            byte[] key = "hello".getBytes();
            byte[] value = "world".getBytes();

            db.put(key, value);

            byte[] result = db.get(key);
            System.out.println("hello -> " + new String(result));

            db.delete(key);
            System.out.println("Deleted key, get returns: " + db.get(key));
        }
    }
}
