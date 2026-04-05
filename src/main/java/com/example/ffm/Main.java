package com.example.ffm;

import java.nio.file.Files;
import java.nio.file.Path;

public class Main {

    static void main() throws Exception {
        Path dbPath = Path.of("/tmp/ffm-rocksdb-ffm-example");
        Files.createDirectories(dbPath);

        try (RocksDB db = RocksDB.open(dbPath.toString())) {
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
