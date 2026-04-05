package io.github.dfa1.rocksdbffm;

public class RocksDBException extends RuntimeException {

    public RocksDBException(String message) {
        super(message);
    }

    public RocksDBException(String message, Throwable cause) {
        super(message, cause);
    }
}
