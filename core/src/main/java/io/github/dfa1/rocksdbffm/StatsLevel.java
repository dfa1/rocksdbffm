package io.github.dfa1.rocksdbffm;

/**
 * Statistics levels for RocksDB.
 */
public enum StatsLevel {
    DISABLE_ALL(0),
    EXCEPT_TICKERS(0),
    EXCEPT_HISTOGRAM_OR_TIMERS(1),
    EXCEPT_TIMERS(2),
    EXCEPT_DETAILED_TIMERS(3),
    EXCEPT_TIME_FOR_MUTEX(4),
    ALL(5);

    private final int value;

    StatsLevel(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
