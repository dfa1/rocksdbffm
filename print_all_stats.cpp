#include <stdio.h>
#include <rocksdb/statistics.h>

int main() {
    printf("TickerType.BLOCK_CACHE_MISS: %d\n", (int)rocksdb::Tickers::BLOCK_CACHE_MISS);
    printf("TickerType.NUMBER_KEYS_WRITTEN: %d\n", (int)rocksdb::Tickers::NUMBER_KEYS_WRITTEN);
    printf("TickerType.NUMBER_KEYS_READ: %d\n", (int)rocksdb::Tickers::NUMBER_KEYS_READ);
    printf("HistogramType.DB_GET: %d\n", (int)rocksdb::Histograms::DB_GET);
    printf("HistogramType.DB_WRITE: %d\n", (int)rocksdb::Histograms::DB_WRITE);
    return 0;
}
