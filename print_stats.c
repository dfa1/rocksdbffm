#include <stdio.h>
#include <rocksdb/statistics.h>

int main() {
    printf("NUMBER_KEYS_WRITTEN: %d\n", rocksdb::NUMBER_KEYS_WRITTEN);
    printf("NUMBER_KEYS_READ: %d\n", rocksdb::NUMBER_KEYS_READ);
    printf("DB_GET: %d\n", rocksdb::DB_GET);
    return 0;
}
