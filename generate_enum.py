import re

with open('/opt/homebrew/Cellar/rocksdb/10.10.1/include/rocksdb/statistics.h', 'r') as f:
    content = f.read()

# Match Tickers enum
tickers_match = re.search(r'enum Tickers : uint32_t \{(.*?)\};', content, re.DOTALL)
if tickers_match:
    tickers = tickers_match.group(1)
    # Remove comments
    tickers = re.sub(r'//.*', '', tickers)
    tickers = re.sub(r'/\*.*?\*/', '', tickers, flags=re.DOTALL)
    
    ticker_list = []
    current_val = 0
    for line in tickers.split('\n'):
        line = line.strip()
        if not line: continue
        
        parts = re.split(r'[,=]', line)
        name = parts[0].strip()
        if not name: continue
        
        if '=' in line:
            current_val = int(parts[1].strip())
        
        ticker_list.append((name, current_val))
        current_val += 1
    
    print("package io.github.dfa1.rocksdbffm;")
    print("")
    print("/**")
    print(" * Ticker types for RocksDB statistics.")
    print(" * Corresponds to {@code enum Tickers} in {@code rocksdb/statistics.h}.")
    print(" */")
    print("public enum TickerType {")
    for name, val in ticker_list:
        if name == "TICKER_ENUM_MAX": continue
        print(f"    {name}({val}),")
    print("    ;")
    print("")
    print("    private final int value;")
    print("")
    print("    TickerType(int value) {")
    print("        this.value = value;")
    print("    }")
    print("")
    print("    public int getValue() {")
    print("        return value;")
    print("    }")
    print("}")
