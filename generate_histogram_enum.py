import re

with open('/opt/homebrew/Cellar/rocksdb/10.10.1/include/rocksdb/statistics.h', 'r') as f:
    content = f.read()

# Match Histograms enum
hist_match = re.search(r'enum Histograms : uint32_t \{(.*?)\};', content, re.DOTALL)
if hist_match:
    hists = hist_match.group(1)
    # Remove comments
    hists = re.sub(r'//.*', '', hists)
    hists = re.sub(r'/\*.*?\*/', '', hists, flags=re.DOTALL)
    
    hist_list = []
    current_val = 0
    for line in hists.split('\n'):
        line = line.strip()
        if not line: continue
        
        parts = re.split(r'[,=]', line)
        name = parts[0].strip()
        if not name: continue
        
        if '=' in line:
            current_val = int(parts[1].strip())
        
        hist_list.append((name, current_val))
        current_val += 1
    
    print("package io.github.dfa1.rocksdbffm;")
    print("")
    print("/**")
    print(" * Histogram types for RocksDB statistics.")
    print(" * Corresponds to {@code enum Histograms} in {@code rocksdb/statistics.h}.")
    print(" */")
    print("public enum HistogramType {")
    for name, val in hist_list:
        if name == "HISTOGRAM_ENUM_MAX": continue
        print(f"    {name}({val}),")
    print("    ;")
    print("")
    print("    private final int value;")
    print("")
    print("    HistogramType(int value) {")
    print("        this.value = value;")
    print("    }")
    print("")
    print("    public int getValue() {")
    print("        return value;")
    print("    }")
    print("}")
