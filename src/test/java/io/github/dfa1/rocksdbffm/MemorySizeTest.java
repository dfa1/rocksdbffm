package io.github.dfa1.rocksdbffm;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class MemorySizeTest {

    @Test
    void factoriesProduceCorrectByteValues() {
        assertEquals(1024, MemorySize.ofKB(1).toBytes());
        assertEquals(1024 * 1024, MemorySize.ofMB(1).toBytes());
        assertEquals(1024 * 1024 * 1024L, MemorySize.ofGB(1).toBytes());
        assertEquals(42, MemorySize.ofBytes(42).toBytes());
    }

    @Test
    void zeroConstant() {
        assertEquals(0, MemorySize.ZERO.toBytes());
        assertEquals(MemorySize.ofBytes(0), MemorySize.ZERO);
    }

    @Test
    void rejectsNegativeValues() {
        assertThrows(IllegalArgumentException.class, () -> MemorySize.ofBytes(-1));
        assertThrows(IllegalArgumentException.class, () -> MemorySize.ofKB(-1));
        assertThrows(IllegalArgumentException.class, () -> MemorySize.ofMB(-1));
        assertThrows(IllegalArgumentException.class, () -> MemorySize.ofGB(-1));
    }

    @Test
    void overflowInFactoryThrows() {
        assertThrows(ArithmeticException.class, () -> MemorySize.ofGB(Long.MAX_VALUE));
        assertThrows(ArithmeticException.class, () -> MemorySize.ofMB(Long.MAX_VALUE));
    }

    @Test
    void valueEquality() {
        assertEquals(MemorySize.ofKB(1024), MemorySize.ofMB(1));
        assertEquals(MemorySize.ofMB(1024), MemorySize.ofGB(1));
        assertNotEquals(MemorySize.ofKB(1), MemorySize.ofMB(1));
    }

    @Test
    void hashCodeConsistentWithEquals() {
        assertEquals(MemorySize.ofMB(1).hashCode(), MemorySize.ofKB(1024).hashCode());
    }

    @Test
    void compareTo() {
        assertTrue(MemorySize.ofKB(1).compareTo(MemorySize.ofMB(1)) < 0);
        assertTrue(MemorySize.ofGB(1).compareTo(MemorySize.ofMB(1)) > 0);
        assertEquals(0, MemorySize.ofMB(1).compareTo(MemorySize.ofKB(1024)));
    }

    @Test
    void add() {
        assertEquals(MemorySize.ofMB(3), MemorySize.ofMB(1).add(MemorySize.ofMB(2)));
        assertEquals(MemorySize.ofKB(1536), MemorySize.ofMB(1).add(MemorySize.ofKB(512)));
    }

    @Test
    void toStringShowsLargestEvenUnit() {
        assertEquals("1 GB", MemorySize.ofGB(1).toString());
        assertEquals("64 MB", MemorySize.ofMB(64).toString());
        assertEquals("16 KB", MemorySize.ofKB(16).toString());
        assertEquals("100 B", MemorySize.ofBytes(100).toString());
        assertEquals("0 B",   MemorySize.ZERO.toString());
        // 1.5 KB = 1536 bytes — not evenly divisible by KB, MB, or GB
        assertEquals("1536 B", MemorySize.ofBytes(1536).toString());
    }
}
