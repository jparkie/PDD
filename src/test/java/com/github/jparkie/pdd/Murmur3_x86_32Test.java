package com.github.jparkie.pdd;

import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * From: https://github.com/apache/spark/blob/branch-2.0/common/unsafe/src/test/java/org/apache/spark/unsafe/hash/Murmur3_x86_32Suite.java
 */
public class Murmur3_x86_32Test {
    @Test
    public void testKnownIntegerInputs() {
        final Murmur3_x86_32 hasher = new Murmur3_x86_32(0);
        assertEquals(593689054, hasher.hashInt(0));
        assertEquals(-189366624, hasher.hashInt(-42));
        assertEquals(-1134849565, hasher.hashInt(42));
        assertEquals(-1718298732, hasher.hashInt(Integer.MIN_VALUE));
        assertEquals(-1653689534, hasher.hashInt(Integer.MAX_VALUE));
    }

    @Test
    public void testKnownLongInputs() {
        final Murmur3_x86_32 hasher = new Murmur3_x86_32(0);
        assertEquals(1669671676, hasher.hashLong(0L));
        assertEquals(-846261623, hasher.hashLong(-42L));
        assertEquals(1871679806, hasher.hashLong(42L));
        assertEquals(1366273829, hasher.hashLong(Long.MIN_VALUE));
        assertEquals(-2106506049, hasher.hashLong(Long.MAX_VALUE));
    }

    @Test
    public void randomizedStressTest() {
        final Murmur3_x86_32 hasher = new Murmur3_x86_32(0);
        final int size = 65536;
        final Random random = new Random();
        // A set used to track collision rate.
        final Set<Integer> hashcodes = new HashSet<>();
        for (int i = 0; i < size; i++) {
            final int vint = random.nextInt();
            final long lint = random.nextLong();
            assertEquals(hasher.hashInt(vint), hasher.hashInt(vint));
            assertEquals(hasher.hashLong(lint), hasher.hashLong(lint));
            hashcodes.add(hasher.hashLong(lint));
        }
        // A very loose bound.
        assertTrue(hashcodes.size() > size * 0.95);
    }

    @Test
    public void randomizedStressTestBytes() {
        final Murmur3_x86_32 hasher = new Murmur3_x86_32(0);
        final int size = 65536;
        final Random random = new Random();
        // A set used to track collision rate.
        final Set<Integer> hashcodes = new HashSet<>();
        for (int i = 0; i < size; i++) {
            final int byteArrSize = random.nextInt(100) * 8;
            final byte[] bytes = new byte[byteArrSize];
            random.nextBytes(bytes);
            assertEquals(
                    hasher.hashUnsafeWords(bytes, Platform.BYTE_ARRAY_OFFSET, byteArrSize),
                    hasher.hashUnsafeWords(bytes, Platform.BYTE_ARRAY_OFFSET, byteArrSize));
            hashcodes.add(hasher.hashUnsafeWords(bytes, Platform.BYTE_ARRAY_OFFSET, byteArrSize));
        }
        // A very loose bound.
        assertTrue(hashcodes.size() > size * 0.95);
    }

    @Test
    public void randomizedStressTestPaddedStrings() {
        final Murmur3_x86_32 hasher = new Murmur3_x86_32(0);
        final int size = 64000;
        // A set used to track collision rate.
        final Set<Integer> hashcodes = new HashSet<>();
        for (int i = 0; i < size; i++) {
            final int byteArrSize = 8;
            final byte[] strBytes = String.valueOf(i).getBytes(StandardCharsets.UTF_8);
            final byte[] paddedBytes = new byte[byteArrSize];
            System.arraycopy(strBytes, 0, paddedBytes, 0, strBytes.length);
            assertEquals(
                    hasher.hashUnsafeWords(paddedBytes, Platform.BYTE_ARRAY_OFFSET, byteArrSize),
                    hasher.hashUnsafeWords(paddedBytes, Platform.BYTE_ARRAY_OFFSET, byteArrSize));
            hashcodes.add(hasher.hashUnsafeWords(paddedBytes, Platform.BYTE_ARRAY_OFFSET, byteArrSize));
        }
        // A very loose bound.
        assertTrue(hashcodes.size() > size * 0.95);
    }
}
