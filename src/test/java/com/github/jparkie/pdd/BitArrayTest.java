package com.github.jparkie.pdd;

import org.junit.Test;

import java.io.*;

import static org.junit.Assert.*;

public class BitArrayTest {
    @Test(expected = IllegalArgumentException.class)
    public void testNumWordsLowerBound() {
        new BitArray(0L);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNumWordsUpperBound() {
        new BitArray(64L * Integer.MAX_VALUE + 1L);
    }

    @Test
    public void testGetSetClear() {
        final BitArray bitArray = new BitArray(64L);
        assertFalse(bitArray.get(0L));
        assertTrue(bitArray.set(0L));
        assertTrue(bitArray.get(0L));
        assertFalse(bitArray.set(0L));
        assertTrue(bitArray.get(0L));
        assertTrue(bitArray.clear(0L));
        assertFalse(bitArray.get(0L));
        assertFalse(bitArray.clear(0L));
        assertFalse(bitArray.get(0L));
    }

    @Test
    public void testBitSize() {
        assertEquals(64L, new BitArray(64L).bitSize());
        assertEquals(128L, new BitArray(65L).bitSize());
        assertEquals(128L, new BitArray(127L).bitSize());
        assertEquals(128L, new BitArray(128L).bitSize());
    }

    @Test
    public void testBitCount() {
        final BitArray bitArray = new BitArray(64L);
        assertEquals(0L, bitArray.bitCount());
        assertTrue(bitArray.set(0L));
        assertEquals(1L, bitArray.bitCount());
        assertFalse(bitArray.set(0L));
        assertEquals(1L, bitArray.bitCount());
        assertTrue(bitArray.clear(0L));
        assertEquals(0L, bitArray.bitCount());
        assertFalse(bitArray.clear(0L));
        assertEquals(0L, bitArray.bitCount());
    }

    @Test
    public void testWriteToReadFrom() throws IOException {
        final BitArray bitArray = new BitArray(64L);
        assertTrue(bitArray.set(0L));
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final DataOutputStream dos = new DataOutputStream(out);
        bitArray.writeTo(dos);
        out.close();
        final ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        final DataInputStream dis = new DataInputStream(in);
        final BitArray serialized = BitArray.readFrom(dis);
        in.close();
        assertEquals(bitArray, serialized);
    }
}
