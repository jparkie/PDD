package com.github.jparkie.pdd.impl;

import com.github.jparkie.pdd.BitArray;
import org.junit.Test;

import java.io.*;
import java.util.Random;

import static org.junit.Assert.*;

public class BSBFDeDuplicatorTest {
    @Test(expected = IllegalArgumentException.class)
    public void testCreateFppLowerBound() {
        BSBFDeDuplicator.create(64L, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateFppUpperBound() {
        BSBFDeDuplicator.create(64L, 1);
    }

    @Test
    public void testCreate() {
        final BSBFDeDuplicator deDuplicator = BSBFDeDuplicator.create(64L, 0.03D);
        assertEquals(64L, deDuplicator.numBits());
        assertEquals(5, deDuplicator.numHashFunctions());
        assertEquals(5, deDuplicator.bloomFilters.length);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorInvalidNumBits() {
        new BSBFDeDuplicator(0L, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorInvalidNumHashFunctions() {
        new BSBFDeDuplicator(64L, 0);
    }

    @Test
    public void testConstructor() {
        final BSBFDeDuplicator deDuplicator = new BSBFDeDuplicator(64L, 1);
        assertEquals(64L, deDuplicator.numBits());
        assertEquals(1, deDuplicator.numHashFunctions());
        assertEquals(1, deDuplicator.bloomFilters.length);
    }

    @Test
    public void testClassifyDistinct() {
        final BSBFDeDuplicator deDuplicator = new BSBFDeDuplicator(64L, 2);
        final Random random = new Random();
        final byte[] element = new byte[128];
        random.nextBytes(element);
        assertTrue(deDuplicator.classifyDistinct(element));
        assertFalse(deDuplicator.classifyDistinct(element));
    }

    @Test
    public void testPeekDistinct() {
        final BSBFDeDuplicator deDuplicator = new BSBFDeDuplicator(64L, 2);
        final Random random = new Random();
        final byte[] element = new byte[128];
        random.nextBytes(element);
        assertTrue(deDuplicator.peekDistinct(element));
        assertTrue(deDuplicator.peekDistinct(element));
    }

    @Test
    public void testReset() {
        final BSBFDeDuplicator deDuplicator = new BSBFDeDuplicator(64L, 2);
        final Random random = new Random();
        final byte[] element = new byte[128];
        random.nextBytes(element);
        assertTrue(deDuplicator.classifyDistinct(element));
        deDuplicator.reset();
        for (BitArray bloomFilter : deDuplicator.bloomFilters) {
            assertEquals(0L, bloomFilter.bitCount());
        }
    }

    @Test
    public void testJavaSerializable() throws IOException, ClassNotFoundException {
        final BSBFDeDuplicator deDuplicator = new BSBFDeDuplicator(64L, 1);
        final Random random = new Random();
        final byte[] element = new byte[128];
        random.nextBytes(element);
        assertTrue(deDuplicator.classifyDistinct(element));
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final ObjectOutputStream oos = new ObjectOutputStream(out);
        oos.writeObject(deDuplicator);
        oos.close();
        out.close();
        final ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        final ObjectInputStream ois = new ObjectInputStream(in);
        final BSBFDeDuplicator serialized = (BSBFDeDuplicator) ois.readObject();
        ois.close();
        in.close();
        assertEquals(deDuplicator, serialized);
    }
}
