package com.github.jparkie.pdd.impl;

import com.github.jparkie.pdd.BitArray;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Random;

import static org.junit.Assert.*;

public class BSBFSDDeDuplicatorTest {
    private static final double FPP_DELTA = 1E-3;
    private static final double FNP_DELTA = 1E-2;
    private static final long NUM_BITS = 512 * 8L;
    private static final long RANDOM_SEED = 13L;
    private static final int CARDINALITY = (int) 1E3;
    private static final int MAX_SEQUENCE_NUMBER = (int) 1E6;

    @Test(expected = IllegalArgumentException.class)
    public void testCreateFppLowerBound() {
        BSBFSDDeDuplicator.create(64L, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateFppUpperBound() {
        BSBFSDDeDuplicator.create(64L, 1);
    }

    @Test
    public void testCreate() {
        final BSBFSDDeDuplicator deDuplicator = BSBFSDDeDuplicator.create(64L, 0.03D);
        assertEquals(64L, deDuplicator.numBits());
        assertEquals(5, deDuplicator.numHashFunctions());
        assertEquals(5, deDuplicator.bloomFilters.length);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorInvalidNumBits() {
        new BSBFSDDeDuplicator(0L, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorInvalidNumHashFunctions() {
        new BSBFSDDeDuplicator(64L, 0);
    }

    @Test
    public void testConstructor() {
        final BSBFSDDeDuplicator deDuplicator = new BSBFSDDeDuplicator(64L, 1);
        assertEquals(64L, deDuplicator.numBits());
        assertEquals(1, deDuplicator.numHashFunctions());
        assertEquals(1, deDuplicator.bloomFilters.length);
    }

    @Test
    public void testClassifyDistinct() {
        final BSBFSDDeDuplicator deDuplicator = new BSBFSDDeDuplicator(64L, 2);
        final Random random = new Random();
        final byte[] element = new byte[128];
        random.nextBytes(element);
        assertTrue(deDuplicator.classifyDistinct(element));
        assertFalse(deDuplicator.classifyDistinct(element));
    }

    @Test
    public void testPeekDistinct() {
        final BSBFSDDeDuplicator deDuplicator = new BSBFSDDeDuplicator(64L, 2);
        final Random random = new Random();
        final byte[] element = new byte[128];
        random.nextBytes(element);
        assertTrue(deDuplicator.peekDistinct(element));
        assertTrue(deDuplicator.peekDistinct(element));
    }

    @Test
    public void testEstimateFpp() {
        final BSBFSDDeDuplicator deDuplicator = new BSBFSDDeDuplicator(NUM_BITS, 2);
        final Random random = new Random(RANDOM_SEED);
        final ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES);
        final boolean[] isVisited = new boolean[CARDINALITY];
        int fpNumber = 0;
        for (long sequenceNumber = 1; sequenceNumber <= MAX_SEQUENCE_NUMBER; sequenceNumber++) {
            final int currentElement = random.nextInt(CARDINALITY);
            byteBuffer.clear();
            byteBuffer.putInt(currentElement);
            final boolean actuallyDistinct = !isVisited[currentElement];
            final boolean reportedDuplicate = !deDuplicator.classifyDistinct(byteBuffer.array());
            if (actuallyDistinct && reportedDuplicate) {
                fpNumber++;
            }
            isVisited[currentElement] = true;
        }
        final double actuallyDistinctProbability = Math.pow((CARDINALITY - 1D) / CARDINALITY, MAX_SEQUENCE_NUMBER);
        final double actualFpp = ((double) fpNumber) / ((double) MAX_SEQUENCE_NUMBER);
        final double estimatedFpp = deDuplicator.estimateFpp(actuallyDistinctProbability);
        assertEquals(actualFpp, estimatedFpp, FPP_DELTA);
    }

    @Test
    public void testEstimateFnp() {
        final BSBFSDDeDuplicator deDuplicator = new BSBFSDDeDuplicator(NUM_BITS, 2);
        final Random random = new Random(RANDOM_SEED);
        final ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES);
        final boolean[] isVisited = new boolean[CARDINALITY];
        int fnNumber = 0;
        for (long sequenceNumber = 1; sequenceNumber <= MAX_SEQUENCE_NUMBER; sequenceNumber++) {
            final int currentElement = random.nextInt(CARDINALITY);
            byteBuffer.clear();
            byteBuffer.putInt(currentElement);
            final boolean actuallyDuplicate = isVisited[currentElement];
            final boolean reportedDistinct = deDuplicator.classifyDistinct(byteBuffer.array());
            if (actuallyDuplicate && reportedDistinct) {
                fnNumber++;
            }
            isVisited[currentElement] = true;
        }
        final double actuallyDistinctProbability = Math.pow((CARDINALITY - 1D) / CARDINALITY, MAX_SEQUENCE_NUMBER);
        final double actualFnp = ((double) fnNumber) / ((double) MAX_SEQUENCE_NUMBER);
        final double estimatedFnp = deDuplicator.estimateFnp(actuallyDistinctProbability);
        assertEquals(actualFnp, estimatedFnp, FNP_DELTA);
    }

    @Test
    public void testReset() {
        final BSBFSDDeDuplicator deDuplicator = new BSBFSDDeDuplicator(64L, 2);
        final Random random = new Random();
        final byte[] element = new byte[128];
        random.nextBytes(element);
        assertTrue(deDuplicator.classifyDistinct(element));
        deDuplicator.reset();
        for (BitArray bloomFilter : deDuplicator.bloomFilters) {
            assertEquals(0L, bloomFilter.bitCount());
        }
        assertEquals(0D, deDuplicator.reportedDuplicateProbability, 0);
    }

    @Test
    public void testJavaSerializable() throws IOException, ClassNotFoundException {
        final BSBFSDDeDuplicator deDuplicator = new BSBFSDDeDuplicator(64L, 1);
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
        final BSBFSDDeDuplicator serialized = (BSBFSDDeDuplicator) ois.readObject();
        ois.close();
        in.close();
        assertEquals(deDuplicator, serialized);
    }
}
