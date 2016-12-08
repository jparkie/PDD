package com.github.jparkie.pdd;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

/**
 * Adapted From: https://github.com/apache/spark/blob/branch-2.0/common/sketch/src/main/java/org/apache/spark/util/sketch/BitArray.java
 */
public final class BitArray {
    private final long[] data;
    private long bitCount;

    public BitArray(long numBits) {
        this(new long[numWords(numBits)]);
    }

    private BitArray(long[] data) {
        this.data = data;
        long bitCount = 0;
        for (long word : data) {
            bitCount += Long.bitCount(word);
        }
        this.bitCount = bitCount;
    }

    private static int numWords(long numBits) {
        if (numBits <= 0) {
            final String error = String.format("numBits must be positive, but got %d", numBits);
            throw new IllegalArgumentException(error);
        }
        final long numWords = (long) Math.ceil(numBits / 64D);
        if (numWords > Integer.MAX_VALUE) {
            final String error = String.format("Cannot allocate enough space for %d bits", numBits);
            throw new IllegalArgumentException(error);
        }
        return (int) numWords;
    }

    public boolean get(long index) {
        final int arrayIndex = (int)(index >>> 6);
        final long bitMask = 1L << index;
        return (data[arrayIndex] & bitMask) != 0;
    }

    public boolean set(long index) {
        final int arrayIndex = (int)(index >>> 6);
        final long bitMask = 1L << index;
        if ((data[arrayIndex] & bitMask) == 0) {
            data[arrayIndex] |= bitMask;
            bitCount++;
            return true;
        }
        return false;
    }

    public boolean clear(long index) {
        final int arrayIndex = (int)(index >>> 6);
        final long bitMask = 1L << index;
        if ((data[arrayIndex] & bitMask) != 0) {
            data[arrayIndex] &= ~bitMask;
            bitCount--;
            return true;
        }
        return false;
    }

    public long bitSize() {
        return (long) data.length * Long.SIZE;
    }

    public long bitCount() {
        return bitCount;
    }

    public void putAll(BitArray array) {
        if (data.length != array.data.length) {
            final String error = String.format(
                    "BitArrays must be of equal length (%d != %d)",
                    data.length,
                    array.data.length);
            throw new IllegalArgumentException(error);
        }
        long bitCount = 0;
        for (int i = 0; i < data.length; i++) {
            data[i] |= array.data[i];
            bitCount += Long.bitCount(data[i]);
        }
        this.bitCount = bitCount;
    }

    // @formatter:off
    public void writeTo(DataOutputStream out) throws IOException {
        out.writeInt(data.length);
        for (long datum : data) {
            out.writeLong(datum);
        }
    }

    public static BitArray readFrom(DataInputStream in) throws IOException {
        final int numWords = in.readInt();
        long[] data = new long[numWords];
        for (int i = 0; i < numWords; i++) {
            data[i] = in.readLong();
        }
        return new BitArray(data);
    }
    // @formatter:on

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        final BitArray that = (BitArray) other;
        return Arrays.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }
}
