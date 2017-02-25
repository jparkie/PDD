package com.github.jparkie.pdd.impl;

import com.github.jparkie.pdd.BitArray;
import com.github.jparkie.pdd.Murmur3_x86_32;
import com.github.jparkie.pdd.Platform;
import com.github.jparkie.pdd.ProbabilisticDeDuplicator;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.SplittableRandom;

/**
 * De-Duplication by a Biased Sampling based Bloom Filter (BSBF).
 * <p>
 * Described by Suman K. Bera, Sourav Dutta, Ankur Narang, Souvik Bhattacherjee in
 * Advanced Bloom Filter Based Algorithms for Efficient Approximate Data De-Duplication in Streams:
 * <p>
 * https://arxiv.org/abs/1212.3964
 */
public class BSBFDeDuplicator implements ProbabilisticDeDuplicator, Serializable {
    private static final BSBFDeDuplicatorSerializer serializer = new BSBFDeDuplicatorSerializer();

    long numBits;
    int numHashFunctions;
    BitArray[] bloomFilters;

    private transient int[] hashBuffer;
    private transient SplittableRandom random;

    public BSBFDeDuplicator(long numBits, int numHashFunctions) {
        this(numBits, numHashFunctions, bloomFilters(numBits, numHashFunctions));
    }

    BSBFDeDuplicator(long numBits, int numHashFunctions, BitArray[] bloomFilters) {
        this.numBits = numBits;
        this.numHashFunctions = numHashFunctions;
        this.bloomFilters = bloomFilters;
        this.hashBuffer = new int[this.bloomFilters.length];
        this.random = new SplittableRandom(generateRandomSeed(numBits, numHashFunctions));
    }

    private BSBFDeDuplicator() {
    }

    public static BSBFDeDuplicator create(long numBits, double fpp) {
        return new BSBFDeDuplicator(numBits, optimalNumOfHashFunctions(fpp));
    }

    private static int optimalNumOfHashFunctions(double fpp) {
        if (fpp <= 0D || fpp >= 1D) {
            final String error = String.format("fpp must be in the range (0, 1), but got %f", fpp);
            throw new IllegalArgumentException(error);
        }
        /*
         * From Advanced Bloom Filter Based Algorithms for Efficient Approximate Data De-Duplication in Streams:
         * As a trade-off we set k as the arithmetic mean of 1 and ln(fpp) / ln(1 - 1/e).
         */
        return (int) Math.ceil(((Math.log(fpp) / Math.log(1D - (1D / Math.E))) + 1D) / 2D);
    }

    private static BitArray[] bloomFilters(long numBits, int numHashFunctions) {
        if (numBits <= 0L) {
            final String error = String.format("numBits must be positive, but got %d", numBits);
            throw new IllegalArgumentException(error);
        }
        if (numHashFunctions <= 0) {
            final String error = String.format("numHashFunctions must be positive, but got %d", numHashFunctions);
            throw new IllegalArgumentException(error);
        }
        final long bloomFilterBits = numBits / numHashFunctions;
        final BitArray[] bloomFilters = new BitArray[numHashFunctions];
        for (int index = 0; index < numHashFunctions; index++) {
            bloomFilters[index] = new BitArray(bloomFilterBits);
        }
        return bloomFilters;
    }

    private static long generateRandomSeed(long numBits, int numHashFunctions) {
        return 31L * numBits  + numHashFunctions;
    }

    @Override
    public long numBits() {
        return numBits;
    }

    @Override
    public int numHashFunctions() {
        return numHashFunctions;
    }

    @Override
    public boolean classifyDistinct(byte[] element) {
        /*
         * Algorithm 2: BSBF (S)
         * Require: Threshold FPR (FPRt), Memory in bits (M), and Stream (S)
         * Ensure: Detecting duplicate and distinct elements in S
         *
         * Compute the value of k from FPRt.
         * Construct k Bloom filters each having M/k bits of memory.
         *
         * for each element e of S do
         *   Hash e into k bit positions, H = h1,··· ,hk.
         *   if all bit at positions H are set then
         *     Result ← DISTINCT
         *   else
         *     Result ← DUPLICATE
         *   end if
         *   if e is DISTINCT then
         *     Randomly select k bit positions hatH = hˆ1, hˆ2, ..., hˆk one each from the k Bloom filters.
         *     Reset all bits in Hˆ to 0.
         *     Set all the bits in H to 1.
         *   end if
         * end for
         */
        fillHashBuffer(element, hashBuffer);
        final boolean temporaryIsDistinct = !containsHashBuffer(bloomFilters, hashBuffer);
        if (temporaryIsDistinct) {
            setHashBuffer(bloomFilters, hashBuffer, random);
        }
        return temporaryIsDistinct;
    }

    @Override
    public boolean peekDistinct(byte[] element) {
        fillHashBuffer(element, hashBuffer);
        return !containsHashBuffer(bloomFilters, hashBuffer);
    }

    @Override
    public void reset() {
        final int bloomFiltersLength = bloomFilters.length;
        for (int index = 0; index < bloomFiltersLength; index++) {
            bloomFilters[index] = new BitArray(bloomFilters[index].bitSize());
        }
    }

    private void fillHashBuffer(byte[] element, int[] hashBuffer) {
        /*
         * Adam Kirsch and Michael Mitzenmacher. 2008. Less hashing, same performance: Building a better Bloom filter.
         * Random Struct. Algorithms 33, 2 (September 2008), 187-218. DOI=http://dx.doi.org/10.1002/rsa.v33:2
         */
        final int hashBufferLength = hashBuffer.length;
        final int hash1 = Murmur3_x86_32.hashUnsafeBytes(element, Platform.BYTE_ARRAY_OFFSET, element.length, 0);
        final int hash2 = Murmur3_x86_32.hashUnsafeBytes(element, Platform.BYTE_ARRAY_OFFSET, element.length, hash1);
        for (int index = 0; index < hashBufferLength; index++) {
            int combinedHash = hash1 + ((index + 1) * hash2);
            if (combinedHash < 0) {
                combinedHash = ~combinedHash;
            }
            hashBuffer[index] = combinedHash;
        }
    }

    private boolean containsHashBuffer(BitArray[] bloomFilters, int[] hashBuffer) {
        final int hashBufferLength = hashBuffer.length;
        for (int index = 0; index < hashBufferLength; index++) {
            final int combinedHash = hashBuffer[index];
            final BitArray bloomFilter = bloomFilters[index];
            if (!bloomFilter.get(combinedHash % bloomFilter.bitSize())) {
                return false;
            }
        }
        return true;
    }

    private void setHashBuffer(BitArray[] bloomFilters, int[] hashBuffer, SplittableRandom random) {
        final int hashBufferLength = hashBuffer.length;
        for (int index = 0; index < hashBufferLength; index++) {
            final int combinedHash = hashBuffer[index];
            final BitArray bloomFilter = bloomFilters[index];
            bloomFilter.clear(random.nextLong(bloomFilter.bitSize()));
            bloomFilter.set(combinedHash % bloomFilter.bitSize());
        }
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        final BSBFDeDuplicator that = (BSBFDeDuplicator) other;
        if (numBits != that.numBits) {
            return false;
        }
        if (numHashFunctions != that.numHashFunctions) {
            return false;
        }
        if (!Arrays.equals(bloomFilters, that.bloomFilters)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result;
        result = (int) (numBits ^ (numBits >>> 32));
        result = 31 * result + numHashFunctions;
        result = 31 * result + Arrays.hashCode(bloomFilters);
        return result;
    }

    // http://docs.oracle.com/javase/8/docs/api/java/io/Serializable.html
    private void writeObject(ObjectOutputStream out) throws IOException {
        serializer.writeTo(this, out);
    }

    // http://docs.oracle.com/javase/8/docs/api/java/io/Serializable.html
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        final BSBFDeDuplicator tempDeDuplicator = serializer.readFrom(in);
        this.numBits = tempDeDuplicator.numBits;
        this.numHashFunctions = tempDeDuplicator.numHashFunctions;
        this.bloomFilters = tempDeDuplicator.bloomFilters;
        this.hashBuffer = new int[this.bloomFilters.length];
        this.random = new SplittableRandom(generateRandomSeed(this.numBits, this.numHashFunctions));
    }
}
