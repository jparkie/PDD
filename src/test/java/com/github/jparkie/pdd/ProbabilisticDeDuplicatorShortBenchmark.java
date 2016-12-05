package com.github.jparkie.pdd;

import com.github.jparkie.pdd.impl.*;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

// TODO: Improve Benchmark Logic.
public class ProbabilisticDeDuplicatorShortBenchmark {
    private static final long STREAM_SIZE = 1000000L;
    private static final long NUM_BITS = 8 * 8L * 1024L * 1024L;
    private static final int NUM_HASH_FUNCTIONS = 2;
    private static final int LONG_BYTES = Long.SIZE / Byte.SIZE;

    public static void main(String[] args) {
        System.out.println("ProbabilisticDeDuplicatorShortBenchmark");
        System.out.println(String.format("Stream Size: %d.", STREAM_SIZE));
        System.out.println(String.format("ProbabilisticDeDuplicator Number of Bits: %d.", NUM_BITS));
        System.out.println(String.format("ProbabilisticDeDuplicator Number of Hash Functions: %d.", NUM_HASH_FUNCTIONS));
        final ProbabilisticDeDuplicator deDuplicator = new RLBSBFDeDuplicator(NUM_BITS, NUM_HASH_FUNCTIONS);
        final BitArray universeBitArray = new BitArray(STREAM_SIZE);
        final byte[] elementBytes = new byte[LONG_BYTES];
        long numFp = 0L;
        long numFn = 0L;
        final long startTime = System.currentTimeMillis();
        for (long counter = 1; counter <= STREAM_SIZE; counter++) {
            final long randomNumber = ThreadLocalRandom.current().nextLong(universeBitArray.bitSize());
            fillElementBytes(randomNumber, elementBytes);
            if (deDuplicator.classifyDistinct(elementBytes)) {
                if (universeBitArray.get(randomNumber)) {
                    numFn++;
                }
            } else {
                if (!universeBitArray.get(randomNumber)) {
                    numFp++;
                }
            }
            universeBitArray.set(randomNumber);
        }
        System.out.println(String.format("FP Count: %d.", numFp));
        System.out.println(String.format("FN Count: %d.", numFn));
        final double actualFpp = ((double) numFp) / ((double) STREAM_SIZE);
        System.out.println(String.format("Fpp: %f.", actualFpp));
        final double actualFnp = ((double) numFn) / ((double) STREAM_SIZE);
        System.out.println(String.format("Fnp: %f.", actualFnp));
        final long endTime = System.currentTimeMillis();
        final long durationMs = endTime - startTime;
        System.out.println(String.format("Duration (ms): %d.", durationMs));
        final long durationSeconds = TimeUnit.MILLISECONDS.toSeconds(durationMs);
        System.out.println(String.format("Duration (s): %d.", durationSeconds));
    }

    private static void fillElementBytes(long value, byte[] byteArray) {
        for (int index = 7; index >= 0; index--) {
            byteArray[index] = (byte) (value & 0xffL);
            value >>= 8;
        }
    }
}