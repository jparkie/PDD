package com.github.jparkie.pdd;

import com.github.jparkie.pdd.impl.*;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

// TODO: Improve Benchmark Logic.
public class ProbabilisticDeDuplicatorLongBenchmark {
    private static final double STREAM_DISTINCT_PERCENTAGE = 0.60D;
    private static final long STREAM_SIZE = 1000000000L;
    private static final long STREAM_PERCENT = (long) (0.01D * STREAM_SIZE);
    private static final long STREAM_DISTINCT = (long) (STREAM_DISTINCT_PERCENTAGE * STREAM_SIZE);
    private static final long STREAM_DUPLICATE = STREAM_SIZE - STREAM_DISTINCT;
    private static final long NUM_BITS = 128 * 8L * 1024L * 1024L;
    private static final int NUM_HASH_FUNCTIONS = 2;
    private static final int LONG_BYTES = Long.SIZE / Byte.SIZE;

    public static void main(String[] args) {
        System.out.println("ProbabilisticDeDuplicatorLongBenchmark");
        System.out.println(String.format("Stream Distinct Percentage: %f.", STREAM_DISTINCT_PERCENTAGE));
        System.out.println(String.format("Stream Size: %d.", STREAM_SIZE));
        System.out.println(String.format("Stream Distinct Size: %d.", STREAM_DISTINCT));
        System.out.println(String.format("Stream Duplicate Size: %d.", STREAM_DUPLICATE));
        System.out.println(String.format("ProbabilisticDeDuplicator Number of Bits: %d.", NUM_BITS));
        System.out.println(String.format("ProbabilisticDeDuplicator Number of Hash Functions: %d.", NUM_HASH_FUNCTIONS));
        final ProbabilisticDeDuplicator deDuplicator = new RLBSBFDeDuplicator(NUM_BITS, NUM_HASH_FUNCTIONS);
        final byte[] elementBytes = new byte[LONG_BYTES];
        long numFp = 0L;
        long numFn = 0L;
        // Phase 1:
        final long startTime = System.currentTimeMillis();
        System.out.println(String.format("Phase 1 Distinct Fill: %d.", STREAM_DISTINCT));
        for (long counter = 1; counter <= STREAM_DISTINCT; counter++) {
            if (counter % STREAM_PERCENT == 0) {
                System.out.println(String.format("Current Counter: %d/%d", counter, STREAM_DISTINCT));
            }
            fillElementBytes(counter, elementBytes);
            if (!deDuplicator.classifyDistinct(elementBytes)) {
                numFp++;
            }
        }
        // Phase 2:
        System.out.println(String.format("Phase 2 Duplicate Fill: %d.", STREAM_DUPLICATE));
        for (long counter = 1; counter <= STREAM_DUPLICATE; counter++) {
            if (counter % STREAM_PERCENT == 0) {
                System.out.println(String.format("Current Counter: %d/%d", counter, STREAM_DUPLICATE));
            }
            final long duplicateElement = ThreadLocalRandom.current().nextLong(STREAM_DISTINCT);
            fillElementBytes(duplicateElement, elementBytes);
            if (deDuplicator.classifyDistinct(elementBytes)) {
                numFn++;
            }
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
