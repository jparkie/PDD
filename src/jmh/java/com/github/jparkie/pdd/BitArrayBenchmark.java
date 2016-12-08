package com.github.jparkie.pdd;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

/**
 * Benchmark                          Mode  Cnt          Score         Error  Units
 * BitArrayBenchmark.benchmarkClear  thrpt  200   22563935.037 ±  417953.613  ops/s
 * BitArrayBenchmark.benchmarkGet    thrpt  200  282401627.693 ± 1870596.504  ops/s
 * BitArrayBenchmark.benchmarkSet    thrpt  200   23075666.015 ±   58029.165  ops/s
 */
public class BitArrayBenchmark {
    private static final long BIT_ARRAY_LENGTH = 64L;
    private static final long BIT_ARRAY_INDEX = 1L;

    @State(Scope.Benchmark)
    public static class GetState {
        private BitArray bitArray = new BitArray(BIT_ARRAY_LENGTH);
    }

    @State(Scope.Benchmark)
    public static class SetState {
        private BitArray bitArray = new BitArray(BIT_ARRAY_LENGTH);

        @Setup(Level.Invocation)
        public void doSetup() {
            bitArray.clear(BIT_ARRAY_INDEX);
        }
    }

    @State(Scope.Benchmark)
    public static class ClearState {
        private BitArray bitArray = new BitArray(BIT_ARRAY_LENGTH);

        @Setup(Level.Invocation)
        public void doSetup() {
            bitArray.set(BIT_ARRAY_INDEX);
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void benchmarkGet(GetState getState, Blackhole blackhole) {
        blackhole.consume(getState.bitArray.get(BIT_ARRAY_INDEX));
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void benchmarkSet(SetState setState, Blackhole blackhole) {
        blackhole.consume(setState.bitArray.set(BIT_ARRAY_INDEX));
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void benchmarkClear(ClearState clearState, Blackhole blackhole) {
        blackhole.consume(clearState.bitArray.clear(BIT_ARRAY_INDEX));
    }
}
