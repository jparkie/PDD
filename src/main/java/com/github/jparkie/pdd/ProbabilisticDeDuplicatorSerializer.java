package com.github.jparkie.pdd;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * A Serializer for a {@link ProbabilisticDeDuplicator}.
 *
 * @param <T> A subtype of {@link ProbabilisticDeDuplicator}.
 */
public interface ProbabilisticDeDuplicatorSerializer<T extends ProbabilisticDeDuplicator> {
    /**
     * The binary format version this serializer will write and read.
     *
     * @return The binary format version.
     */
    int version();

    /**
     * Writes out this {@link ProbabilisticDeDuplicator} to an output stream in binary format.
     * It is the caller's responsibility to close the stream.
     *
     * @param probabilisticDeDuplicator The {@link ProbabilisticDeDuplicator} to write.
     * @param out The output stream to write the {@link ProbabilisticDeDuplicator}.
     * @throws IOException Thrown if the write fails.
     */
    void writeTo(T probabilisticDeDuplicator, OutputStream out) throws IOException;

    /**
     * Reads in a {@link ProbabilisticDeDuplicator} from an input stream.
     * It is the caller's responsibility to close the stream.
     *
     * @param in The input stream to read the {@link ProbabilisticDeDuplicator}.
     * @return The persisted {@link ProbabilisticDeDuplicator}.
     * @throws IOException Thrown if the read fails.
     */
    T readFrom(InputStream in) throws IOException;
}