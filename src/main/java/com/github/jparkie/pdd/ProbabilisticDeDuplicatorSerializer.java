package com.github.jparkie.pdd;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * A Serializer for a ProbabilisticDeDuplicator.
 *
 * @param <T> A subtype of ProbabilisticDeDuplicator.
 */
public interface ProbabilisticDeDuplicatorSerializer<T extends ProbabilisticDeDuplicator> {
    /**
     * The binary format version this serializer will write and read.
     */
    byte version();

    /**
     * Writes out this {@link ProbabilisticDeDuplicator} to an output stream in binary format. It is the caller's
     * responsibility to close the stream.
     */
    void writeTo(T probabilisticDeDuplicator, OutputStream out) throws IOException;

    /**
     * Reads in a {@link ProbabilisticDeDuplicator} from an input stream. It is the caller's responsibility to close
     * the stream.
     */
    T readFrom(InputStream in) throws IOException;
}