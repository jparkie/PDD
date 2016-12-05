package com.github.jparkie.pdd;

/**
 * A classifier which detects whether a given element is a distinct or a duplicate element.
 */
public interface ProbabilisticDeDuplicator {
    /**
     * The number of bits that the {@link ProbabilisticDeDuplicator} should use.
     *
     * @return The number of bits.
     */
    long numBits();

    /**
     * The number of hash functions that the {@link ProbabilisticDeDuplicator} should use.
     *
     * @return The number of hash functions.
     */
    int numHashFunctions();

    /**
     * Probabilistically classifies whether a given element is a distinct or a duplicate element.
     * This operation does record the result into its history.
     *
     * @param element An element from an unbounded sequence.
     * @return True if the element is a distinct element; otherwise, false if the element is a duplicate element.
     */
    boolean classifyDistinct(byte[] element);

    /**
     * Probabilistically peeks whether a given element is a distinct or a duplicate element.
     * This operation does not record the result into its history.
     *
     * @param element An element from an unbounded sequence.
     * @return True if the element is contained in the {@link ProbabilisticDeDuplicator}; otherwise, false.
     */
    boolean peekDistinct(byte[] element);

    /**
     * Reset the history of the {@link ProbabilisticDeDuplicator}.
     */
    void reset();
}
