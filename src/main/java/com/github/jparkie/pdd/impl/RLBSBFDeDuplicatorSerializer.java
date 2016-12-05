package com.github.jparkie.pdd.impl;

import com.github.jparkie.pdd.BitArray;
import com.github.jparkie.pdd.ProbabilisticDeDuplicatorSerializer;

import java.io.*;

public class RLBSBFDeDuplicatorSerializer implements ProbabilisticDeDuplicatorSerializer<RLBSBFDeDuplicator> {
    @Override
    public byte version() {
        return 1;
    }

    @Override
    public void writeTo(RLBSBFDeDuplicator probabilisticDeDuplicator, OutputStream out) throws IOException {
        final DataOutputStream dos = new DataOutputStream(out);
        dos.writeByte(version());
        dos.writeLong(probabilisticDeDuplicator.numBits);
        dos.writeInt(probabilisticDeDuplicator.numHashFunctions);
        for (BitArray bloomFilter : probabilisticDeDuplicator.bloomFilters) {
            bloomFilter.writeTo(dos);
        }
    }

    @Override
    public RLBSBFDeDuplicator readFrom(InputStream in) throws IOException {
        final DataInputStream dis = new DataInputStream(in);
        final byte serializedVersion = dis.readByte();
        if (serializedVersion != version()) {
            final String error = String.format("Unexpected ProbabilisticDeDuplicator version number (%d), expected %d", serializedVersion, version());
            throw new IllegalArgumentException(error);
        }
        final long numBits = dis.readLong();
        final int numHashFunctions = dis.readInt();
        final BitArray[] bloomFilters = new BitArray[numHashFunctions];
        for (int index = 0; index < numHashFunctions; index++) {
            bloomFilters[index] = BitArray.readFrom(dis);
        }
        return new RLBSBFDeDuplicator(numBits, numHashFunctions, bloomFilters);
    }
}
