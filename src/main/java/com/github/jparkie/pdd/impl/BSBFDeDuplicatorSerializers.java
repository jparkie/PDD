package com.github.jparkie.pdd.impl;

import com.github.jparkie.pdd.BitArray;
import com.github.jparkie.pdd.ProbabilisticDeDuplicatorSerializer;

import java.io.*;

public enum BSBFDeDuplicatorSerializers implements ProbabilisticDeDuplicatorSerializer<BSBFDeDuplicator> {
    VERSION_1(1) {
        @Override
        public void writeTo(BSBFDeDuplicator probabilisticDeDuplicator, OutputStream out) throws IOException {
            /* Commented for reference; TODO: Remove the next version.
            final DataOutputStream dos = new DataOutputStream(out);
            dos.writeByte(version());
            dos.writeLong(probabilisticDeDuplicator.numBits);
            dos.writeInt(probabilisticDeDuplicator.numHashFunctions);
            for (BitArray bloomFilter : probabilisticDeDuplicator.bloomFilters) {
                bloomFilter.writeTo(dos);
            }
            */
            final String error =
                    "A BSBFDeDuplicatorSerializers can no longer be serialized by the VERSION_1 scheme.";
            throw new UnsupportedOperationException(error);
        }

        @Override
        public BSBFDeDuplicator readFrom(InputStream in) throws IOException {
            final DataInputStream dis = new DataInputStream(in);
            final byte serializedVersion = dis.readByte();
            if (serializedVersion != version()) {
                final String error = String.format(
                        "Unexpected ProbabilisticDeDuplicator version number (%d), expected %d",
                        serializedVersion,
                        version()
                );
                throw new IOException(error);
            }
            final long numBits = dis.readLong();
            final int numHashFunctions = dis.readInt();
            final BitArray[] bloomFilters = new BitArray[numHashFunctions];
            for (int index = 0; index < numHashFunctions; index++) {
                bloomFilters[index] = BitArray.readFrom(dis);
            }
            return new BSBFDeDuplicator(numBits, numHashFunctions, bloomFilters, 0D);
        }
    },
    VERSION_2(2) {
        @Override
        public void writeTo(BSBFDeDuplicator probabilisticDeDuplicator, OutputStream out) throws IOException {
            final DataOutputStream dos = new DataOutputStream(out);
            dos.writeInt(version());
            dos.writeLong(probabilisticDeDuplicator.numBits);
            dos.writeInt(probabilisticDeDuplicator.numHashFunctions);
            for (BitArray bloomFilter : probabilisticDeDuplicator.bloomFilters) {
                bloomFilter.writeTo(dos);
            }
            dos.writeDouble(probabilisticDeDuplicator.reportedDuplicateProbability);
        }

        @Override
        public BSBFDeDuplicator readFrom(InputStream in) throws IOException {
            final DataInputStream dis = new DataInputStream(in);
            final int serializedVersion = dis.readInt();
            if (serializedVersion != version()) {
                final String error = String.format(
                        "Unexpected ProbabilisticDeDuplicator version number (%d), expected %d",
                        serializedVersion,
                        version()
                );
                throw new IOException(error);
            }
            final long numBits = dis.readLong();
            final int numHashFunctions = dis.readInt();
            final BitArray[] bloomFilters = new BitArray[numHashFunctions];
            for (int index = 0; index < numHashFunctions; index++) {
                bloomFilters[index] = BitArray.readFrom(dis);
            }
            final double reportedDuplicateProbability = dis.readDouble();
            return new BSBFDeDuplicator(numBits, numHashFunctions, bloomFilters, reportedDuplicateProbability);
        }
    };

    private final int version;

    BSBFDeDuplicatorSerializers(int version) {
        this.version = version;
    }

    @Override
    public int version() {
        return version;
    }
}
