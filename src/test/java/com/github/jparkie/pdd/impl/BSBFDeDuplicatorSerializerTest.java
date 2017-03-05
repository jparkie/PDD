package com.github.jparkie.pdd.impl;

import com.github.jparkie.pdd.ProbabilisticDeDuplicatorSerializer;
import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BSBFDeDuplicatorSerializerTest {
    // TODO: Consider how to write regression tests for version comparability.
    @Ignore
    @Test
    public void testWriteToReadFromVersion1() throws IOException {
        final ProbabilisticDeDuplicatorSerializer<BSBFDeDuplicator> serializer =
                BSBFDeDuplicatorSerializers.VERSION_1;
        final BSBFDeDuplicator deDuplicator = new BSBFDeDuplicator(64L, 1);
        final ByteBuffer byteBuffer = ByteBuffer.allocate(64);
        byteBuffer.putLong(1L);
        assertTrue(deDuplicator.classifyDistinct(byteBuffer.array()));
        byteBuffer.clear();
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        serializer.writeTo(deDuplicator, out);
        out.close();
        final ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        final BSBFDeDuplicator serialized = serializer.readFrom(in);
        in.close();
        assertEquals(deDuplicator, serialized);
    }

    @Test
    public void testWriteToReadFromVersion2() throws IOException {
        final ProbabilisticDeDuplicatorSerializer<BSBFDeDuplicator> serializer =
                BSBFDeDuplicatorSerializers.VERSION_2;
        final BSBFDeDuplicator deDuplicator = new BSBFDeDuplicator(64L, 1);
        final ByteBuffer byteBuffer = ByteBuffer.allocate(64);
        byteBuffer.putLong(1L);
        assertTrue(deDuplicator.classifyDistinct(byteBuffer.array()));
        byteBuffer.clear();
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        serializer.writeTo(deDuplicator, out);
        out.close();
        final ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        final BSBFDeDuplicator serialized = serializer.readFrom(in);
        in.close();
        assertEquals(deDuplicator, serialized);
    }
}
