package com.github.jparkie.pdd.impl;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BSBFSDDeDuplicatorSerializerTest {
    @Test
    public void testWriteToReadFrom() throws IOException {
        final BSBFSDDeDuplicatorSerializer serializer = new BSBFSDDeDuplicatorSerializer();
        final BSBFSDDeDuplicator deDuplicator = new BSBFSDDeDuplicator(64L, 1);
        final Random random = new Random();
        final byte[] element = new byte[128];
        random.nextBytes(element);
        assertTrue(deDuplicator.classifyDistinct(element));
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        serializer.writeTo(deDuplicator, out);
        out.close();
        final ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        final BSBFSDDeDuplicator serialized = serializer.readFrom(in);
        in.close();
        assertEquals(deDuplicator, serialized);
    }
}
