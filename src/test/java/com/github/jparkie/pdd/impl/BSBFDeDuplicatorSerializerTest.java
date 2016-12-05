package com.github.jparkie.pdd.impl;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BSBFDeDuplicatorSerializerTest {
    @Test
    public void testWriteToReadFrom() throws IOException {
        final BSBFDeDuplicatorSerializer serializer = new BSBFDeDuplicatorSerializer();
        final BSBFDeDuplicator deDuplicator = new BSBFDeDuplicator(64L, 1);
        final Random random = new Random();
        final byte[] element = new byte[128];
        random.nextBytes(element);
        assertTrue(deDuplicator.classifyDistinct(element));
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        serializer.writeTo(deDuplicator, out);
        out.close();
        final ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        final BSBFDeDuplicator serialized = serializer.readFrom(in);
        in.close();
        assertEquals(deDuplicator, serialized);
    }
}
