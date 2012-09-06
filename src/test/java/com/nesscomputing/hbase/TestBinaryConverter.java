package com.nesscomputing.hbase;

import java.util.List;
import java.util.UUID;

import org.apache.commons.io.Charsets;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.junit.Assert;
import org.junit.Test;

public class TestBinaryConverter
{
    @Test
    public void testSimple()
    {
        final Put put = new Put(UUID.randomUUID().toString().getBytes(Charsets.UTF_8));

        put.add("family1".getBytes(Charsets.UTF_8), "qualifier1".getBytes(Charsets.UTF_8), System.currentTimeMillis(), "data1".getBytes(Charsets.UTF_8));
        put.add("family2".getBytes(Charsets.UTF_8), "qualifier2".getBytes(Charsets.UTF_8), System.currentTimeMillis(), "data2".getBytes(Charsets.UTF_8));
        put.add("family3".getBytes(Charsets.UTF_8), "qualifier3".getBytes(Charsets.UTF_8), System.currentTimeMillis(), "data3".getBytes(Charsets.UTF_8));

        final byte [] data = new BinaryConverter.PutToBinary().apply(put);

        final Put put2 = new BinaryConverter.BinaryToPut().apply(data);

        Assert.assertNotNull(put2);
        Assert.assertArrayEquals(put.getRow(), put2.getRow());

        for (final List<KeyValue> entries : put.getFamilyMap().values()) {
            for (final KeyValue kv : entries) {
                Assert.assertTrue(put2.has(kv.getFamily(), kv.getQualifier(), kv.getTimestamp(), kv.getValue()));
            }
        }

        for (final byte [] family : put.getFamilyMap().keySet()) {
            Assert.assertTrue(put2.getFamilyMap().containsKey(family));
        }

        Assert.assertEquals(put.getFamilyMap().size(), put2.getFamilyMap().size());
    }
}
