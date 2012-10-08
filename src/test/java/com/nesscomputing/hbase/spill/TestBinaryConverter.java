/**
 * Copyright (C) 2012 Ness Computing, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nesscomputing.hbase.spill;

import java.io.ByteArrayInputStream;
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

        final Put put2 = new BinaryConverter.StreamToPut().apply(new ByteArrayInputStream(data));

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
