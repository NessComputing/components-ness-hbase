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
package com.nesscomputing.hbase;

import java.io.File;
import java.io.IOException;

import com.google.common.io.Files;

import org.apache.commons.io.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.skife.config.TimeSpan;

import com.nesscomputing.testing.lessio.AllowExternalProcess;
import com.nesscomputing.testing.lessio.AllowLocalFileAccess;

@AllowExternalProcess()
@AllowLocalFileAccess(paths={"%TMP_DIR%"})
public class TestHBaseSpill
{
    private final File spillDir = Files.createTempDir();
    private final int queueLength = 10;

    private final HBaseWriterConfig hbaseWriterConfig = new HBaseWriterConfig() {
        @Override
        public boolean isEnabled() {
            return true;
        }

        public int getQueueLength()
        {
            return queueLength;
        }

        @Override
        public TimeSpan getEnqueueTimeout() {
            return new TimeSpan("1ms");
        }

        @Override
        public String getTableName() {
            return null;
        }

        public File getSpillingDirectory()
        {
            return spillDir;
        }
    };

    private Configuration conf = null;

    @Before
    public void setUp()
    {
        conf = EasyMock.createNiceMock(Configuration.class);
        EasyMock.replay(conf);
    }

    @After
    public void tearDown()
    {
        if (spillDir != null && spillDir.exists()) {
            File [] children = spillDir.listFiles();
            for (File child : children) {
                child.delete();
            }
        }
        spillDir.delete();

        EasyMock.verify(conf);
    }

    @Test
    public void testSpillOnEnqueue()
    {
        final HBaseWriter dummyWriter = new HBaseWriter("test", hbaseWriterConfig, conf);

        final Put data = new Put("row".getBytes(Charsets.UTF_8));
        data.add("family".getBytes(Charsets.UTF_8), "qualifier".getBytes(Charsets.UTF_8), "Hello, World".getBytes(Charsets.UTF_8));


        for (int i = 0 ; i < queueLength; i++) {
            dummyWriter.write(data);
            Assert.assertEquals(0L, dummyWriter.getSpillsOk());
            Assert.assertEquals(0L, dummyWriter.getSpillsFailed());
            Assert.assertEquals(i + 1, dummyWriter.getQueueLength());
        }
        dummyWriter.write(data);
        Assert.assertEquals(1L, dummyWriter.getSpillsOk());
        Assert.assertEquals(queueLength + 1, dummyWriter.getOpsEnqSpilled());

    }

    @Test
    public void testSpillOnDequeue() throws Exception
    {
        final HBaseWriter dummyWriter = new HBaseWriter("test", hbaseWriterConfig, conf) {
            @Override
            protected HTable connectHTable() throws IOException
            {
                throw new IOException("oops");
            }

        };

        final Put data = new Put("row".getBytes(Charsets.UTF_8));
        data.add("family".getBytes(Charsets.UTF_8), "qualifier".getBytes(Charsets.UTF_8), "Hello, World".getBytes(Charsets.UTF_8));


        for (int i = 0 ; i < queueLength; i++) {
            dummyWriter.write(data);
            Assert.assertEquals(0L, dummyWriter.getSpillsOk());
            Assert.assertEquals(0L, dummyWriter.getSpillsFailed());
            Assert.assertEquals(i + 1, dummyWriter.getQueueLength());
        }

        Assert.assertEquals(0, dummyWriter.getOpsDeqSpilled());

        dummyWriter.runLoop();

        Assert.assertEquals(queueLength, dummyWriter.getOpsDeqSpilled());
        Assert.assertEquals(1L, dummyWriter.getSpillsOk());
    }



}
