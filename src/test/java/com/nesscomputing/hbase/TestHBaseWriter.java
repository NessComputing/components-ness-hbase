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

import java.util.List;
import java.util.concurrent.Callable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.skife.config.TimeSpan;

import com.google.common.collect.Lists;

public class TestHBaseWriter
{
    private static final HBaseWriterConfig HBASE_WRITER_CONFIG = new HBaseWriterConfig() {
        @Override
        public boolean isEnabled() {
            return true;
        }

        @Override
        public TimeSpan getEnqueueTimeout() {
            return new TimeSpan("10ms");
        }

        @Override
        public String getTableName() {
            return null;
        }
    };

    private static final Callable<Put> CALLABLE = new Callable<Put>() {
        @Override
        public Put call() {
            return new Put();
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
        EasyMock.verify(conf);
    }



    @Test
    public void testCoolOffWithQueueFlush() throws Exception
    {
        final List<Callable<Put>> flushList = Lists.newArrayList();

        final HBaseWriter dummyWriter = new HBaseWriter(HBASE_WRITER_CONFIG, conf) {
            @Override
            protected void flushToHBase(final List<Callable<Put>> dbObjects)
            {
                flushList.addAll(dbObjects);
            }
        };

        int count = 0;

        while (dummyWriter.write(CALLABLE)) {
            count++;
        }

        // Make sure that the queue is full.
        Assert.assertEquals(HBASE_WRITER_CONFIG.getQueueLength(), count);

        Assert.assertEquals(0, flushList.size());

        // Run a single loop of the thread to flush the queue.
        dummyWriter.runLoop();
        Assert.assertEquals(HBASE_WRITER_CONFIG.getQueueLength(), flushList.size());

        // But writing should still fail.
        Assert.assertFalse(dummyWriter.write(CALLABLE));

        Thread.sleep(500L);

        // But writing should still fail.
        Assert.assertFalse(dummyWriter.write(CALLABLE));

        Thread.sleep(1000L);

        // Now it should work again.
        Assert.assertTrue(dummyWriter.write(CALLABLE));
    }

    @Test
    public void testCoolOffWithoutQueueFlush() throws Exception
    {
        final List<Callable<Put>> flushList = Lists.newArrayList();

        final HBaseWriter dummyWriter = new HBaseWriter(HBASE_WRITER_CONFIG, conf) {
            @Override
            protected void flushToHBase(final List<Callable<Put>> dbObjects)
            {
                flushList.addAll(dbObjects);
            }
        };

        int count = 0;

        while (dummyWriter.write(CALLABLE)) {
            count++;
        }

        // Make sure that the queue is full.
        Assert.assertEquals(HBASE_WRITER_CONFIG.getQueueLength(), count);

        // Wait until "past cooloff time".
        Thread.sleep(1100L);

        // Writing should still fail, but it should log only every second..
        for (int i = 0; i < 30; i++) {
            Assert.assertFalse(dummyWriter.write(CALLABLE));
            Thread.sleep(100L);
        }
    }

}
