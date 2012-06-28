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
package com.nesscomputing.hbase.event;

import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.nesscomputing.event.NessEvent;
import com.nesscomputing.event.NessEventType;

public class TestHBaseEventWriter
{
    private HBaseEventWriter eventWriter = null;

    @Before
    public void setUp() throws Exception
    {
        Assert.assertNull(eventWriter);
        eventWriter = new HBaseEventWriter(new HBaseEventWriterConfig() {},
                                           new Configuration(), new ObjectMapper());
    }

    @After
    public void tearDown()
    {
        Assert.assertNotNull(eventWriter);
        eventWriter = null;
    }

    @Test
    public void testAcceptNullUser()
    {
        final NessEvent event = NessEvent.createEvent(null, NessEventType.getForName(null));
        final Put put = eventWriter.encodeNessEvent(event);

        Assert.assertNotNull(put);
    }

    @Test
    public void testAcceptComplexPayload()
    {
        final NessEvent event = NessEvent.createEvent(null, NessEventType.getForName(null), ImmutableMap.<String, Object>builder()
                                                                                                      .put("a-string", "string")
                                                                                                      .put("a-number", 200)
                                                                                                      .put("a-boolean", Boolean.TRUE)
                                                                                                      .put("a-long", 4815162342L)
                                                                                                      .put("a-map", ImmutableMap.of("foo", "bar"))
                                                                                                      .put("a-list", ImmutableList.of(1, 2, 3, 4, "hello", "world"))
                                                                                                      .build());
        final Put put = eventWriter.encodeNessEvent(event);

        Assert.assertNotNull(put);
    }

    @Test
    public void testNullPayload()
    {
        final NessEvent event = NessEvent.createEvent(null, NessEventType.getForName(null), Collections.singletonMap("a-null-value", null));
        final Put put = eventWriter.encodeNessEvent(event);

        Assert.assertNotNull(put);
    }

}
