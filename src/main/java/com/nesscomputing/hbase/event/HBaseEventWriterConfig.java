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

import org.skife.config.Config;
import org.skife.config.Default;
import org.skife.config.DefaultNull;
import org.skife.config.TimeSpan;

public class HBaseEventWriterConfig
{
    /**
     * Length of the internal queue to buffer bursts from JMS into HBase. A longer queue increases
     * the risk of losing events if the service crashes before they could be stuffed into Hadoop.
     * A shorter queue with higher timeout (see below) will slow the event acceptor and events will pile
     * up in ActiveMQ.
     */
    @Config("ness.event.hbase.queue-length")
    @Default("1000")
    public int getQueueLength()
    {
        return 1000;
    }

    /**
     * Maximum amount of time that the listener tries to
     * stuff an event into HBase. Default is to wait until
     * there is space in the queue.
     */
    @Config("ness.event.hbase.enqueue-timeout")
    @DefaultNull
    public TimeSpan getEnqueueTimeout()
    {
        return null;
    }

    /**
     * Cooloff time after failing to enqueue an event.
     */
    @Config("ness.event.hbase.failure-cooloff-time")
    @Default("1s")
    public TimeSpan getFailureCooloffTime()
    {
        return new TimeSpan("1s");
    }
}
