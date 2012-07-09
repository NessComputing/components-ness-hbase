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

import org.skife.config.Config;
import org.skife.config.Default;
import org.skife.config.DefaultNull;
import org.skife.config.TimeSpan;

/**
 * Configure an HBase writer for a given object type to an HBase table.
 */
public abstract class HBaseWriterConfig
{
    /**
     * Enable / disable the writer.
     */
    @Config({"ness.hbase.writer.${writername}.enabled","ness.hbase.writer.enabled"})
    @Default("false")
    public boolean isEnabled()
    {
        return false;
    }

    /**
     * Length of the internal queue to buffer bursts from JMS into HBase. A longer queue increases
     * the risk of losing events if the service crashes before they could be stuffed into Hadoop.
     * A shorter queue with higher timeout (see below) will slow the event acceptor and events will pile
     * up in ActiveMQ.
     */
    @Config({"ness.hbase.writer.${writername}.queue-length","ness.hbase.writer.queue-length"})
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
    @Config({"ness.hbase.writer.${writername}.enqueue-timeout","ness.hbase.writer.enqueue-timeout"})
    @DefaultNull
    public TimeSpan getEnqueueTimeout()
    {
        return null;
    }

    /**
     * Cooloff time after failing to enqueue an event.
     */
    @Config({"ness.hbase.writer.${writername}.failure-cooloff-time","ness.hbase.writer.failure-cooloff-time"})
    @Default("1s")
    public TimeSpan getFailureCooloffTime()
    {
        return new TimeSpan("1s");
    }

    /**
     * Name of the HBase table to write into.
     */
    @Config({"ness.hbase.writer.${writername}.tablename","ness.hbase.writer.tablename"})
    public abstract String getTableName();
}
