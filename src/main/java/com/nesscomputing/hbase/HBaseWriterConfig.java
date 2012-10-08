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
     * Time that the writer thread sleeps (and accumulates new events).
     */
    @Config({"ness.hbase.writer.${writername}.ticker-time","ness.hbase.writer.ticker-time"})
    @Default("100ms")
    public TimeSpan getTickerTime()
    {
        return new TimeSpan("100ms");
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
     * If a problem occurs, the backoff delay calculated with this delay.
     */
    @Config({"ness.hbase.writer.${writername}.backoff-delay", "ness.hbase.writer.backoff-delay"})
    @Default("3s")
    public TimeSpan getBackoffDelay()
    {
        return new TimeSpan("3s");
    }

    /**
     * maximum shift factor (2^1 .. 2^x) for the exponential backoff.
     */
    @Config({"ness.hbase.writer.${writername}.max-backoff-factor", "ness.hbase.writer.max-backoff-factor"})
    @Default("6")
    public int getMaxBackoffFactor()
    {
        return 6;
    }

    /**
     * Enable / disable spilling to disk.
     */
    @Config({"ness.hbase.writer.${writername}.spilling.enabled","ness.hbase.writer.spilling.enabled"})
    @Default("true")
    public boolean isSpillingEnabled()
    {
        return true;
    }

    /**
     * place to spill to.
     */
    @Config({"ness.hbase.writer.${writername}.spilling.directory","ness.hbase.writer.spilling.directory"})
    @DefaultNull
    public File getSpillingDirectory()
    {
        return null;
    }

    /**
     * Cycle time for the spill reader.
     */
    @Config({"ness.hbase.writer.${writername}.spill-reader-cycle","ness.hbase.writer.spill-reader-cycle"})
    @Default("2h")
    public TimeSpan getSpillCycleTime()
    {
        return new TimeSpan("2h");
    }

    /**
     * Minimum age for a spill file to be considered for re-reading.
     */
    @Config({"ness.hbase.writer.${writername}.spill-file-min-age","ness.hbase.writer.spill-file-min-age"})
    @Default("8h")
    public TimeSpan getSpillFileMinAge()
    {
        return new TimeSpan("8h");
    }

    /**
     * Name of the HBase table to write into.
     */
    @Config({"ness.hbase.writer.${writername}.tablename","ness.hbase.writer.tablename"})
    public abstract String getTableName();
}
