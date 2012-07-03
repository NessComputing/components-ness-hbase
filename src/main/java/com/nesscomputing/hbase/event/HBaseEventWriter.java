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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.skife.config.TimeSpan;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.nesscomputing.event.NessEvent;
import com.nesscomputing.event.NessEventReceiver;
import com.nesscomputing.lifecycle.LifecycleStage;
import com.nesscomputing.lifecycle.guice.OnStage;
import com.nesscomputing.logging.Log;


/**
 * Batch writer to HBase.  Enqueues either as its buffer fills or periodically, to bound the amount of lost information.
 * May only enqueue events while the writer is running. Spawns a background thread, so you likely have to bind this as
 * an eager singleton.
 *
 * This takes a HBaseEventStrategy to do the actual translation from NessEvent to HBase Put.
 *
 * @author steven + nik
 */
@Singleton
public class HBaseEventWriter implements NessEventReceiver, Runnable
{
    private static final Log LOG = Log.findLog();

    /** Queue of events pending writes.  */
    private final LinkedBlockingQueue<NessEvent> eventQueue;

    private volatile boolean taskRunning = true;

    /** Holds a reference to the HBase table that we write into. */
    private final AtomicReference<HTable> table = new AtomicReference<HTable>(null);

    private final AtomicLong cooloffTime = new AtomicLong(-1L);

    private volatile Thread writerThread = null;

    private final HBaseEventWriterConfig hbaseEventWriterConfig;
    private final Configuration hadoopConfig;
    private HBaseEventStrategy eventStrategy;

    private final TimeSpan enqueueTimeout;

    @Inject
    HBaseEventWriter(
        final HBaseEventWriterConfig hbaseEventWriterConfig,
        final Configuration hadoopConfig,
        final HBaseEventStrategy eventStrategy
    )
    {
        this.hbaseEventWriterConfig = hbaseEventWriterConfig;
        this.hadoopConfig = hadoopConfig;
        this.eventStrategy = eventStrategy;

        this.eventQueue = new LinkedBlockingQueue<NessEvent>(hbaseEventWriterConfig.getQueueLength());
        this.enqueueTimeout = hbaseEventWriterConfig.getEnqueueTimeout();
    }

    @OnStage(LifecycleStage.START)
    synchronized void start() throws IOException
    {
        Preconditions.checkState(writerThread == null, "already started, boldly refusing to start twice!");
        Preconditions.checkState(table.get() == null, "Already have a htable object, something went very wrong!");

        LOG.info("Starting HBase Event Writer");

        final HTable hTable = new HTable(hadoopConfig, eventStrategy.getTableName());
        hTable.setAutoFlush(true); // We do our own caching so no need to do it twice.
        table.set(hTable);

        writerThread = new Thread(this, String.format("hbase-%s-writer", eventStrategy.getTableName()));
        writerThread.start();
    }

    @OnStage(LifecycleStage.STOP)
    synchronized void stop() throws InterruptedException
    {
        if (writerThread != null) {
            LOG.info("Stopping HBase Event Writer");
            try {
                taskRunning = false;
                writerThread.interrupt();
                writerThread.join(500L);
            }
            catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            finally {
                writerThread = null;
            }

            final HTable htable = table.getAndSet(null);
            closeQuietly(htable);
        } else {
            LOG.debug("Never started, ignoring stop()");
        }
    }

    @Override
    public boolean accept(final NessEvent event)
    {
        return eventStrategy.acceptEvent(event);
    }

    @Override
    public void receive(final NessEvent event)
    {
        Preconditions.checkState(taskRunning, "Attempt to enqueue while the writer is shut down!");

        if (event == null) {
            return;
        }

        LOG.trace("Receiving Event: %s", event);

        final long cooloffTime = this.cooloffTime.get();

        if (cooloffTime > 0 && System.nanoTime() < cooloffTime) {
            LOG.trace("Cooling off from enqueue failure");
            return;
        }

        LOG.trace("Enqueue event %s", event);

        try {
            if (enqueueTimeout == null) {
                eventQueue.put(event);
                this.cooloffTime.set(-1L);
                return;
            }

            if (eventQueue.offer(event, enqueueTimeout.getPeriod(), enqueueTimeout.getUnit())) {
                this.cooloffTime.set(-1L);
                return;
            }
        }
        catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }

        LOG.warn("Could not offer message '%s' to queue", event);
        this.cooloffTime
            .compareAndSet(
                -1L,
                System.nanoTime() + hbaseEventWriterConfig.getFailureCooloffTime().getMillis() * 1000000L
            );
    }

    private void write(final List<NessEvent> events)
    {
        LOG.trace("Starting write of %d events...", events.size());

        final List<Put> putList = new ArrayList<Put>(events.size());

        for (final NessEvent event : events) {
            try {
                putList.add(eventStrategy.encodeEvent(event));
            }
            catch (IllegalArgumentException e) {
                LOG.error(e, "Malformed event could not be encoded: %s", event);
            }
        }

        try {
            final HTable htable = table.get();
            if (htable != null) {
                htable.put(putList);
                LOG.trace("Wrote %d events to HBase.", putList.size());
            } else {
                LOG.warn("HTable is null, probably shutting down!");
            }
        }
        catch (IOException e) {
            LOG.error(e, "Unable to put new events into HBase!");
        }

    }

    private void closeQuietly(final HTable htable)
    {
        if (htable == null) {
            return;
        }

        try {
            htable.close();
        }
        catch (IOException ioe) {
            LOG.warnDebug(ioe, "While closing HTable");
        }
    }

    @Override
    public void run()
    {
        LOG.info("NessEvent writer starting...");
        try {
            while (taskRunning) {
                // TODO: this does not batch as I'd like.  The code to do it correctly
                // is somewhat tricky and ugly, but this is where performance problems
                // will arise.  Maybe.
                final NessEvent e = eventQueue.poll(100, TimeUnit.MILLISECONDS);
                if (e == null) {
                    continue;
                }
                final List<NessEvent> events = Lists.newArrayList(e);
                eventQueue.drainTo(events);
                write(events);
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        LOG.info("Exiting");
    }
}
