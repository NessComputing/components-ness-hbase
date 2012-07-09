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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.skife.config.TimeSpan;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.nesscomputing.logging.Log;


/**
 * Batch writer to HBase.  Enqueues either as its buffer fills or periodically, to bound the amount of lost information.
 *
 * May only enqueue events while the writer is running.
 */
public class HBaseWriter implements Runnable
{
    private static final Log LOG = Log.findLog();

    /** Queue of pending writes.  */
    private final LinkedBlockingQueue<Put> writeQueue;

    private AtomicBoolean taskRunning = new AtomicBoolean(true);

    /** Holds a reference to the HBase table that this writer uses. */
    private final AtomicReference<HTable> table = new AtomicReference<HTable>(null);

    private final AtomicLong cooloffTime = new AtomicLong(-1L);

    private final AtomicReference<Thread> writerThread = new AtomicReference<Thread>(null);

    private final HBaseWriterConfig hbaseWriterConfig;
    private final Configuration hadoopConfig;

    private final TimeSpan enqueueTimeout;

    HBaseWriter(final HBaseWriterConfig hbaseWriterConfig,
                final Configuration hadoopConfig)
    {
        this.hbaseWriterConfig = hbaseWriterConfig;
        this.hadoopConfig = hadoopConfig;

        this.writeQueue = new LinkedBlockingQueue<Put>(hbaseWriterConfig.getQueueLength());
        this.enqueueTimeout = hbaseWriterConfig.getEnqueueTimeout();
    }

    synchronized void start()
    {
        if (hbaseWriterConfig.isEnabled()) {
            try {
            Preconditions.checkState(writerThread.get() == null, "already started, boldly refusing to start twice!");
            Preconditions.checkState(table.get() == null, "Already have a htable object, something went very wrong!");

            LOG.info("Starting HBase Writer for HBase table %s.", hbaseWriterConfig.getTableName());

            final HTable hTable = new HTable(hadoopConfig, hbaseWriterConfig.getTableName());

            hTable.setAutoFlush(true); // We do our own caching so no need to do it twice.
            table.set(hTable);

            final Thread thread = new Thread(this, String.format("hbase-%s-writer", hbaseWriterConfig.getTableName()));
            writerThread.set(thread);
            thread.start();
            }
            catch (IOException ioe) {
                LOG.warnDebug(ioe, "Could not start HBase writer for %s", hbaseWriterConfig.getTableName());
            }
        }
    }

    synchronized void stop()
    {
        final Thread thread = writerThread.getAndSet(null);
        if (thread != null) {
            LOG.info("Stopping HBase Writer for table %s.", hbaseWriterConfig.getTableName());
            try {
                taskRunning.set(false);
                thread.interrupt();
                thread.join(500L);
            }
            catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }

            final HTable htable = table.getAndSet(null);
            closeQuietly(htable);
        } else {
            LOG.debug("Never started, ignoring stop()");
        }
    }

    public void write(final Put putOp)
    {
        if (!hbaseWriterConfig.isEnabled()) {
            return ;
        }

        Preconditions.checkState(taskRunning.get(), "Attempt to enqueue while the writer is shut down!");

        if (putOp == null) {
            return;
        }

        LOG.trace("Receiving put op: '%s'", putOp);

        final long cooloffTime = this.cooloffTime.get();

        if (cooloffTime > 0 && System.nanoTime() < cooloffTime) {
            LOG.trace("Cooling off from enqueue failure");
            return;
        }

        LOG.trace("Enqueue put op '%s'", putOp);

        try {
            if (enqueueTimeout == null) {
                writeQueue.put(putOp);
                this.cooloffTime.set(-1L);
                return;
            }

            if (writeQueue.offer(putOp, enqueueTimeout.getPeriod(), enqueueTimeout.getUnit())) {
                this.cooloffTime.set(-1L);
                return;
            }
        }
        catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }

        LOG.warn("Could not offer put op '%s' to queue", putOp);
        this.cooloffTime.compareAndSet(-1L, System.nanoTime() + hbaseWriterConfig.getFailureCooloffTime().getMillis() * 1000000L);
    }

    private void flushToHBase(final List<Put> putOps)
    {
        LOG.trace("Starting write of %d put ops...", putOps.size());

        try {
            final HTable htable = table.get();
            if (htable != null) {
                htable.put(putOps);
                LOG.trace("Wrote %d put ops to HBase table %s.", putOps.size(), hbaseWriterConfig.getTableName());
            } else {
                LOG.warn("HTable is null, probably shutting down!");
            }
        }
        catch (IOException e) {
            LOG.error(e, "Unable to send put ops to HBase!");
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
        LOG.info("HBaseWriter for %s starting...", hbaseWriterConfig.getTableName());
        try {
            while (taskRunning.get()) {
                // TODO: this does not batch as I'd like.  The code to do it correctly
                // is somewhat tricky and ugly, but this is where performance problems
                // will arise.  Maybe.
                final Put put = writeQueue.poll(100, TimeUnit.MILLISECONDS);
                if (put != null) {
                    final List<Put> puts = Lists.newArrayList();
                    puts.add(put);
                    writeQueue.drainTo(puts);
                    flushToHBase(puts);
                }
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        LOG.info("Exiting");
    }
}
