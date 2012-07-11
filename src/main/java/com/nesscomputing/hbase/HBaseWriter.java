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
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.skife.config.TimeSpan;
import org.weakref.jmx.Managed;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
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

    private static final Function<Callable<Put>, Put> CALLABLE_FUNCTION = new Function<Callable<Put>, Put>() {
        @Override
        public Put apply(@Nullable final Callable<Put> callable) {
            try {
                return callable == null ? null : callable.call();
            }
            catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }
    };

    /** Queue of pending writes.  */
    private final LinkedBlockingQueue<Callable<Put>> writeQueue;

    private AtomicBoolean taskRunning = new AtomicBoolean(true);

    /** Holds a reference to the HBase table that this writer uses. */
    private final AtomicReference<HTable> table = new AtomicReference<HTable>(null);

    private final AtomicLong cooloffTime = new AtomicLong(-1L);

    private final AtomicReference<Thread> writerThread = new AtomicReference<Thread>(null);

    private final HBaseWriterConfig hbaseWriterConfig;
    private final Configuration hadoopConfig;

    private final TimeSpan enqueueTimeout;

    private final AtomicLong opsEnqueued = new AtomicLong(0L);
    private final AtomicLong opsEnqTimeout = new AtomicLong(0L);
    private final AtomicLong opsEnqCooloff = new AtomicLong(0L);
    private final AtomicLong opsDequeued = new AtomicLong(0L);
    private final AtomicLong opsSent = new AtomicLong(0L);
    private final AtomicLong opsLost = new AtomicLong(0L);
    private final AtomicInteger longestBurst = new AtomicInteger(0);

    private final String tableName;

    HBaseWriter(final HBaseWriterConfig hbaseWriterConfig,
                final Configuration hadoopConfig)
    {
        this.hbaseWriterConfig = hbaseWriterConfig;
        this.hadoopConfig = hadoopConfig;

        this.writeQueue = new LinkedBlockingQueue<Callable<Put>>(hbaseWriterConfig.getQueueLength());
        this.enqueueTimeout = hbaseWriterConfig.getEnqueueTimeout();
        this.tableName = hbaseWriterConfig.getTableName();
    }

    synchronized void start()
    {
        if (hbaseWriterConfig.isEnabled()) {
            try {
            Preconditions.checkState(writerThread.get() == null, "already started, boldly refusing to start twice!");
            Preconditions.checkState(table.get() == null, "Already have a htable object, something went very wrong!");

            LOG.info("Starting HBase Writer for HBase table %s.", tableName);

            final HTable hTable = new HTable(hadoopConfig, tableName);

            hTable.setAutoFlush(true); // We do our own caching so no need to do it twice.
            table.set(hTable);

            final Thread thread = new Thread(this, String.format("hbase-%s-writer", tableName));
            writerThread.set(thread);
            thread.start();
            }
            catch (IOException ioe) {
                LOG.warnDebug(ioe, "Could not start HBase writer for %s", tableName);
            }
        }
    }

    synchronized void stop()
    {
        final Thread thread = writerThread.getAndSet(null);
        if (thread != null) {
            LOG.info("Stopping HBase Writer for table %s.", tableName);
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

    public boolean write(final Put putOp)
    {
        return write(new Callable<Put>() {
            @Override
            public Put call() {
                return putOp;
            }
        });
    }

    public boolean write(final Callable<Put> callable)
    {
        if (!hbaseWriterConfig.isEnabled()) {
            return false;
        }

        Preconditions.checkState(taskRunning.get(), "Attempt to enqueue while the writer is shut down!");

        if (callable == null) {
            return false;
        }

        final long cooloffTime = this.cooloffTime.get();

        if (cooloffTime > 0) {
            if (System.nanoTime() < cooloffTime) {
                opsEnqCooloff.incrementAndGet();
                LOG.trace("Cooling off from enqueue failure");
                return false;
            }
            else {
                this.cooloffTime.set(-1L);
            }
        }

        try {
            if (enqueueTimeout == null) {
                writeQueue.put(callable);
                opsEnqueued.incrementAndGet();
                this.cooloffTime.set(-1L);
                return true;
            }
            else {
                if (writeQueue.offer(callable, enqueueTimeout.getPeriod(), enqueueTimeout.getUnit())) {
                    opsEnqueued.incrementAndGet();
                    this.cooloffTime.set(-1L);
                    return true;
                }
                opsEnqTimeout.incrementAndGet();
            }
        }
        catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }

        LOG.warn("Could not offer put op to queue, sleeping for %s!", hbaseWriterConfig.getFailureCooloffTime());
        this.cooloffTime.compareAndSet(-1L, System.nanoTime() + hbaseWriterConfig.getFailureCooloffTime().getMillis() * 1000000L);

        return false;
    }

    protected void flushToHBase(final List<Callable<Put>> putOps)
    {
        LOG.trace("Starting write of %d put ops...", putOps.size());

        try {
            final HTable htable = table.get();
            if (htable != null) {
                htable.put(Lists.transform(putOps, CALLABLE_FUNCTION));
                opsSent.addAndGet(putOps.size());
                LOG.trace("Wrote %d put ops to HBase table %s.", putOps.size(), tableName);
            } else {
                LOG.warn("HTable is null, probably shutting down!");
            }
        }
        catch (IOException e) {
            opsLost.addAndGet(putOps.size());
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
        final TimeSpan tickerTime = hbaseWriterConfig.getTickerTime();
        LOG.info("HBaseWriter for %s starting (ticker: %s)...", tableName, tickerTime);

        try {
            while (taskRunning.get()) {
                runLoop();
                Thread.sleep(tickerTime.getMillis());
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        LOG.info("Exiting");
    }

    @VisibleForTesting
    void runLoop()
    {
        final Callable<Put> put = writeQueue.poll();
        if (put != null) {
            final List<Callable<Put>> puts = Lists.newArrayList();
            puts.add(put);
            writeQueue.drainTo(puts);

            final int size = puts.size();
            opsDequeued.addAndGet(size);
            if (size > longestBurst.get()) {
                longestBurst.set(size);
            }

            flushToHBase(puts);
        }
    }

    @Managed
    public long getOpsEnqueued()
    {
        return opsEnqueued.get();
    }

    @Managed
    public long getOpsEnqTimeout()
    {
        return opsEnqTimeout.get();
    }

    @Managed
    public long getOpsEnqCooloff()
    {
        return opsEnqCooloff.get();
    }

    @Managed
    public long getOpsDequeued()
    {
        return opsDequeued.get();
    }

    @Managed
    public long getOpsSent()
    {
        return opsSent.get();
    }

    @Managed
    public long getOpsLost()
    {
        return opsLost.get();
    }

    @Managed
    public int getQueueLength()
    {
        return writeQueue.size();
    }

    @Managed
    public int getLongestBurst()
    {
        return longestBurst.get();
    }
}
