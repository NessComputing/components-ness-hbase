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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.skife.config.TimeSpan;
import org.weakref.jmx.Managed;

import com.nesscomputing.hbase.spill.SpillController;
import com.nesscomputing.logging.Log;

/**
 * Batch writer to HBase.  Enqueues either as its buffer fills or periodically, to bound the amount of lost information.
 *
 * May only enqueue events while the writer is running.
 */
public class HBaseWriter extends AbstractHBaseSupport implements Runnable
{
    private static final Log LOG = Log.findLog();

    public static final Function<Callable<Put>, Put> CALLABLE_FUNCTION = new Function<Callable<Put>, Put>() {
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

    private final AtomicReference<Thread> writerThread = new AtomicReference<Thread>(null);

    private final TimeSpan enqueueTimeout;
    private final AtomicLong cooloffTime = new AtomicLong(-1L);

    private final AtomicLong opsEnqueued = new AtomicLong(0L);
    private final AtomicLong opsEnqTimeout = new AtomicLong(0L);
    private final AtomicLong opsEnqCooloff = new AtomicLong(0L);
    private final AtomicLong opsDequeued = new AtomicLong(0L);
    private final AtomicLong opsSent = new AtomicLong(0L);
    private final AtomicLong opsLost = new AtomicLong(0L);
    private final AtomicInteger longestBurst = new AtomicInteger(0);

    private volatile int backoff = 1;

    private final HBaseWriterConfig hbaseWriterConfig;
    private final SpillController spillController;

    HBaseWriter(@Nonnull final HBaseWriterConfig hbaseWriterConfig,
                @Nonnull final Configuration hadoopConfig,
                @Nonnull final SpillController spillController)
    {
        super(hbaseWriterConfig, hadoopConfig);

        Preconditions.checkNotNull(spillController, "spill controller not be null!");

        this.hbaseWriterConfig = hbaseWriterConfig;

        this.writeQueue = new LinkedBlockingQueue<Callable<Put>>(hbaseWriterConfig.getQueueLength());
        this.enqueueTimeout = hbaseWriterConfig.getEnqueueTimeout();

        this.spillController = spillController;
    }

    synchronized void start()
    {
        if (hbaseWriterConfig.isEnabled()) {
            Preconditions.checkState(writerThread.get() == null, "already started, boldly refusing to start twice!");
            Preconditions.checkState(table.get() == null, "Already have a htable object, something went very wrong!");

            LOG.info("Starting HBase Writer for HBase table %s.", tableName);

            final Thread thread = new Thread(this, String.format("hbase-%s-writer", tableName));
            writerThread.set(thread);
            thread.start();
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

            disconnectHTable();
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
                // If a spiller is configured, spill the queue and go on with life.
                else if (spillController.isSpillingEnabled()) {
                    final List<Callable<Put>> spilled = new ArrayList<Callable<Put>>(hbaseWriterConfig.getQueueLength() + 1);
                    spilled.add(callable);
                    writeQueue.drainTo(spilled);
                    spillController.spillEnqueueing(spilled);
                    this.cooloffTime.set(-1L);
                    return true;
                }
                else {
                    opsEnqTimeout.incrementAndGet();
                }
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
        throws InterruptedException
    {
        LOG.trace("Starting write of %d put ops...", putOps.size());

        for (;;) {
            try {
                final HTable htable = connectHTable();
                htable.put(Lists.transform(putOps, CALLABLE_FUNCTION));
                opsSent.addAndGet(putOps.size());
                LOG.trace("Wrote %d put ops to HBase table %s.", putOps.size(), tableName);
                backoff = 1;
                return;
            }
            catch (IOException e) {
                if (spillController.isSpillingEnabled()) {
                    spillController.spillDequeueing(putOps);
                    backoff(e);
                    return;
                }
                else {
                    if (backoff(e)) {
                        break;
                    }
                }
            }
        }
        opsLost.addAndGet(putOps.size());
    }

    protected boolean backoff(final Throwable t) throws InterruptedException
    {
        final long backoffTime = hbaseWriterConfig.getBackoffDelay().getMillis() * backoff;
        LOG.warnDebug(t, "Could not send data to HBase, sleeping for %d ms...", backoffTime);

        Thread.sleep(backoffTime);
        final boolean maxBackoff = (backoff == (1 << hbaseWriterConfig.getMaxBackoffFactor()));
        if (!maxBackoff) {
            backoff <<= 1;
        }

        disconnectHTable();
        return maxBackoff;
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
        catch (Exception e) {
            LOG.warnDebug(e, "Caught exception before killing the writer thread!");
        }
        LOG.info("Exiting");
    }

    @VisibleForTesting
    void runLoop()
        throws InterruptedException
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

    @Managed(description="number of objects enqueued.")
    public long getOpsEnqueued()
    {
        return opsEnqueued.get();
    }

    @Managed(description="number of objects lost because enqueueing failed with timeout.")
    public long getOpsEnqTimeout()
    {
        return opsEnqTimeout.get();
    }

    @Managed(description="number of objects lost because writer is in cooloff mode.")
    public long getOpsEnqCooloff()
    {
        return opsEnqCooloff.get();
    }

    @Managed(description="number of objects dequeued.")
    public long getOpsDequeued()
    {
        return opsDequeued.get();
    }

    @Managed(description="number of objects that could neither sent nor spilled.")
    public long getOpsLost()
    {
        return opsLost.get();
    }

    @Managed(description="number of objects successfully sent.")
    public long getOpsSent()
    {
        return opsSent.get();
    }

    @Managed(description="current length of the internal queue.")
    public int getQueueLength()
    {
        return writeQueue.size();
    }

    @Managed(description="longest queue burst dequeued in a single operation.")
    public int getLongestBurst()
    {
        return longestBurst.get();
    }
}
