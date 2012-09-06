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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
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
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.skife.config.TimeSpan;
import org.weakref.jmx.Managed;

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

    private static final Function<Put, byte []> SPILL_FUNCTION = new BinaryConverter.PutToBinary();

    /** Queue of pending writes.  */
    private final LinkedBlockingQueue<Callable<Put>> writeQueue;

    private AtomicBoolean taskRunning = new AtomicBoolean(true);

    /** Holds a reference to the HBase table that this writer uses. */
    private final AtomicReference<HTable> table = new AtomicReference<HTable>(null);

    private final AtomicLong cooloffTime = new AtomicLong(-1L);

    private final AtomicReference<Thread> writerThread = new AtomicReference<Thread>(null);

    private final String writerName;
    private final HBaseWriterConfig hbaseWriterConfig;
    private final Configuration hadoopConfig;

    private final TimeSpan enqueueTimeout;

    private final AtomicLong opsEnqueued = new AtomicLong(0L);
    private final AtomicLong opsEnqSpilled = new AtomicLong(0L);
    private final AtomicLong opsDeqSpilled = new AtomicLong(0L);
    private final AtomicLong opsEnqTimeout = new AtomicLong(0L);
    private final AtomicLong opsEnqCooloff = new AtomicLong(0L);
    private final AtomicLong opsDequeued = new AtomicLong(0L);
    private final AtomicLong opsSent = new AtomicLong(0L);
    private final AtomicLong opsLost = new AtomicLong(0L);
    private final AtomicLong spillsOk = new AtomicLong(0L);
    private final AtomicLong spillsFailed = new AtomicLong(0L);
    private final AtomicInteger longestBurst = new AtomicInteger(0);

    private final File spillingDirectory;
    private final String spillingId;
    private final AtomicInteger spillingCount = new AtomicInteger(0);

    private int backoff = 1;

    private final String tableName;

    HBaseWriter(@Nonnull final String writerName,
                @Nonnull final HBaseWriterConfig hbaseWriterConfig,
                @Nonnull final Configuration hadoopConfig)
    {
        this.writerName = writerName;
        this.hbaseWriterConfig = hbaseWriterConfig;
        this.hadoopConfig = hadoopConfig;

        this.writeQueue = new LinkedBlockingQueue<Callable<Put>>(hbaseWriterConfig.getQueueLength());
        this.enqueueTimeout = hbaseWriterConfig.getEnqueueTimeout();
        this.tableName = hbaseWriterConfig.getTableName();

        final File spillingDirectory = hbaseWriterConfig.getSpillingDirectory();

        if (hbaseWriterConfig.isSpillingEnabled() && spillingDirectory != null) {

            if (!spillingDirectory.exists() && !spillingDirectory.mkdirs()) {
                LOG.error("Could not create directory '%s', spilling will probably not work!", spillingDirectory);
            }

            if (spillingDirectory.exists()
              && spillingDirectory.isDirectory()
              && spillingDirectory.canWrite()
              && spillingDirectory.canExecute()) {
                this.spillingDirectory = spillingDirectory;
                LOG.info("Spilling enabled for %s, directory is '%s'.", writerName, spillingDirectory.getAbsolutePath());
            }
            else {
                this.spillingDirectory = null;
                LOG.info("Spilling disabled for %s, directory '%s' is not usable!", writerName, spillingDirectory);
            }
        }
        else {
            this.spillingDirectory = null;
            LOG.info("Spilling disabled for %s.", writerName);
        }

        this.spillingId = UUID.randomUUID().toString();
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
                else if (spillingDirectory != null) {
                    final List<Callable<Put>> spilled = new ArrayList<Callable<Put>>(hbaseWriterConfig.getQueueLength() + 1);
                    spilled.add(callable);
                    writeQueue.drainTo(spilled);
                    spill(spilled);
                    opsEnqSpilled.addAndGet(spilled.size());
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
                if (spillingDirectory != null) {
                    spill(putOps);
                    opsDeqSpilled.addAndGet(putOps.size());
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

    private boolean backoff(final Throwable t) throws InterruptedException
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

    private void disconnectHTable()
    {
        final HTable htable = table.getAndSet(null);

        if (htable == null) {
            return;
        }

        LOG.info("Disconnecting from HBase for Table '%s'", tableName);
        try {
            htable.close();
        }
        catch (IOException ioe) {
            LOG.warnDebug(ioe, "While closing HTable");
        }
        LOG.info("Disconnect complete!");
    }

    @VisibleForTesting
    protected HTable connectHTable()
        throws IOException
    {
        HTable hTable = table.get();
        if (hTable == null) {
            LOG.info("Connecting to HBase for Table '%s'", tableName);
            hTable = new HTable(hadoopConfig, tableName);
            hTable.setAutoFlush(true); // We do our own caching so no need to do it twice.
            table.set(hTable);
            LOG.info("Connection complete!");
        }
        return hTable;
    }

    private void spill(final List<Callable<Put>> elements)
    {
        final String fileName = String.format("%s-%s-%05d", writerName, spillingId, spillingCount.getAndIncrement());
        final File spillFile = new File(spillingDirectory, fileName + ".temp");

        OutputStream os = null;

        try {
            os = new FileOutputStream(spillFile);
            BinaryConverter.writeInt(os, 1); // File format version 1
            BinaryConverter.writeInt(os, elements.size()); // Number of elements in this file.
            BinaryConverter.writeLong(os, System.currentTimeMillis()); // Timestamp in nanos

            final List<byte []> data = Lists.transform(elements, Functions.compose(SPILL_FUNCTION, CALLABLE_FUNCTION));
            for (byte [] d : data) {
                os.write(d);
            }

            os.flush();

            Closeables.closeQuietly(os);
            if (!spillFile.renameTo(new File(spillingDirectory, fileName + ".spilled"))) {
                LOG.warn("Could not rename spillfile %s!", spillFile.getAbsolutePath());
            }
            spillsOk.incrementAndGet();
        }
        catch (IOException ioe) {
            LOG.warnDebug(ioe, "Could not spill %d elements to %s.temp, losing them!", elements.size(), fileName);
            opsLost.addAndGet(elements.size());
            if (!spillFile.delete()) {
                LOG.warn("Could not delete bad spillfile %s", spillFile.getAbsolutePath());
            }
            spillsFailed.incrementAndGet();
        }
        finally {
            Closeables.closeQuietly(os);
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

    @Managed(description="number of objects spilled to disk when trying to enqueue.")
    public long getOpsEnqSpilled()
    {
        return opsEnqSpilled.get();
    }

    @Managed(description="number of objects spilled to disk after dequeueing.")
    public long getOpsDeqSpilled()
    {
        return opsDeqSpilled.get();
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

    @Managed(description="number of successful spills to disk.")
    public long getSpillsOk()
    {
        return spillsOk.get();
    }

    @Managed(description="number of failed spills to disk.")
    public long getSpillsFailed()
    {
        return spillsFailed.get();
    }
}
