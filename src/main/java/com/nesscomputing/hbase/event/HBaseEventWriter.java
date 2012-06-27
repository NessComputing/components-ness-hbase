package com.nesscomputing.hbase.event;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.codehaus.jackson.map.ObjectMapper;
import org.skife.config.TimeSpan;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
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
 * Batch writer to HBase.  Enqueues either as its buffer fills or periodically, to bound
 * the amount of lost information.  May only enqueue events while the writer is running.
 * Spawns a background thread, so you likely have to bind this as an eager singleton.
 * @author steven
 */
@Singleton
public class HBaseEventWriter  implements NessEventReceiver, Runnable
{
    private static final Log LOG = Log.findLog();

    // "Immutable".  Change this at runtime and you win a Darwin award.
    private static final byte[] EVENT_COLUMN_FAMILY = "ev".getBytes(Charsets.UTF_8);

    /** Queue of events pending writes.  */
    private final LinkedBlockingQueue<NessEvent> eventQueue;

    private volatile boolean taskRunning = true;

    /** Holds a reference to the HBase table that we write into. */
    private final AtomicReference<HTable> table = new AtomicReference<HTable>(null);

    private final AtomicLong cooloffTime = new AtomicLong(-1L);

    private volatile Thread writerThread = null;

    private final HBaseEventWriterConfig hbaseEventWriterConfig;
    private final Configuration hadoopConfig;
    private final ObjectMapper objectMapper;

    private final TimeSpan enqueueTimeout;

    @Inject
    HBaseEventWriter(final HBaseEventWriterConfig hbaseEventWriterConfig,
                     final Configuration hadoopConfig,
                     final ObjectMapper objectMapper
                     ) throws IOException
    {
        this.hbaseEventWriterConfig = hbaseEventWriterConfig;
        this.hadoopConfig = hadoopConfig;
        this.objectMapper = objectMapper;

        this.eventQueue =  new LinkedBlockingQueue<NessEvent>(hbaseEventWriterConfig.getQueueLength());
        this.enqueueTimeout = hbaseEventWriterConfig.getEnqueueTimeout();
    }

    @OnStage(LifecycleStage.START)
    synchronized void start() throws IOException
    {
        Preconditions.checkState(writerThread == null, "already started, boldly refusing to start twice!");
        Preconditions.checkState(table.get() == null, "Already have a htable object, something went very wrong!");

        final HTable hTable = new HTable(hadoopConfig, "events");
        hTable.setAutoFlush(true); // We do our own caching so no need to do it twice.
        table.set(hTable);

        writerThread = new Thread(this, "hbase-writer");
        writerThread.start();
    }

    @OnStage(LifecycleStage.STOP)
    synchronized void stop() throws InterruptedException
    {
        if (writerThread != null) {

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
        }
        else {
            LOG.debug("Never started, ignoring stop()");
        }
    }

    @Override
    public boolean accept(final NessEvent event)
    {
        return event != null;
    }

    @Override
    public void receive(final NessEvent event)
    {
        Preconditions.checkState(taskRunning, "Attempt to enqueue while the writer is shut down!");

        if (event == null) {
            return;
        }

        final long cooloffTime = this.cooloffTime.get();

        if (cooloffTime > 0  && System.nanoTime() < cooloffTime) {
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
        this.cooloffTime.compareAndSet(-1L, System.nanoTime() + hbaseEventWriterConfig.getFailureCooloffTime().getMillis() * 1000000L);
    }

    private void write(final List<NessEvent> events)
    {
        LOG.trace("Starting write of %d events...", events.size());

        final List<Put> putList = new ArrayList<Put>(events.size());

        for (final NessEvent event : events) {
            try {
                putList.add(encodeNessEvent(event));
            } catch (IllegalArgumentException e) {
                LOG.error(e, "Malformed event could not be encoded: %s", event);
            }
        }

        try {
            final HTable htable = table.get();
            if (htable != null) {
                htable.put(putList);
                LOG.trace("Wrote %d events to HBase.", putList.size());
            }
            else {
                LOG.warn("HTable is null, probably shutting down!");
            }
        } catch (IOException e) {
            LOG.error(e, "Unable to put new events into HBase!");
        }

    }

    @VisibleForTesting
    Put encodeNessEvent(final NessEvent event)
    {
        final Put put = new Put(HBaseEncoder.rowKeyForEvent(event));
        // Try to keep the v1 format as much alive as possible
        addKey(put, "entryTimestamp", HBaseEncoder.bytesForObject(event.getTimestamp()));
        addKey(put, "eventType", HBaseEncoder.bytesForString(event.getType().getName()));

        final UUID userId = event.getUser();

        if (userId != null) {
            addKey(put, "user", HBaseEncoder.bytesForObject(userId));
        }

        addKey(put, "uuid", HBaseEncoder.bytesForObject(event.getId()));
        addKey(put, "v", HBaseEncoder.bytesForObject(event.getVersion()));

        for (Entry<String, ? extends Object> e : event.getPayload().entrySet()) {
            try {
                final String value = stringify(e.getValue());
                LOG.trace("%s --> %s", e.getKey(), value);
                addKey(put, e.getKey(), HBaseEncoder.bytesForString(value));
            }
            catch (IOException ioe) {
                LOG.warn(ioe, "Could not serialize '%s'", e.getValue());
            }
        }

        return put;
    }

    private String stringify(final Object value)
        throws IOException
    {
        if (value == null) {
            return null;
        }
        else if (value instanceof Map || value instanceof List || value.getClass().isArray()) {
            return objectMapper.writeValueAsString(value);
        }
        else {
            return value.toString();
        }
    }

    private void addKey(final Put put, final String key,  final byte[] value)
    {
        put.add(EVENT_COLUMN_FAMILY, key.getBytes(Charsets.UTF_8), value);
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
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        LOG.info("Exiting");
    }
}
