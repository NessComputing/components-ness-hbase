package com.nesscomputing.hbase;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;

import com.nesscomputing.logging.Log;

public abstract class AbstractHBaseSupport
{
    private static final Log LOG = Log.findLog();

    /** Holds a reference to the HBase table that this writer uses. */
    protected final AtomicReference<HTable> table = new AtomicReference<HTable>(null);

    protected final String tableName;
    protected final Configuration hadoopConfig;


    protected AbstractHBaseSupport(@Nonnull final HBaseWriterConfig hbaseWriterConfig,
                                   @Nonnull final Configuration hadoopConfig)
    {
        Preconditions.checkNotNull(hbaseWriterConfig, "writer config must not be null!");
        Preconditions.checkNotNull(hadoopConfig, "hadoop config must not be null!");

        this.hadoopConfig = hadoopConfig;

        this.tableName = hbaseWriterConfig.getTableName();
    }

    protected void disconnectHTable()
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
}

