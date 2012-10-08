package com.nesscomputing.hbase.spill;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closeables;

import org.apache.hadoop.hbase.client.Put;

import com.nesscomputing.logging.Log;

public class SpilledFile
{
    private static final Log LOG = Log.findLog();

    private final File file;

    private final int version;
    private final int elements;
    private final long timestamp;

    SpilledFile(@Nonnull final File file) throws IOException
    {
        Preconditions.checkNotNull(file, "the file must not be null!");

        this.file = file;

        InputStream is = null;

        try {
            is = new FileInputStream(file);
            version = BinaryConverter.readInt(is);
            elements = BinaryConverter.readInt(is);
            timestamp = BinaryConverter.readLong(is);

            LOG.trace("Opened spill file %s : v%d, count: %d, age: %d", file.getAbsolutePath(), version, elements, timestamp);
        }
        finally {
            Closeables.closeQuietly(is);
        }
    }

    public String getName()
    {
        return file.getName();
    }

    public int getVersion()
    {
        return version;
    }

    public int getElements()
    {
        return elements;
    }

    public long getTimestamp()
    {
        return timestamp;
    }

    public List<Put> load() throws IOException
    {
        InputStream is = null;

        final ImmutableList.Builder<Put> builder = ImmutableList.builder();

        try {
            is = new BufferedInputStream(new FileInputStream(file));
            is.skip(4 + 4 + 8); // int, int, long

            Put put = null;

            while ((put = BinaryConverter.PUT_READ_FUNCTION.apply(is)) != null) {
                builder.add(put);
            }

            return builder.build();
        }
        finally {
            Closeables.closeQuietly(is);
        }
    }
}
