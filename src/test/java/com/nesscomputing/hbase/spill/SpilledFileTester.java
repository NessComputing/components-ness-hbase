package com.nesscomputing.hbase.spill;

import java.io.File;
import java.util.List;

import org.apache.hadoop.hbase.client.Put;
import org.junit.Assert;
import org.junit.Ignore;

public class SpilledFileTester
{
    @Ignore
    public void testSimple() throws Exception
    {
        final File file = new File("/tmp/log-c37ec4b4-a495-4256-b3b1-f2b25f83821e-00797.spilled");
        final SpilledFile spilledFile = new SpilledFile(file);

        Assert.assertEquals(1, spilledFile.getVersion());
        int count = spilledFile.getElements();

        final List<Put> data = spilledFile.load();
        Assert.assertEquals(count, data.size());
    }
}
