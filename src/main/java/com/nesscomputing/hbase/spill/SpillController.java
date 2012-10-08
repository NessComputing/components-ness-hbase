package com.nesscomputing.hbase.spill;

import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nonnull;

import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;

import org.apache.hadoop.hbase.client.Put;
import org.weakref.jmx.Managed;

import com.nesscomputing.hbase.HBaseWriter;
import com.nesscomputing.hbase.HBaseWriterConfig;
import com.nesscomputing.logging.Log;

public class SpillController
{
    private static final Log LOG = Log.findLog();

    private final AtomicLong opsEnqSpilled = new AtomicLong(0L);
    private final AtomicLong opsDeqSpilled = new AtomicLong(0L);
    private final AtomicLong spillsOk = new AtomicLong(0L);
    private final AtomicLong spillsFailed = new AtomicLong(0L);
    private final AtomicLong spillsLost = new AtomicLong(0L);

    private final AtomicLong spillFilesOk = new AtomicLong(0L);
    private final AtomicLong spillFilesFailed = new AtomicLong(0L);
    private final AtomicLong spilledPutsRead = new AtomicLong(0L);


    private final String spillName;
    private final File spillingSrcDir;
    private final File spillingOkDir;
    private final File spillingFailDir;

    private final String spillingId;
    private final boolean spillingEnabled;

    private final AtomicInteger spillingCount = new AtomicInteger(0);
    private final FileFilter validSpillfileFilter;

    public SpillController(@Nonnull final String spillName,
                           @Nonnull final HBaseWriterConfig hbaseWriterConfig)
    {
        Preconditions.checkNotNull(spillName, "spill name must not be null!");
        Preconditions.checkNotNull(hbaseWriterConfig , "config must not be null!");

        this.spillName = spillName;

        this.validSpillfileFilter = new ValidSpillfileFilter(hbaseWriterConfig.getSpillFileMinAge().getMillis());

        final File baseDir = hbaseWriterConfig.getSpillingDirectory();

        if (baseDir != null                           // must have a base dir
            && hbaseWriterConfig.isSpillingEnabled()  // spilling must be enabled
            && hbaseWriterConfig.isEnabled()) {       // and the writer must be enabled, too.

            spillingSrcDir = createSpillFolder(baseDir, "spill");
            spillingOkDir = createSpillFolder(baseDir, "success");
            spillingFailDir = createSpillFolder(baseDir, "failed");

            this.spillingEnabled = (spillingSrcDir != null) && (spillingOkDir != null) && (spillingFailDir != null);
        }
        else {
            spillingSrcDir = null;
            spillingOkDir = null;
            spillingFailDir = null;

            this.spillingEnabled = false;
        }

        this.spillingId = UUID.randomUUID().toString();

        if (spillingEnabled) {
            LOG.info("Spilling enabled for %s, id is %s, directory is '%s'.", spillName, spillingId, spillingSrcDir.getAbsolutePath());
        }
        else {
            LOG.info("Spilling disabled for %s!", spillName);
        }
    }

    private static File createSpillFolder(final File baseDir, final String folderName)
    {
        if (baseDir == null) {
            return null;
        }

        final File spillingDirectory = new File(baseDir, folderName);

        if (!spillingDirectory.exists() && !spillingDirectory.mkdirs()) {
            LOG.error("Could not create directory '%s', spilling will probably not work!", spillingDirectory);
            return null;
        }

        if (spillingDirectory.exists()
            && spillingDirectory.isDirectory()
            && spillingDirectory.canWrite()
            && spillingDirectory.canExecute()) {
            LOG.debug("Directory is '%s' is usable.", spillingDirectory.getAbsolutePath());
            return spillingDirectory;
        }
        else {
            LOG.warn("Directory '%s' is not usable.", spillingDirectory.getAbsolutePath());
            return null;
        }
    }

    public void spillEnqueueing(final List<Callable<Put>> spillList)
    {
        spill(spillList);
        opsEnqSpilled.addAndGet(spillList.size());
    }

    public void spillDequeueing(final List<Callable<Put>> spillList)
    {
        spill(spillList);
        opsDeqSpilled.addAndGet(spillList.size());
    }

    public boolean isSpillingEnabled()
    {
        return spillingEnabled;
    }

    private void spill(final List<Callable<Put>> elements)
    {
        final String fileName = String.format("%s-%s-%05d", spillName, spillingId, spillingCount.getAndIncrement());
        final File spillFile = new File(spillingSrcDir, fileName + ".temp");

        OutputStream os = null;

        try {
            os = new FileOutputStream(spillFile);
            BinaryConverter.writeInt(os, 1); // File format version 1
            BinaryConverter.writeInt(os, elements.size()); // Number of elements in this file.
            BinaryConverter.writeLong(os, System.currentTimeMillis()); // Timestamp in millis

            final List<byte []> data = Lists.transform(elements, Functions.compose(BinaryConverter.PUT_WRITE_FUNCTION, HBaseWriter.CALLABLE_FUNCTION));
            for (byte [] d : data) {
                os.write(d);
            }

            os.flush();

            Closeables.closeQuietly(os);
            if (!spillFile.renameTo(new File(spillingSrcDir, fileName + ".spilled"))) {
                LOG.warn("Could not rename spillfile %s!", spillFile.getAbsolutePath());
            }
            spillsOk.incrementAndGet();
        }
        catch (IOException ioe) {
            LOG.warnDebug(ioe, "Could not spill %d elements to %s.temp, losing them!", elements.size(), fileName);
            spillsLost.addAndGet(elements.size());
            if (!spillFile.delete()) {
                LOG.warn("Could not delete bad spillfile %s", spillFile.getAbsolutePath());
            }
            spillsFailed.incrementAndGet();
        }
        finally {
            Closeables.closeQuietly(os);
        }
    }

    public List<SpilledFile> findSpilledFiles()
    {
        if (!spillingEnabled) {
            return Collections.emptyList();
        }

        final File [] files = spillingSrcDir.listFiles(validSpillfileFilter);
        final ImmutableList.Builder<SpilledFile> builder = ImmutableList.builder();

        for (File file : files) {
            try {
                builder.add(new SpilledFile(file));
            }
            catch (IOException ioe) {
                LOG.warnDebug(ioe, "While opening spill file '%s'", file.getAbsolutePath());
            }
        }

        return builder.build();
    }

    public void fileOk(final SpilledFile spilledFile)
    {
        final File spillFile = new File(spillingSrcDir, spilledFile.getName());
        final File destFile = new File(spillingOkDir, spilledFile.getName());
        if (!spillFile.renameTo(destFile)) {
            LOG.error("Could not move spill file '%s' to '%s'!", spillFile, destFile);
        }
        spillFilesOk.incrementAndGet();
        spilledPutsRead.addAndGet(spilledFile.getElements());
    }

    public void fileFailed(final SpilledFile spilledFile)
    {
        final File spillFile = new File(spillingSrcDir, spilledFile.getName());
        final File destFile = new File(spillingFailDir, spilledFile.getName());
        if (!spillFile.renameTo(destFile)) {
            LOG.error("Could not move spill file '%s' to '%s'!", spillFile, destFile);
        }
        spillFilesFailed.incrementAndGet();
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

    @Managed(description="number of objects that could not be spilled.")
    public long getSpillsLost()
    {
        return spillsLost.get();
    }

    @Managed(description="number of spill files successfully read.")
    public long getSpillFilesOk()
    {
        return spillFilesOk.get();
    }

    @Managed(description="number of spill files that were unreadable.")
    public long getSpillFilesFailed()
    {
        return spillFilesFailed.get();
    }

    @Managed(description="number of puts read out of spill files.")
    public long getSpilledPutsRead()
    {
        return spilledPutsRead.get();
    }


    public final class ValidSpillfileFilter implements FileFilter
    {
        private final long minAge;

        public ValidSpillfileFilter(final long minAge)
        {
            this.minAge = minAge;
        }

        @Override
        public boolean accept(final File file)
        {
            if (!file.getName().endsWith(".spilled")) {
                return false;
            }

            try {
                final SpilledFile spilledFile = new SpilledFile(file);

                // This spill reader only handled V1 spill files
                if (spilledFile.getVersion() != 1) {
                    return false;
                }
                // Spill file must be at least this --> <-- old.
                if (spilledFile.getTimestamp() + minAge > System.currentTimeMillis()) {
                    return false;
                }

                // Otherwise the file is accepted.
                return true;
            }
            catch (IOException ioe) {
                LOG.warnDebug(ioe, "While reading spill file '%s'", file.getAbsolutePath());
                return false;
            }
        }
    }
}
