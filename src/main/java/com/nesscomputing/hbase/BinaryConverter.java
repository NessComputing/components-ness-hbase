package com.nesscomputing.hbase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;

import com.nesscomputing.logging.Log;

final class BinaryConverter
{
    private static final Log LOG = Log.findLog();

    private BinaryConverter()
    {
    }

    static void writeInt(final OutputStream stream, final int value) throws IOException
    {
        final byte [] buffer = new byte [4];
        final ByteBuffer bb = ByteBuffer.wrap(buffer);
        bb.putInt(value);
        stream.write(buffer);
    }

    static void writeLong(final OutputStream stream, final long value) throws IOException
    {
        final byte [] buffer = new byte [8];
        final ByteBuffer bb = ByteBuffer.wrap(buffer);
        bb.putLong(value);
        stream.write(buffer);
    }

    static void writeByteArray(final OutputStream stream, final byte [] bytes) throws IOException
    {
        if (bytes == null) {
            writeInt(stream, 0);
        }
        else {
            writeInt(stream, bytes.length);
            stream.write(bytes);
        }
    }

    static Long readLong(final InputStream stream) throws IOException
    {
        final byte [] buffer = readBuffer(stream, 8);
        if (buffer == null) {
            return null;
        }

        final ByteBuffer bb = ByteBuffer.wrap(buffer);
        return bb.getLong();
    }

    static Integer readInt(final InputStream stream) throws IOException
    {
        final byte [] buffer = readBuffer(stream, 4);
        if (buffer == null) {
            return null;
        }

        final ByteBuffer bb = ByteBuffer.wrap(buffer);
        return bb.getInt();
    }

    static byte [] readBytes(final InputStream stream) throws IOException
    {
        final Integer length = readInt(stream);
        if (length == null) {
            return null;
        }
        return readBuffer(stream, length);
    }

    private static byte [] readBuffer(final InputStream stream, int length) throws IOException
    {
        final byte [] buffer = new byte[length];
        if (stream.read(buffer, 0, buffer.length) != buffer.length) {
            return null;
        }
        return buffer;
    }

    /**
     * Turns a Put into a compact, binary representation of keys and values.
     */
    static final class PutToBinary implements Function<Put, byte []>
    {
        @Override
        public byte [] apply(@Nullable Put input)
        {
            if (input == null) {
                return new byte [0];
            }
            try {
                final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                writeInt(baos, 1); // Version 1 of the format
                writeByteArray(baos, input.getRow()); // Write the row first
                for (List<KeyValue> entry : input.getFamilyMap().values()) {
                    for (KeyValue keyValue : entry) {
                        writeByteArray(baos, keyValue.getBuffer()); // KeyValue as a serialized blob
                    }
                }
                return baos.toByteArray();
            }
            catch (IOException ioe) {
                LOG.error(ioe, "While writing into byte array stream!");
                return new byte [0];
            }
        }
    }

    /**
     * Reads binary representation of a put and reassembles it.
     */
    static final class BinaryToPut implements Function<byte [], Put>
    {
        @Override
        public Put apply(@Nullable byte [] input)
        {
            if (input == null) {
                return null;
            }
            try {
                final ByteArrayInputStream bais = new ByteArrayInputStream(input);
                final Integer version = readInt(bais);
                Preconditions.checkState(version != null && version == 1, "Only version 1 data is supported!");
                final byte [] key = readBytes(bais);
                Preconditions.checkState(key != null, "Could not read key from input stream!");
                final Put put = new Put(key);

                byte [] kvBytes = null;
                while((kvBytes = readBytes(bais)) != null) {
                    put.add(new KeyValue(kvBytes));
                }
                return put;
            }
            catch (IOException ioe) {
                LOG.error(ioe, "While reading from a byte array stream!");
                return null;
            }
        }
    }
}
