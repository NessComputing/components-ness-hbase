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
package com.nesscomputing.hbase.spill;

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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

final class BinaryConverter
{
    private static final Log LOG = Log.findLog();

    public static final Function<Put, byte []> PUT_WRITE_FUNCTION = new PutToBinary();
    public static final Function<InputStream, Put> PUT_READ_FUNCTION = new StreamToPut();

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
        @SuppressFBWarnings("NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE")
        public byte [] apply(Put input)
        {
            Preconditions.checkNotNull(input, "input must not be null!");

            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                writeByteArray(baos, input.getRow()); // Write the row first
                for (List<KeyValue> entry : input.getFamilyMap().values()) {
                    for (KeyValue keyValue : entry) {
                        writeByteArray(baos, keyValue.getBuffer()); // KeyValue as a serialized blob
                    }
                }

                final byte [] putBytes = baos.toByteArray();

                baos = new ByteArrayOutputStream();
                writeInt(baos, 2); // Version 2 of the format
                // Version 2 has a size field to tell the reader how many bytes
                // will constitute the put to follow.
                writeByteArray(baos, putBytes);
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
    static final class StreamToPut implements Function<InputStream, Put>
    {
        @Override
        public Put apply(@Nullable InputStream stream)
        {
            if (stream == null || ! stream.markSupported()) {
                return null;
            }
            try {
                final Integer version = readInt(stream);
                if (version == null) {
                    return null;
                }

                if (version == 1) {
                    // Version 1 had no total size field, so the reader
                    // must guess whether a field is part of the current put
                    // or starts a new put. The good news is that it generally
                    // guesses right.

                    final byte [] key = readBytes(stream);
                    Preconditions.checkState(key != null, "Could not read key from input stream!");
                    final Put put = new Put(key);

                    for (;;) {
                        // Hack to find the end of an element and start with the next.
                        stream.mark(4);
                        final Integer length = readInt(stream);
                        // There are no fields in the stream with length == 1, so
                        // this guess is usually right
                        if (length == null || length == 1) {
                            stream.reset();
                            return put;
                        }
                        final byte [] kvBytes = readBuffer(stream, length);
                        put.add(new KeyValue(kvBytes));
                    }
                }
                else if (version == 2) {
                    // Version 2 has a size field which makes the readers' life easy
                    final byte [] putBytes = readBytes(stream);
                    final ByteArrayInputStream bais = new ByteArrayInputStream(putBytes);

                    final byte [] key = readBytes(bais);
                    Preconditions.checkState(key != null, "Could not read key from input stream!");
                    final Put put = new Put(key);
                    byte [] kvBytes;
                    while ((kvBytes = BinaryConverter.readBytes(bais)) != null) {
                        put.add(new KeyValue(kvBytes));
                    }
                    return put;
                }
                else {
                    LOG.warn("Encountered a put v%d which is unknown!", version);
                    return null;
                }
            }
            catch (IOException ioe) {
                LOG.error(ioe, "While reading put from input stream!");
                return null;
            }
        }
    }
}
