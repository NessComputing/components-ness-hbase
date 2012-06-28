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
package com.nesscomputing.hbase.event;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.UUID;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.nesscomputing.event.NessEvent;

/**
 * Convert to/from HBase representations.
 * @author steven
 */
public final class HBaseEncoder {
    private HBaseEncoder() {}

    public static byte[] rowKeyForEvent(NessEvent e) {
        ByteBuffer converter = ByteBuffer.allocate(24);
        LongBuffer longConverter = converter.asLongBuffer();
        // Throw IAE instead of NPE so that we are uniform in what exceptions we throw
        Preconditions.checkArgument(e != null, "null event");
        UUID id = e.getId();

        Preconditions.checkArgument(id != null, "null id");
        Preconditions.checkArgument(e.getTimestamp() != null, "null timestamp");
        longConverter.put(0, e.getTimestamp().getMillis());
        longConverter.put(1, id.getMostSignificantBits());
        longConverter.put(2, id.getLeastSignificantBits());
        return converter.array();
    }

    /** Encode {@link String} as <code>byte[]</code>.  Just uses UTF8 encoding. */
    public static byte[] bytesForString(String string) {
        if (string == null) {
            return new byte[0];
        }
        return string.getBytes(Charsets.UTF_8);
    }

    /** Decode {@link String} from a <code>byte[]</code>.  Assumes UTF8. */
    public static String stringFromBytes(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        return new String(bytes, Charsets.UTF_8);
    }

    /** Encode an {@link Object} into a <code>byte[]</code> */
    public static byte[] bytesForObject(Object value) {
        if (value == null) {
            return new byte[0];
        }
        return bytesForString(value.toString());
    }
}
