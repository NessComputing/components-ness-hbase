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

import com.google.common.base.Charsets;

/**
 * Convert to/from HBase representations.
 * @author steven
 */
public final class HBaseEncoder {
    private HBaseEncoder() {}

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
