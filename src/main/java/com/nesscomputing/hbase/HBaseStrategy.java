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

import javax.annotation.Nonnull;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;

/**
 * Strategy for turning java code into HBase compatible Put / Get operations.
 */
public interface HBaseStrategy<T>
{
    /**
     * Returns a human readable name for the objects that this strategy can process.
     */
    public String getObjectName();

    /**
     * Convert a data object into an HBase Put operation.
     *
     * @throws IllegalArgumentException If the data object could not be converted.
     */
    Put encode(@Nonnull T obj) throws IllegalArgumentException;

    /**
     * Converts the result of an HBase Get operation into a data object.
     */
    T decode(@Nonnull Get row) throws IllegalArgumentException;

    /**
     * Returns the row key for a given object.
     */
    byte [] key(@Nonnull T obj);
}
