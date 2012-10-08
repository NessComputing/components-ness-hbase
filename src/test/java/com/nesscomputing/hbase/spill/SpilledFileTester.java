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
