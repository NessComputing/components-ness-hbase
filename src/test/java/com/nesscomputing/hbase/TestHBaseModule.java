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

import org.junit.Assert;
import org.junit.Test;

import com.google.inject.CreationException;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Stage;
import com.nesscomputing.config.Config;
import com.nesscomputing.config.ConfigModule;

public class TestHBaseModule
{
    @Test(expected=CreationException.class)
    public void testDieUnconfigured()
    {
        Guice.createInjector(Stage.PRODUCTION,
                             ConfigModule.forTesting(),
                             new HBaseModule());
    }

    @Test
    public void testBasicConfig()
    {
        final Config config = Config.getFixedConfig("ness.hbase.zookeeper.quorum", "host1,host2,host3",
                                                    "ness.hbase.zookeeper.path", "/foobar");
        final Injector inj = Guice.createInjector(Stage.PRODUCTION,
                                                  new ConfigModule(config),
                                                  new HBaseModule());

        final HBaseConfig hbc = inj.getInstance(HBaseConfig.class);
        Assert.assertEquals("host1,host2,host3", hbc.getHBaseZookeeperQuorum());
        Assert.assertEquals("/foobar", hbc.getHBaseZookeeperPath());

    }
}
