package com.nesscomputing.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.nesscomputing.config.Config;

/**
 * Provides handy bindings for interacting with HBase.
 * @author steven
 */
public class HBaseModule extends AbstractModule {
    @Override
    protected void configure() {
    }

    @Provides
    public Configuration provideHadoopConfiguration(Config trumpetConfig) {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", trumpetConfig.getConfiguration().getString("hbase.zookeeper", "localhost"));
        config.set("zookeeper.znode.parent", trumpetConfig.getConfiguration().getString("hbase.zookeeper.path", "/hbase"));
        return config;
    }
}
