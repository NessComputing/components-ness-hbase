package com.nesscomputing.hbase.event;

import java.lang.annotation.Annotation;

import org.apache.hadoop.conf.Configuration;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provider;

/**
 * Provides an HBaseEventWriter pulling config from a given annotation.
 */
public class HBaseEventWriterProvider implements Provider<HBaseEventWriter>
{
    private Injector injector;
    private Annotation bindingAnnotation;

    public HBaseEventWriterProvider(Annotation bindingAnnotation)
    {
        this.bindingAnnotation = bindingAnnotation;
    }

    @Inject
    public void setInjector(Injector injector)
    {
        this.injector = injector;
    }

    @Override
    public HBaseEventWriter get()
    {
        return new HBaseEventWriter(
            injector.getInstance(HBaseEventWriterConfig.class),
            injector.getInstance(Key.get(Configuration.class, bindingAnnotation)),
            injector.getInstance(Key.get(HBaseEventStrategy.class, bindingAnnotation))
        );
    }
}
