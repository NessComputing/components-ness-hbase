package com.nesscomputing.hbase.event;

import java.lang.annotation.Annotation;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.google.inject.name.Names;
import com.nesscomputing.config.ConfigProvider;

/**
 * This module an event writer given an HBaseEventStrategy and an optional annotation.
 */
public class HBaseEventWriterModule extends AbstractModule
{
    private Annotation bindingAnnotation;
    private Class<? extends HBaseEventStrategy> strategy;

    public HBaseEventWriterModule(Class<? extends HBaseEventStrategy> strategy)
    {
        this(Names.named("__default__"), strategy);
    }

    public HBaseEventWriterModule(
        Annotation bindingAnnotation,
        Class<? extends HBaseEventStrategy> strategy
    )
    {
        this.bindingAnnotation = bindingAnnotation;
        this.strategy = strategy;
    }

    @Override
    protected void configure()
    {
        // I don't think this will throw an exception if it this module is installed twice, but it might. :)
        bind(HBaseEventWriterConfig.class).toProvider(ConfigProvider.of(HBaseEventWriterConfig.class)).in(Scopes.SINGLETON);
        bind(HBaseEventStrategy.class).annotatedWith(bindingAnnotation).to(strategy);
        bind(HBaseEventWriter.class).annotatedWith(bindingAnnotation).toProvider(new HBaseEventWriterProvider(bindingAnnotation));
    }
}
