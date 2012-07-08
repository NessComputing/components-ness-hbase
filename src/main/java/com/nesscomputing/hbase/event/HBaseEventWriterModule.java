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

import java.lang.annotation.Annotation;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
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
        this(null, strategy);
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

        if (bindingAnnotation != null) {
            bind(HBaseEventStrategy.class).annotatedWith(bindingAnnotation).to(strategy);
            bind(HBaseEventWriter.class).annotatedWith(bindingAnnotation).toProvider(new HBaseEventWriterProvider(bindingAnnotation));
        } else {
            bind(HBaseEventStrategy.class).to(strategy);
            bind(HBaseEventWriter.class).toProvider(new HBaseEventWriterProvider(bindingAnnotation));
        }
    }
}
