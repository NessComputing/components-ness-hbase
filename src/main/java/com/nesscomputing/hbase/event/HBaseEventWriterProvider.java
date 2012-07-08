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
        HBaseEventStrategy strategy = null;
        Configuration config = null;
        if (bindingAnnotation != null) {
            config = injector.getInstance(Key.get(Configuration.class, bindingAnnotation));
            strategy = injector.getInstance(Key.get(HBaseEventStrategy.class, bindingAnnotation));
        } else {
            config = injector.getInstance(Configuration.class);
            strategy = injector.getInstance(HBaseEventStrategy.class);
        }

        return new HBaseEventWriter(
            injector.getInstance(HBaseEventWriterConfig.class),
            config,
            strategy
        );
    }
}
