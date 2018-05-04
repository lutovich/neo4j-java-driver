/*
 * Copyright (c) 2002-2018 Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.driver.internal.logging;

import org.slf4j.LoggerFactory;

import java.net.URL;
import java.security.CodeSource;

import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.Logging;

public class Slf4jLogging implements Logging
{
    public Slf4jLogging()
    {
        CodeSource src = Slf4jLogging.class.getProtectionDomain().getCodeSource();
        if (src != null) {
            System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++++++++ "+src.getLocation());
        }
    }

    @Override
    public Logger getLog( String name )
    {
        return new Slf4jLogger( LoggerFactory.getLogger( name ) );
    }

    private static class Slf4jLogger implements Logger
    {
        final org.slf4j.Logger delegate;

        Slf4jLogger( org.slf4j.Logger delegate )
        {
            this.delegate = delegate;
        }

        @Override
        public void error( String message, Throwable cause )
        {
            delegate.error( message, cause );
        }

        @Override
        public void info( String message, Object... params )
        {
            delegate.info( message, params );
        }

        @Override
        public void warn( String message, Object... params )
        {
            delegate.warn( message, params );
        }

        @Override
        public void warn( String message, Throwable cause )
        {
            delegate.warn( message, cause );
        }

        @Override
        public void debug( String message, Object... params )
        {
            delegate.debug( message, params );
        }

        @Override
        public void trace( String message, Object... params )
        {
            delegate.trace( message, params );
        }

        @Override
        public boolean isTraceEnabled()
        {
            return delegate.isTraceEnabled();
        }

        @Override
        public boolean isDebugEnabled()
        {
            return delegate.isDebugEnabled();
        }
    }
}
