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

import io.netty.util.internal.logging.AbstractInternalLogger;

import org.neo4j.driver.v1.Logger;

import static java.lang.String.format;


public class NettyLogger extends AbstractInternalLogger
{
    private Logger log;

    public NettyLogger( String name, Logger log )
    {
        super( name );
        this.log = log;
    }

    @Override
    public boolean isTraceEnabled()
    {
        return log.isTraceEnabled();
    }

    @Override
    public void trace( String msg )
    {
        log.trace( msg );
    }

    @Override
    public void trace( String format, Object arg )
    {
        log.trace( format, arg );
    }

    @Override
    public void trace( String format, Object argA, Object argB )
    {
        log.trace( format, argA, argB );
    }

    @Override
    public void trace( String format, Object... arguments )
    {
        log.trace( format, arguments );
    }

    @Override
    public void trace( String msg, Throwable t )
    {
        log.trace( "{}{}%n{}", msg, t );
    }

    @Override
    public boolean isDebugEnabled()
    {
        return log.isDebugEnabled();
    }

    @Override
    public void debug( String msg )
    {
        log.debug( msg );
    }

    @Override
    public void debug( String format, Object arg )
    {
        log.debug( format, arg );
    }

    @Override
    public void debug( String format, Object argA, Object argB )
    {
        log.debug( format, argA, argB );
    }

    @Override
    public void debug( String format, Object... arguments )
    {
        log.debug( format, arguments );
    }

    @Override
    public void debug( String msg, Throwable t )
    {
        log.debug( "{}%n{}", msg, t );
    }

    @Override
    public boolean isInfoEnabled()
    {
        return true;
    }

    @Override
    public void info( String msg )
    {
        log.info( msg );
    }

    @Override
    public void info( String format, Object arg )
    {
        log.info( format, arg );
    }

    @Override
    public void info( String format, Object argA, Object argB )
    {
        log.info( format, argA, argB );
    }

    @Override
    public void info( String format, Object... arguments )
    {
        log.info( format, arguments );
    }

    @Override
    public void info( String msg, Throwable t )
    {
        log.info( "{}%n{}", msg, t );
    }

    @Override
    public boolean isWarnEnabled()
    {
        return true;
    }

    @Override
    public void warn( String msg )
    {
        log.warn( msg );
    }

    @Override
    public void warn( String format, Object arg )
    {
        log.warn( format, arg );
    }

    @Override
    public void warn( String format, Object... arguments )
    {
        log.warn( format, arguments );
    }

    @Override
    public void warn( String format, Object argA, Object argB )
    {
        log.warn( format, argA, argB );
    }

    @Override
    public void warn( String msg, Throwable t )
    {
        log.warn( "{}%n{}", msg, t );
    }

    @Override
    public boolean isErrorEnabled()
    {
        return true;
    }

    @Override
    public void error( String msg )
    {
        log.error( msg, null );
    }

    @Override
    public void error( String format, Object arg )
    {
        error( format, new Object[]{arg} );
    }

    @Override
    public void error( String format, Object argA, Object argB )
    {
        error( format, new Object[]{argA, argB} );
    }

    @Override
    public void error( String format, Object... arguments )
    {
        if ( arguments.length == 0 )
        {
            log.error( format, null );
            return;
        }

        Object arg = arguments[arguments.length - 1];
        if ( arg instanceof Throwable )
        {
            // still give all arguments to string format,
            // for the worst case, the redundant parameter will be ignored.
            log.error( format( format, arguments ), (Throwable) arg );
        }
    }

    @Override
    public void error( String msg, Throwable t )
    {
        log.error( msg, t );
    }
}
