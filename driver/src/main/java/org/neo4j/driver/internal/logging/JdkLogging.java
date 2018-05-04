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

import java.text.MessageFormat;
import java.util.logging.Level;

import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.Logging;

public class JdkLogging implements Logging
{
    private final Level level;

    public JdkLogging( Level level )
    {
        this.level = level;
    }

    @Override
    public Logger getLog( String name )
    {
        java.util.logging.Logger logger = java.util.logging.Logger.getLogger( name );
        logger.setLevel( level );
        return new JdkLogger( logger );
    }

    /**
     * JUL uses different message format than other logging frameworks, like Log4j. String message template in JUL is a template as in {@link MessageFormat},
     * example "Let's count! One, {0}, three, {1}, four, {2} ... done!". Templates in majority of logging frameworks tdo not have parameter index in curlies
     * so the previous message would be "Let's count! One, {}, three, {}, four, {} ... done!".
     * <p>
     * We use "{}" in the codebase to optimize for the most common case. Majority of applications have some SLF4J implementation in the classpath. That is
     * why we have to reformat messages here.
     */
    private static String formatMessage( String message, Object... params )
    {
        if ( params == null || params.length == 0 || message == null || message.isEmpty() )
        {
            return message;
        }

        StringBuilder messageBuilder = new StringBuilder( message.length() + params.length );
        messageBuilder.append( message.charAt( 0 ) );
        int paramsInserted = 0;

        for ( int i = 1; i < message.length(); i++ )
        {
            char c1 = message.charAt( i - 1 );
            char c2 = message.charAt( i );

            if ( c1 == '{' && c2 == '}' )
            {
                messageBuilder.append( paramsInserted++ ).append( c2 );
                if ( paramsInserted == params.length )
                {
                    messageBuilder.append( message, i + 1, message.length() );
                    break;
                }
            }
            else
            {
                messageBuilder.append( c2 );
            }
        }

        return MessageFormat.format( messageBuilder.toString(), params );
    }

    private static class JdkLogger implements Logger
    {
        final java.util.logging.Logger delegate;
        final boolean debugEnabled;
        final boolean traceEnabled;

        JdkLogger( java.util.logging.Logger delegate )
        {
            this.delegate = delegate;
            this.debugEnabled = delegate.isLoggable( Level.FINE );
            this.traceEnabled = delegate.isLoggable( Level.FINEST );
        }

        @Override
        public void error( String message, Throwable cause )
        {
            delegate.log( Level.SEVERE, message, cause );
        }

        @Override
        public void info( String message, Object... params )
        {
            delegate.log( Level.INFO, formatMessage( message, params ) );
        }

        @Override
        public void warn( String message, Object... params )
        {
            delegate.log( Level.WARNING, formatMessage( message, params ) );
        }

        @Override
        public void warn( String message, Throwable cause )
        {
            delegate.log( Level.WARNING, message, cause );
        }

        @Override
        public void debug( String message, Object... params )
        {
            delegate.log( Level.FINE, formatMessage( message, params ) );
        }

        @Override
        public void trace( String message, Object... params )
        {
            delegate.log( Level.FINEST, formatMessage( message, params ) );
        }

        @Override
        public boolean isTraceEnabled()
        {
            return traceEnabled;
        }

        @Override
        public boolean isDebugEnabled()
        {
            return debugEnabled;
        }
    }
}
