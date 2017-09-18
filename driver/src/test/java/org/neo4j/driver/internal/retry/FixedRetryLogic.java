/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.driver.internal.retry;

import io.netty.channel.DefaultEventLoop;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.ImmediateEventExecutor;

import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.driver.internal.async.InternalFuture;
import org.neo4j.driver.internal.async.InternalPromise;
import org.neo4j.driver.internal.util.Supplier;

public class FixedRetryLogic implements RetryLogic
{
    private final int retryCount;
    private final EventLoopGroup eventLoopGroup;

    public FixedRetryLogic( int retryCount )
    {
        this( retryCount, new DefaultEventLoop( ImmediateEventExecutor.INSTANCE ) );
    }

    public FixedRetryLogic( int retryCount, EventLoopGroup eventLoopGroup )
    {
        this.retryCount = retryCount;
        this.eventLoopGroup = eventLoopGroup;
    }

    @Override
    public <T> T retry( Supplier<T> work )
    {
        int invokedWork = 0;

        while ( true )
        {
            try
            {
                return work.get();
            }
            catch ( Throwable error )
            {
                if ( invokedWork++ >= retryCount )
                {
                    throw error;
                }
            }
        }
    }

    @Override
    public <T> InternalFuture<T> retryAsync( Supplier<InternalFuture<T>> work )
    {
        InternalPromise<T> resultPromise = new InternalPromise<T>( eventLoopGroup.next() );
        AtomicInteger workInvocationCount = new AtomicInteger();

        invokeWorkInEventLoop( work, resultPromise, workInvocationCount );

        return resultPromise;
    }

    private <T> void invokeWorkInEventLoop( final Supplier<InternalFuture<T>> work,
            final InternalPromise<T> resultPromise, final AtomicInteger workInvocationCount )
    {
        EventLoop eventLoop = eventLoopGroup.next();
        eventLoop.execute( new Runnable()
        {
            @Override
            public void run()
            {
                invokeWork( work, resultPromise, workInvocationCount );
            }
        } );
    }

    private <T> void invokeWork( final Supplier<InternalFuture<T>> work, final InternalPromise<T> resultPromise,
            final AtomicInteger workInvocationCount )
    {
        try
        {
            final InternalFuture<T> workFuture = work.get();
            workFuture.addListener( new GenericFutureListener<Future<T>>()
            {
                @Override
                public void operationComplete( Future<T> future ) throws Exception
                {
                    if ( future.isCancelled() )
                    {
                        workFuture.cancel( true );
                    }
                    else if ( future.isSuccess() )
                    {
                        resultPromise.setSuccess( future.getNow() );
                    }
                    else
                    {
                        if ( workInvocationCount.incrementAndGet() >= retryCount )
                        {
                            resultPromise.setFailure( future.cause() );
                        }
                        else
                        {
                            invokeWorkInEventLoop( work, resultPromise, workInvocationCount );
                        }
                    }
                }
            } );
        }
        catch ( Throwable error )
        {
            if ( workInvocationCount.incrementAndGet() >= retryCount )
            {
                resultPromise.setFailure( error );
            }
            else
            {
                invokeWorkInEventLoop( work, resultPromise, workInvocationCount );
            }
        }
    }
}
