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
package org.neo4j.driver.internal.async;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.FastThreadLocalThread;

import java.util.concurrent.Executor;
import java.util.concurrent.Future;

import org.neo4j.driver.v1.Session;

/**
 * Manages creation of Netty {@link EventLoopGroup}s, which are basically {@link Executor}s that perform IO operations.
 */
public final class EventLoopGroupFactory
{
    private static final boolean DISABLE_EPOLL_TRANSPORT = Boolean.getBoolean( "disableEpollTransport" );

    private static final String THREAD_NAME_PREFIX = "Neo4jDriverIO";
    private static final int THREAD_PRIORITY = Thread.MAX_PRIORITY;
    private static final int DEFAULT_THREAD_COUNT = 0;

    private EventLoopGroupFactory()
    {
    }

    /**
     * Get class of {@link Channel} for {@link Bootstrap#channel(Class)} method.
     *
     * @return class of the channel, which should be consistent with {@link EventLoopGroup}s returned by
     * {@link #newEventLoopGroup()} and {@link #newEventLoopGroup(int)}.
     */
    public static Class<? extends Channel> channelClass()
    {
        if ( canUseEpollTransport() )
        {
            return EpollSocketChannel.class;
        }
        return NioSocketChannel.class;
    }

    /**
     * Create new {@link EventLoopGroup} with default thread count. Returned group should by given to
     * {@link Bootstrap#group(EventLoopGroup)}.
     *
     * @return new group consistent with channel class returned by {@link #channelClass()}.
     */
    public static EventLoopGroup newEventLoopGroup()
    {
        return newEventLoopGroup( DEFAULT_THREAD_COUNT );
    }

    /**
     * Create new {@link EventLoopGroup} with specified thread count. Returned group should by given to
     * {@link Bootstrap#group(EventLoopGroup)}.
     *
     * @param threadCount amount of IO threads for the new group.
     * @return new group consistent with channel class returned by {@link #channelClass()}.
     */
    public static EventLoopGroup newEventLoopGroup( int threadCount )
    {
        if ( canUseEpollTransport() )
        {
            return newEpollEventLoopGroup( threadCount );
        }
        return newNioEventLoopGroup( threadCount );
    }

    /**
     * Assert that current thread is not an event loop used for async IO operations. This check is needed because
     * blocking API methods like {@link Session#run(String)} are implemented on top of corresponding async API methods
     * like {@link Session#runAsync(String)} using basically {@link Future#get()} calls. Deadlocks might happen when IO
     * thread executes blocking API call and has to wait for itself to read from the network.
     *
     * @throws IllegalStateException when current thread is an event loop IO thread.
     */
    public static void assertNotInEventLoopThread() throws IllegalStateException
    {
        if ( isEventLoopThread( Thread.currentThread() ) )
        {
            throw new IllegalStateException(
                    "Blocking operation can't be executed in IO thread because it might result in a deadlock. " +
                    "Please do not use blocking API when chaining futures returned by async API methods." );
        }
    }

    /**
     * Check if given thread is an event loop IO thread.
     *
     * @param thread the thread to check.
     * @return {@code true} when given thread belongs to the event loop, {@code false} otherwise.
     */
    public static boolean isEventLoopThread( Thread thread )
    {
        return thread instanceof DriverThread;
    }

    private static NioEventLoopGroup newNioEventLoopGroup( int threadCount )
    {
        return new NioEventLoopGroup( threadCount, new DriverThreadFactory() );
    }

    private static EpollEventLoopGroup newEpollEventLoopGroup( int threadCount )
    {
        return new EpollEventLoopGroup( threadCount, new DriverThreadFactory() );
    }

    private static boolean canUseEpollTransport()
    {
        boolean result = Epoll.isAvailable() && !DISABLE_EPOLL_TRANSPORT;
        System.out.println( "Epoll.isAvailable() = " + Epoll.isAvailable() );
        if(!Epoll.isAvailable()) {
            Epoll.unavailabilityCause().printStackTrace(System.out);
        }
        return result;
    }

    /**
     * Same as {@link DefaultThreadFactory} created by {@link MultithreadEventLoopGroup} by default, except produces
     * threads of {@link DriverThread} class. Such threads can be recognized by {@link #assertNotInEventLoopThread()}.
     */
    private static class DriverThreadFactory extends DefaultThreadFactory
    {
        DriverThreadFactory()
        {
            super( THREAD_NAME_PREFIX, THREAD_PRIORITY );
        }

        @Override
        protected Thread newThread( Runnable r, String name )
        {
            return new DriverThread( threadGroup, r, name );
        }
    }

    /**
     * Same as default thread created by {@link DefaultThreadFactory} except this dedicated class can be easily
     * recognized by {@link #assertNotInEventLoopThread()}.
     */
    private static class DriverThread extends FastThreadLocalThread
    {
        DriverThread( ThreadGroup group, Runnable target, String name )
        {
            super( group, target, name );
        }
    }
}
