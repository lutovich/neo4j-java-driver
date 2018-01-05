/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.driver.internal;

import io.netty.bootstrap.Bootstrap;
import io.netty.util.concurrent.EventExecutorGroup;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.net.URI;

import org.neo4j.driver.internal.async.BootstrapFactory;
import org.neo4j.driver.internal.cluster.RoutingSettings;
import org.neo4j.driver.internal.cluster.loadbalancing.LoadBalancer;
import org.neo4j.driver.internal.retry.RetryLogic;
import org.neo4j.driver.internal.retry.RetrySettings;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.spi.ConnectionProvider;
import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.util.Futures.completedWithNull;
import static org.neo4j.driver.internal.util.Futures.failedFuture;
import static org.neo4j.driver.v1.AccessMode.READ;
import static org.neo4j.driver.v1.Config.defaultConfig;

public class DriverFactoryTest
{
    @ParameterizedTest( name = "{0}" )
    @ValueSource( strings = {"bolt://localhost:7687", "bolt+routing://localhost:7687"} )
    @Retention( RetentionPolicy.RUNTIME )
    public @interface ParameterizedWithUriTest
    {
    }

    @ParameterizedWithUriTest
    public void connectionPoolClosedWhenDriverCreationFails( String uri ) throws Exception
    {
        ConnectionPool connectionPool = connectionPoolMock();
        DriverFactory factory = new ThrowingDriverFactory( connectionPool );

        try
        {
            createDriver( uri, factory );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( UnsupportedOperationException.class ) );
        }
        verify( connectionPool ).close();
    }

    @ParameterizedWithUriTest
    public void connectionPoolCloseExceptionIsSuppressedWhenDriverCreationFails( String uri ) throws Exception
    {
        ConnectionPool connectionPool = connectionPoolMock();
        RuntimeException poolCloseError = new RuntimeException( "Pool close error" );
        when( connectionPool.close() ).thenReturn( failedFuture( poolCloseError ) );

        DriverFactory factory = new ThrowingDriverFactory( connectionPool );

        try
        {
            createDriver( uri, factory );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( UnsupportedOperationException.class ) );
            assertArrayEquals( new Throwable[]{poolCloseError}, e.getSuppressed() );
        }
        verify( connectionPool ).close();
    }

    @ParameterizedWithUriTest
    public void usesStandardSessionFactoryWhenNothingConfigured( String uri )
    {
        Config config = Config.defaultConfig();
        SessionFactoryCapturingDriverFactory factory = new SessionFactoryCapturingDriverFactory();

        createDriver( uri, factory, config );

        SessionFactory capturedFactory = factory.capturedSessionFactory;
        assertThat( capturedFactory.newInstance( READ, null ), instanceOf( NetworkSession.class ) );
    }

    @ParameterizedWithUriTest
    public void usesLeakLoggingSessionFactoryWhenConfigured( String uri )
    {
        Config config = Config.build().withLeakedSessionsLogging().toConfig();
        SessionFactoryCapturingDriverFactory factory = new SessionFactoryCapturingDriverFactory();

        createDriver( uri, factory, config );

        SessionFactory capturedFactory = factory.capturedSessionFactory;
        assertThat( capturedFactory.newInstance( READ, null ), instanceOf( LeakLoggingNetworkSession.class ) );
    }

    @ParameterizedWithUriTest
    public void shouldVerifyConnectivity( String uri )
    {
        SessionFactory sessionFactory = mock( SessionFactory.class );
        when( sessionFactory.verifyConnectivity() ).thenReturn( completedWithNull() );
        when( sessionFactory.close() ).thenReturn( completedWithNull() );
        DriverFactoryWithSessions driverFactory = new DriverFactoryWithSessions( sessionFactory );

        try ( Driver driver = createDriver( uri, driverFactory ) )
        {
            assertNotNull( driver );
            verify( sessionFactory ).verifyConnectivity();
        }
    }

    @ParameterizedWithUriTest
    public void shouldThrowWhenUnableToVerifyConnectivity( String uri )
    {
        SessionFactory sessionFactory = mock( SessionFactory.class );
        ServiceUnavailableException error = new ServiceUnavailableException( "Hello" );
        when( sessionFactory.verifyConnectivity() ).thenReturn( failedFuture( error ) );
        when( sessionFactory.close() ).thenReturn( completedWithNull() );
        DriverFactoryWithSessions driverFactory = new DriverFactoryWithSessions( sessionFactory );

        try
        {
            createDriver( uri, driverFactory );
            fail( "Exception expected" );
        }
        catch ( ServiceUnavailableException e )
        {
            assertEquals( "Hello", e.getMessage() );
        }
    }

    private static Driver createDriver( String uri, DriverFactory driverFactory )
    {
        return createDriver( uri, driverFactory, defaultConfig() );
    }

    private static Driver createDriver( String uri, DriverFactory driverFactory, Config config )
    {
        AuthToken auth = AuthTokens.none();
        RoutingSettings routingSettings = new RoutingSettings( 42, 42, null );
        return driverFactory.newInstance( URI.create( uri ), auth, routingSettings, RetrySettings.DEFAULT, config );
    }

    private static ConnectionPool connectionPoolMock()
    {
        ConnectionPool pool = mock( ConnectionPool.class );
        Connection connection = mock( Connection.class );
        when( pool.acquire( any( BoltServerAddress.class ) ) ).thenReturn( completedFuture( connection ) );
        when( pool.close() ).thenReturn( completedWithNull() );
        return pool;
    }

    private static class ThrowingDriverFactory extends DriverFactory
    {
        final ConnectionPool connectionPool;

        ThrowingDriverFactory( ConnectionPool connectionPool )
        {
            this.connectionPool = connectionPool;
        }

        @Override
        protected InternalDriver createDriver( SessionFactory sessionFactory, SecurityPlan securityPlan, Config config )
        {
            throw new UnsupportedOperationException( "Can't create direct driver" );
        }

        @Override
        protected InternalDriver createRoutingDriver( BoltServerAddress address, ConnectionPool connectionPool,
                Config config, RoutingSettings routingSettings, SecurityPlan securityPlan, RetryLogic retryLogic,
                EventExecutorGroup eventExecutorGroup )
        {
            throw new UnsupportedOperationException( "Can't create routing driver" );
        }

        @Override
        protected ConnectionPool createConnectionPool( AuthToken authToken, SecurityPlan securityPlan,
                Bootstrap bootstrap, Config config )
        {
            return connectionPool;
        }
    }

    private static class SessionFactoryCapturingDriverFactory extends DriverFactory
    {
        SessionFactory capturedSessionFactory;

        @Override
        protected InternalDriver createDriver( SessionFactory sessionFactory, SecurityPlan securityPlan, Config config )
        {
            InternalDriver driver = mock( InternalDriver.class );
            when( driver.verifyConnectivity() ).thenReturn( completedWithNull() );
            return driver;
        }

        @Override
        protected LoadBalancer createLoadBalancer( BoltServerAddress address, ConnectionPool connectionPool,
                EventExecutorGroup eventExecutorGroup, Config config, RoutingSettings routingSettings )
        {
            return null;
        }

        @Override
        protected SessionFactory createSessionFactory( ConnectionProvider connectionProvider,
                RetryLogic retryLogic, Config config )
        {
            SessionFactory sessionFactory = super.createSessionFactory( connectionProvider, retryLogic, config );
            capturedSessionFactory = sessionFactory;
            return sessionFactory;
        }

        @Override
        protected ConnectionPool createConnectionPool( AuthToken authToken, SecurityPlan securityPlan,
                Bootstrap bootstrap, Config config )
        {
            return connectionPoolMock();
        }
    }

    private static class DriverFactoryWithSessions extends DriverFactory
    {
        final SessionFactory sessionFactory;

        DriverFactoryWithSessions( SessionFactory sessionFactory )
        {
            this.sessionFactory = sessionFactory;
        }

        @Override
        protected Bootstrap createBootstrap()
        {
            return BootstrapFactory.newBootstrap( 1 );
        }

        @Override
        protected ConnectionPool createConnectionPool( AuthToken authToken, SecurityPlan securityPlan,
                Bootstrap bootstrap, Config config )
        {
            return connectionPoolMock();
        }

        @Override
        protected SessionFactory createSessionFactory( ConnectionProvider connectionProvider, RetryLogic retryLogic,
                Config config )
        {
            return sessionFactory;
        }
    }
}
