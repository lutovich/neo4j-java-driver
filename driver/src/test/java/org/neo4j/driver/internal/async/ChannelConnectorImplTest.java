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
package org.neo4j.driver.internal.async;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.ssl.SslHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.ConnectionSettings;
import org.neo4j.driver.internal.async.inbound.ConnectTimeoutHandler;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.util.FakeClock;
import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.exceptions.AuthenticationException;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.util.Neo4jExtension;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.v1.util.TestUtil.await;

@ExtendWith( Neo4jExtension.class )
public class ChannelConnectorImplTest
{
    private Neo4jExtension neo4j;
    private Bootstrap bootstrap;

    @BeforeEach
    public void setUp( Neo4jExtension neo4jExtension )
    {
        neo4j = neo4jExtension;
        bootstrap = BootstrapFactory.newBootstrap( 1 );
    }

    @AfterEach
    public void tearDown()
    {
        if ( bootstrap != null )
        {
            bootstrap.config().group().shutdownGracefully().syncUninterruptibly();
        }
    }

    @Test
    public void shouldConnect() throws Exception
    {
        ChannelConnector connector = newConnector( neo4j.authToken() );

        ChannelFuture channelFuture = connector.connect( neo4j.address(), bootstrap );
        assertTrue( channelFuture.await( 10, TimeUnit.SECONDS ) );
        Channel channel = channelFuture.channel();

        assertNull( channelFuture.get() );
        assertTrue( channel.isActive() );
    }

    @Test
    public void shouldSetupHandlers() throws Exception
    {
        ChannelConnector connector = newConnector( neo4j.authToken(), SecurityPlan.forAllCertificates(), 10_000 );

        ChannelFuture channelFuture = connector.connect( neo4j.address(), bootstrap );
        assertTrue( channelFuture.await( 10, TimeUnit.SECONDS ) );

        Channel channel = channelFuture.channel();
        ChannelPipeline pipeline = channel.pipeline();
        assertTrue( channel.isActive() );

        assertNotNull( pipeline.get( SslHandler.class ) );
        assertNull( pipeline.get( ConnectTimeoutHandler.class ) );
    }

    @Test
    public void shouldFailToConnectToWrongAddress() throws Exception
    {
        ChannelConnector connector = newConnector( neo4j.authToken() );

        ChannelFuture channelFuture = connector.connect( new BoltServerAddress( "wrong-localhost" ), bootstrap );
        assertTrue( channelFuture.await( 10, TimeUnit.SECONDS ) );
        Channel channel = channelFuture.channel();

        try
        {
            channelFuture.get();
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( ExecutionException.class ) );
            assertThat( e.getCause(), instanceOf( ServiceUnavailableException.class ) );
            assertThat( e.getCause().getMessage(), startsWith( "Unable to connect" ) );
        }
        assertFalse( channel.isActive() );
    }

    @Test
    public void shouldFailToConnectWithWrongCredentials() throws Exception
    {
        AuthToken authToken = AuthTokens.basic( "neo4j", "wrong-password" );
        ChannelConnector connector = newConnector( authToken );

        ChannelFuture channelFuture = connector.connect( neo4j.address(), bootstrap );
        assertTrue( channelFuture.await( 10, TimeUnit.SECONDS ) );
        Channel channel = channelFuture.channel();

        try
        {
            channelFuture.get();
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( ExecutionException.class ) );
            assertThat( e.getCause(), instanceOf( AuthenticationException.class ) );
        }
        assertFalse( channel.isActive() );
    }

    @Test
    public void shouldEnforceConnectTimeout() throws Exception
    {
        ChannelConnector connector = newConnector( neo4j.authToken(), 1000 );

        // try connect to a non-routable ip address 10.0.0.0, it will never respond
        ChannelFuture channelFuture = connector.connect( new BoltServerAddress( "10.0.0.0" ), bootstrap );

        try
        {
            await( channelFuture );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( ServiceUnavailableException.class ) );
            assertThat( e.getCause(), instanceOf( ConnectException.class ) );
        }
    }

    @Test
    public void shouldFailWhenProtocolNegotiationTakesTooLong() throws Exception
    {
        // run without TLS so that Bolt handshake is the very first operation after connection is established
        testReadTimeoutOnConnect( SecurityPlan.insecure() );
    }

    @Test
    public void shouldFailWhenTLSHandshakeTakesTooLong() throws Exception
    {
        // run with TLS so that TLS handshake is the very first operation after connection is established
        testReadTimeoutOnConnect( SecurityPlan.forAllCertificates() );
    }

    private void testReadTimeoutOnConnect( SecurityPlan securityPlan ) throws IOException
    {
        try ( ServerSocket server = new ServerSocket( 0 ) ) // server that accepts connections but does not reply
        {
            int timeoutMillis = 1_000;
            BoltServerAddress address = new BoltServerAddress( "localhost", server.getLocalPort() );
            ChannelConnector connector = newConnector( neo4j.authToken(), securityPlan, timeoutMillis );

            ChannelFuture channelFuture = connector.connect( address, bootstrap );
            try
            {
                await( channelFuture );
                fail( "Exception expected" );
            }
            catch ( ServiceUnavailableException e )
            {
                assertEquals( e.getMessage(), "Unable to establish connection in " + timeoutMillis + "ms" );
            }
        }
    }

    private ChannelConnectorImpl newConnector( AuthToken authToken ) throws Exception
    {
        return newConnector( authToken, Integer.MAX_VALUE );
    }

    private ChannelConnectorImpl newConnector( AuthToken authToken, int connectTimeoutMillis ) throws Exception
    {
        return newConnector( authToken, SecurityPlan.forAllCertificates(), connectTimeoutMillis );
    }

    private ChannelConnectorImpl newConnector( AuthToken authToken, SecurityPlan securityPlan,
            int connectTimeoutMillis )
    {
        ConnectionSettings settings = new ConnectionSettings( authToken, connectTimeoutMillis );
        return new ChannelConnectorImpl( settings, securityPlan, DEV_NULL_LOGGING, new FakeClock() );
    }
}
