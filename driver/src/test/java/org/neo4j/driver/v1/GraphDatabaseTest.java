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
package org.neo4j.driver.v1;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.net.URI;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.util.StubServer;
import org.neo4j.driver.v1.util.TestNeo4j;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.util.Matchers.clusterDriver;
import static org.neo4j.driver.internal.util.Matchers.directDriver;
import static org.neo4j.driver.v1.Config.TrustStrategy.trustOnFirstUse;
import static org.neo4j.driver.v1.util.StubServer.INSECURE_CONFIG;

public class GraphDatabaseTest
{
    @Test
    public void boltSchemeShouldInstantiateDirectDriver() throws Exception
    {
        // Given
        StubServer server = StubServer.start( "dummy_connection.script", 9001 );
        URI uri = URI.create( "bolt://localhost:9001" );

        // When
        Driver driver = GraphDatabase.driver( uri, INSECURE_CONFIG );

        // Then
        assertThat( driver, is( directDriver() ) );

        // Finally
        driver.close();
        assertThat( server.exitStatus(), equalTo( 0 ) );
    }

    @Test
    public void boltPlusDiscoverySchemeShouldInstantiateClusterDriver() throws Exception
    {
        // Given
        StubServer server = StubServer.start( "discover_servers.script", 9001 );
        URI uri = URI.create( "bolt+routing://127.0.0.1:9001" );

        // When
        Driver driver = GraphDatabase.driver( uri, INSECURE_CONFIG );

        // Then
        assertThat( driver, is( clusterDriver() ) );

        // Finally
        driver.close();
        assertThat( server.exitStatus(), equalTo( 0 ) );
    }

    @Test
    public void boltPlusDiscoverySchemeShouldNotSupportTrustOnFirstUse()
    {
        URI uri = URI.create( "bolt+routing://127.0.0.1:9001" );

        Config config = Config.build()
                .withEncryption()
                .withTrustStrategy( trustOnFirstUse( new File( "./known_hosts" ) ) )
                .toConfig();

        try
        {
            GraphDatabase.driver( uri, config );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalArgumentException.class ) );
        }
    }

    @Test
    public void throwsWhenBoltSchemeUsedWithRoutingParams()
    {
        try
        {
            GraphDatabase.driver( "bolt://localhost:7687/?policy=my_policy" );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalArgumentException.class ) );
        }
    }

    @Test
    public void shouldLogWhenUnableToCreateRoutingDriver() throws Exception
    {
        StubServer server1 = StubServer.start( "non_discovery_server.script", 9001 );
        StubServer server2 = StubServer.start( "non_discovery_server.script", 9002 );

        Logging logging = mock( Logging.class );
        Logger logger = mock( Logger.class );
        when( logging.getLog( anyString() ) ).thenReturn( logger );

        Config config = Config.build()
                .withoutEncryption()
                .withLogging( logging )
                .toConfig();

        List<URI> routingUris = asList(
                URI.create( "bolt+routing://localhost:9001" ),
                URI.create( "bolt+routing://localhost:9002" ) );

        try
        {
            GraphDatabase.routingDriver( routingUris, AuthTokens.none(), config );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( ServiceUnavailableException.class ) );
        }

        verify( logger ).warn( eq( "Unable to create routing driver for URI: bolt+routing://localhost:9001" ),
                any( Throwable.class ) );

        verify( logger ).warn( eq( "Unable to create routing driver for URI: bolt+routing://localhost:9002" ),
                any( Throwable.class ) );

        assertEquals( 0, server1.exitStatus() );
        assertEquals( 0, server2.exitStatus() );
    }
}
