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
package org.neo4j.driver.v1.integration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.neo4j.driver.internal.DriverFactory;
import org.neo4j.driver.internal.cluster.RoutingSettings;
import org.neo4j.driver.internal.retry.RetrySettings;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.internal.util.DriverFactoryWithClock;
import org.neo4j.driver.internal.util.FakeClock;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.util.Neo4jExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.driver.v1.util.Neo4jRunner.DEFAULT_AUTH_TOKEN;
import static org.neo4j.driver.v1.util.Neo4jRunner.DEFAULT_URI;

/**
 * Mainly concerned about the connection pool - we want to make sure that bad connections are evacuated from the
 * pool properly if the server dies, or all connections are lost for some other reason.
 */
@ExtendWith( Neo4jExtension.class )
public class ServerKilledIT
{
    private Neo4jExtension neo4j;

    @BeforeEach
    void setUp( Neo4jExtension neo4jExtension )
    {
        neo4j = neo4jExtension;
    }

    static Stream<Arguments> configsToTest()
    {
        return Stream.of(
                Arguments.of( "plaintext", Config.build().withoutEncryption() ),
                Arguments.of( "tls encrypted", Config.build().withEncryption() )
        );
    }

    @ParameterizedTest( name = "{0} connections" )
    @MethodSource( "configsToTest" )
    public void shouldRecoverFromServerRestart( String testName, Config.ConfigBuilder config )
    {
        // Given config with sessionLivenessCheckTimeout not set, i.e. turned off
        try ( Driver driver = GraphDatabase.driver( DEFAULT_URI, DEFAULT_AUTH_TOKEN, config.toConfig() ) )
        {
            acquireAndReleaseConnections( 4, driver );

            // When
            neo4j.forceRestartDb();

            // Then we should be able to start using sessions again, at most O(numSessions) session calls later
            int toleratedFailures = 4;
            for ( int i = 0; i < 10; i++ )
            {
                try ( Session s = driver.session() )
                {
                    s.run( "RETURN 'Hello, world!'" );
                }
                catch ( ServiceUnavailableException e )
                {
                    if ( toleratedFailures-- == 0 )
                    {
                        fail( "Expected (for now) at most four failures, one for each old connection, but now I've " +
                              "gotten " + "five: " + e.getMessage() );
                    }
                }
            }
        }
    }

    @ParameterizedTest( name = "{0} connections" )
    @MethodSource( "configsToTest" )
    public void shouldDropBrokenOldSessions( String testName, Config.ConfigBuilder config )
    {
        // config with set liveness check timeout
        int livenessCheckTimeoutMinutes = 10;
        config.withConnectionLivenessCheckTimeout( livenessCheckTimeoutMinutes, TimeUnit.MINUTES );

        FakeClock clock = new FakeClock();

        try ( Driver driver = createDriver( clock, config.toConfig() ) )
        {
            acquireAndReleaseConnections( 5, driver );

            // restart database to invalidate all idle connections in the pool
            neo4j.forceRestartDb();
            // move clock forward more than configured liveness check timeout
            clock.progress( TimeUnit.MINUTES.toMillis( livenessCheckTimeoutMinutes + 1 ) );

            // now all idle connections should be considered too old and will be verified during acquisition
            // they will appear broken because of the database restart and new valid connection will be created
            try ( Session session = driver.session() )
            {
                List<Record> records = session.run( "RETURN 1" ).list();
                assertEquals( 1, records.size() );
                assertEquals( 1, records.get( 0 ).get( 0 ).asInt() );
            }
        }
    }

    private static void acquireAndReleaseConnections( int count, Driver driver )
    {
        if ( count > 0 )
        {
            Session session = driver.session();
            session.run( "RETURN 1" );
            acquireAndReleaseConnections( count - 1, driver );
            session.close();
        }
    }

    private static Driver createDriver( Clock clock, Config config )
    {
        DriverFactory factory = new DriverFactoryWithClock( clock );
        RoutingSettings routingSettings = new RoutingSettings( 1, 1, null );
        RetrySettings retrySettings = RetrySettings.DEFAULT;
        return factory.newInstance( DEFAULT_URI, DEFAULT_AUTH_TOKEN, routingSettings, retrySettings, config );
    }
}
