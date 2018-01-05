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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;

import org.neo4j.driver.internal.security.InternalAuthToken;
import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.AuthenticationException;
import org.neo4j.driver.v1.exceptions.SecurityException;
import org.neo4j.driver.v1.util.Neo4jExtension;
import org.neo4j.driver.v1.util.Neo4jSettings;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.v1.AuthTokens.basic;
import static org.neo4j.driver.v1.AuthTokens.custom;
import static org.neo4j.driver.v1.Values.ofValue;
import static org.neo4j.driver.v1.Values.parameters;

@ExtendWith( Neo4jExtension.class )
public class CredentialsIT
{
    private static final String PASSWORD = "secret";

    private static Neo4jExtension neo4j;

    @BeforeAll
    public static void enableAuth( Neo4jExtension neo4jExtension ) throws Exception
    {
        neo4j = neo4jExtension;

        // db will be restarted with custom password, disable cleanup which uses driver with default credentials
        neo4j.setCleanDbBeforeEach( false );

        neo4j.restartDb( Neo4jSettings.TEST_SETTINGS
                .updateWith( Neo4jSettings.AUTH_ENABLED, "true" )
                .updateWith( Neo4jSettings.DATA_DIR, tempDirectory() ) );

        InternalAuthToken authToken = new InternalAuthToken( parameters(
                "scheme", "basic",
                "principal", "neo4j",
                "credentials", "neo4j",
                "new_credentials", PASSWORD ).asMap( ofValue() ) );

        try ( Driver driver = GraphDatabase.driver( neo4j.uri(), authToken );
              Session session = driver.session() )
        {
            session.run( "RETURN 1" ).consume();
        }
    }

    @Test
    public void basicCredentialsShouldWork() throws Throwable
    {
        // When & Then
        try( Driver driver = GraphDatabase.driver( neo4j.uri(),
                basic( "neo4j", PASSWORD ) );
             Session session = driver.session() )
        {
            Value single = session.run( "RETURN 1" ).single().get( 0 );
            assertThat( single.asLong(), equalTo( 1L ) );
        }
    }

    @Test
    public void shouldGetHelpfulErrorOnInvalidCredentials() throws Throwable
    {
        // When
        try ( Driver driver = GraphDatabase.driver( neo4j.uri(), basic( "thisisnotthepassword", PASSWORD ) );
              Session session = driver.session() )
        {
            session.run( "RETURN 1" );
            fail( "Should fail with an auth error already" );
        }
        catch ( Throwable e )
        {
            assertThat( e, instanceOf( SecurityException.class ) );
            assertThat( e.getMessage(), containsString( "The client is unauthorized due to authentication failure." ) );
        }
    }

    @Test
    public void shouldBeAbleToProvideRealmWithBasicAuth() throws Throwable
    {
        // When & Then
        try( Driver driver = GraphDatabase.driver( neo4j.uri(),
                basic( "neo4j", PASSWORD, "native" ) );
             Session session = driver.session() )
        {
            Value single = session.run( "CREATE () RETURN 1" ).single().get( 0 );
            assertThat( single.asLong(), equalTo( 1L ) );
        }
    }

    @Test
    public void shouldBeAbleToConnectWithCustomToken() throws Throwable
    {
        // When & Then
        try( Driver driver = GraphDatabase.driver( neo4j.uri(),
                custom( "neo4j", PASSWORD, "native", "basic" ) );
             Session session = driver.session() )
        {
            Value single = session.run( "CREATE () RETURN 1" ).single().get( 0 );
            assertThat( single.asLong(), equalTo( 1L ) );
        }
    }

    @Test
    public void shouldBeAbleToConnectWithCustomTokenWithAdditionalParameters() throws Throwable
    {
        HashMap<String,Object> parameters = new HashMap<>();
        parameters.put( "secret", 16 );

        // When & Then
        try( Driver driver = GraphDatabase.driver( neo4j.uri(),
                custom( "neo4j", PASSWORD, "native", "basic", parameters ) );
             Session session = driver.session() )
        {
            Value single = session.run( "CREATE () RETURN 1" ).single().get( 0 );
            assertThat( single.asLong(), equalTo( 1L ) );
        }
    }

    @Test
    public void directDriverShouldFailEarlyOnWrongCredentials()
    {
        testDriverFailureOnWrongCredentials( "bolt://localhost" );
    }

    @Test
    public void routingDriverShouldFailEarlyOnWrongCredentials()
    {
        testDriverFailureOnWrongCredentials( "bolt+routing://localhost" );
    }

    private void testDriverFailureOnWrongCredentials( String uri )
    {
        Config config = Config.build().withLogging( DEV_NULL_LOGGING ).toConfig();
        AuthToken authToken = AuthTokens.basic( "neo4j", "wrongSecret" );

        try
        {
            GraphDatabase.driver( uri, authToken, config );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( AuthenticationException.class ) );
        }
    }

    private static String tempDirectory()
    {
        try
        {
            Path tempDirectory = Files.createTempDirectory( Paths.get( "target" ), "data" );
            String absolutePath = tempDirectory.toAbsolutePath().toString();
            return absolutePath.replace( "\\", "/" );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }
}
