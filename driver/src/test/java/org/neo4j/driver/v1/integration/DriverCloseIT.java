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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;
import java.util.logging.Level;

import org.neo4j.driver.internal.logging.ConsoleLogging;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.util.Neo4jExtension;
import org.neo4j.driver.v1.util.StubServer;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.driver.v1.util.Neo4jRunner.DEFAULT_AUTH_TOKEN;

public class DriverCloseIT
{
    public abstract static class DriverCloseITBase
    {
        protected abstract Driver createDriver();

        @Test
        public void isEncryptedThrowsForClosedDriver()
        {
            Driver driver = createDriver();

            driver.close();

            try
            {
                driver.isEncrypted();
                fail( "Exception expected" );
            }
            catch ( Exception e )
            {
                assertThat( e, instanceOf( IllegalStateException.class ) );
            }
        }

        @Test
        public void sessionThrowsForClosedDriver()
        {
            Driver driver = createDriver();

            driver.close();

            try
            {
                driver.session();
                fail( "Exception expected" );
            }
            catch ( Exception e )
            {
                assertThat( e, instanceOf( IllegalStateException.class ) );
            }
        }

        @Test
        public void sessionWithModeThrowsForClosedDriver()
        {
            Driver driver = createDriver();

            driver.close();

            try
            {
                driver.session( AccessMode.WRITE );
                fail( "Exception expected" );
            }
            catch ( Exception e )
            {
                assertThat( e, instanceOf( IllegalStateException.class ) );
            }
        }

        @Test
        public void closeClosedDriver()
        {
            Driver driver = createDriver();

            driver.close();
            driver.close();
            driver.close();
        }
    }

    @Nested
    @ExtendWith( Neo4jExtension.class )
    public class DirectDriverCloseIT extends DriverCloseITBase
    {
        private Neo4jExtension neo4j;

        @BeforeEach
        public void beforeEach( Neo4jExtension neo4jExtension )
        {
            neo4j = neo4jExtension;
        }

        @Override
        protected Driver createDriver()
        {
            return GraphDatabase.driver( neo4j.uri(), neo4j.authToken() );
        }

        @Test
        public void useSessionAfterDriverIsClosed()
        {
            Driver driver = createDriver();
            Session session = driver.session();

            driver.close();

            try
            {
                session.run( "create ()" );
                fail( "Exception expected" );
            }
            catch ( Exception e )
            {
                assertThat( e, instanceOf( IllegalStateException.class ) );
            }
        }
    }

    @Nested
    public class RoutingDriverCloseIT extends DriverCloseITBase
    {
        private StubServer router;

        @BeforeEach
        public void beforeEach() throws Exception
        {
            router = StubServer.start( "acquire_endpoints.script", 9001 );
        }

        @AfterEach
        public void tearDown() throws Exception
        {
            if ( router != null )
            {
                router.exitStatus();
            }
        }

        @Override
        protected Driver createDriver()
        {
            Config config = Config.build()
                    .withoutEncryption()
                    .withLogging( new ConsoleLogging( Level.OFF ) )
                    .toConfig();

            return GraphDatabase.driver( "bolt+routing://127.0.0.1:9001", DEFAULT_AUTH_TOKEN, config );
        }

        @Test
        public void useSessionAfterDriverIsClosed() throws Exception
        {
            StubServer readServer = StubServer.start( "read_server.script", 9005 );

            Driver driver = createDriver();

            try ( Session session = driver.session( AccessMode.READ ) )
            {
                List<Record> records = session.run( "MATCH (n) RETURN n.name" ).list();
                assertEquals( 3, records.size() );
            }

            Session session = driver.session( AccessMode.READ );

            driver.close();

            try
            {
                session.run( "MATCH (n) RETURN n.name" );
                fail( "Exception expected" );
            }
            catch ( Exception e )
            {
                assertThat( e, instanceOf( IllegalStateException.class ) );
            }

            assertEquals( 0, readServer.exitStatus() );
        }
    }
}
