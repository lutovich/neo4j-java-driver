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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.time.LocalDate;

import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.util.TestNeo4jSession;

import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;
import static org.neo4j.driver.internal.util.ServerVersion.v3_4_0;

public class TemporalTypesIT
{
    @Rule
    public final TestNeo4jSession session = new TestNeo4jSession();

    @Before
    public void setUp()
    {
        assumeTrue( session.version().greaterThanOrEqual( v3_4_0 ) );
    }

    @Test
    public void shouldSendDate()
    {
        LocalDate localDate = LocalDate.now();

        Record record1 = session.run( "CREATE (n:Node {date: $date}) RETURN 42", singletonMap( "date", localDate ) ).single();
        assertEquals( 42, record1.get( 0 ).asInt() );

        Record record2 = session.run( "MATCH (n:Node) RETURN n.date" ).single();
        assertEquals( localDate, record2.get( 0 ).asLocalDate() );
    }

    @Test
    public void shouldReceiveDate()
    {
        Record record = session.run( "RETURN date({year: 1995, month: 12, day: 4})" ).single();
        assertEquals( LocalDate.of( 1995, 12, 4 ), record.get( 0 ).asLocalDate() );
    }

    @Test
    public void shouldSendAndReceiveDate()
    {
        LocalDate localDate = LocalDate.now();

        Record record = session.run( "CREATE (n:Node {date: $date}) RETURN n.date", singletonMap( "date", localDate ) ).single();
        assertEquals( localDate, record.get( 0 ).asLocalDate() );
    }
}
