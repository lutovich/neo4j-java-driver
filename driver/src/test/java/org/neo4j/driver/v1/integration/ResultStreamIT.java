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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.neo4j.driver.v1.util.Neo4jSessionExtension;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.driver.v1.Values.parameters;

@ExtendWith( Neo4jSessionExtension.class )
public class ResultStreamIT
{
    private Neo4jSessionExtension session;

    @BeforeEach
    void setUp( Neo4jSessionExtension sessionExtension )
    {
        session = sessionExtension;
    }

    @Test
    public void shouldAllowIteratingOverResultStream() throws Throwable
    {
        // When
        StatementResult res = session.run( "UNWIND [1,2,3,4] AS a RETURN a" );

        // Then I should be able to iterate over the result
        int idx = 1;
        while ( res.hasNext() )
        {
            assertEquals( idx++, res.next().get( "a" ).asLong() );
        }
    }

    @Test
    public void shouldHaveFieldNamesInResult()
    {
        // When
        StatementResult res = session.run( "CREATE (n:TestNode {name:'test'}) RETURN n" );

        // Then
        assertEquals( "[n]", res.keys().toString() );
        assertNotNull( res.single() );
        assertEquals( "[n]", res.keys().toString() );
    }

    @Test
    public void shouldGiveHelpfulFailureMessageWhenAccessNonExistingField() throws Throwable
    {
        // Given
        StatementResult rs =
                session.run( "CREATE (n:Person {name:{name}}) RETURN n", parameters( "name", "Tom Hanks" ) );

        // When
        Record single = rs.single();

        // Then
        assertTrue( single.get( "m" ).isNull() );
    }

    @Test
    public void shouldGiveHelpfulFailureMessageWhenAccessNonExistingPropertyOnNode() throws Throwable
    {
        // Given
        StatementResult rs =
                session.run( "CREATE (n:Person {name:{name}}) RETURN n", parameters( "name", "Tom Hanks" ) );

        // When
        Record record = rs.single();

        // Then
        assertTrue( record.get( "n" ).get( "age" ).isNull() );
    }

    @Test
    public void shouldNotReturnNullKeysOnEmptyResult()
    {
        // Given
        StatementResult rs = session.run( "CREATE (n:Person {name:{name}})", parameters( "name", "Tom Hanks" ) );

        // THEN
        assertNotNull( rs.keys() );
    }

    @Test
    public void shouldBeAbleToReuseSessionAfterFailure() throws Throwable
    {
        // Given
        StatementResult res1 = session.run( "INVALID" );
        try
        {
            res1.consume();
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            //ignore
        }

        // When
        StatementResult res2 = session.run( "RETURN 1" );

        // Then
        assertTrue( res2.hasNext() );
        assertEquals( res2.next().get("1").asLong(), 1L );
    }

    @Test
    public void shouldBeAbleToAccessSummaryAfterFailure() throws Throwable
    {
        // Given
        StatementResult res1 = session.run( "INVALID" );
        ResultSummary summary;

        // When
        try
        {
            res1.consume();
        }
        catch ( Exception e )
        {
            //ignore
        }
        finally
        {
            summary = res1.summary();
        }

        // Then
        assertThat( summary, notNullValue() );
        assertThat( summary.server().address(), equalTo( "localhost:7687" ) );
        assertThat( summary.counters().nodesCreated(), equalTo( 0 ) );
    }

    @Test
    public void shouldBeAbleToAccessSummaryAfterTransactionFailure()
    {
        StatementResult result = null;
        try
        {
            try ( Transaction tx = session.beginTransaction() )
            {
                result = tx.run( "UNWIND [1,2,0] AS x RETURN 10/x" );
                tx.success();
            }
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( ClientException.class ) );
            assertNotNull( result );
            assertEquals( 0, result.summary().counters().nodesCreated() );
        }
    }

    @Test
    public void shouldBufferRecordsAfterSummary() throws Throwable
    {
        // Given
        StatementResult result = session.run("UNWIND [1,2] AS a RETURN a");

        // When
        ResultSummary summary = result.summary();

        // Then
        assertThat( summary, notNullValue() );
        assertThat( summary.server().address(), equalTo( "localhost:7687" ) );
        assertThat( summary.counters().nodesCreated(), equalTo( 0 ) );

        assertThat( result.next().get( "a" ).asInt(), equalTo( 1 ) );
        assertThat( result.next().get( "a" ).asInt(), equalTo( 2 ) );
    }

    @Test
    public void shouldDiscardRecordsAfterConsume() throws Throwable
    {
        // Given
        StatementResult result = session.run("UNWIND [1,2] AS a RETURN a");

        // When
        ResultSummary summary = result.consume();

        // Then
        assertThat( summary, notNullValue() );
        assertThat( summary.server().address(), equalTo( "localhost:7687" ) );
        assertThat( summary.counters().nodesCreated(), equalTo( 0 ) );

        assertThat( result.hasNext(), equalTo( false ) );
    }

    @Test
    public void shouldHasNoElementsAfterFailure()
    {
        StatementResult result = session.run( "INVALID" );

        try
        {
            result.hasNext();
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( ClientException.class ) );
        }

        assertFalse( result.hasNext() );
    }

    @Test
    public void shouldBeAnEmptyLitAfterFailure()
    {
        StatementResult result = session.run( "UNWIND (0, 1) as i RETURN 10 / i" );

        try
        {
            result.list();
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( ClientException.class ) );
        }

        assertTrue( result.list().isEmpty() );
    }
}
