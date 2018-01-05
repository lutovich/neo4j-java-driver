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

import java.util.stream.Stream;

import org.neo4j.driver.internal.value.ListValue;
import org.neo4j.driver.internal.value.MapValue;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.util.Neo4jSessionExtension;

import static java.util.Collections.emptyMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.driver.v1.Values.parameters;
import static org.neo4j.driver.v1.Values.value;

@ExtendWith( Neo4jSessionExtension.class )
public class ScalarTypeIT
{
    private Neo4jSessionExtension session;

    @BeforeEach
    void setUp( Neo4jSessionExtension sessionExtension )
    {
        session = sessionExtension;
    }

    static Stream<Arguments> typesToTest()
    {
        return Stream.of(
                Arguments.of( "RETURN 1 as v", value( 1L ) ),
                Arguments.of( "RETURN 1.1 as v", value( 1.1d ) ),
                Arguments.of( "RETURN 'hello' as v", value( "hello" ) ),
                Arguments.of( "RETURN true as v", value( true ) ),
                Arguments.of( "RETURN false as v", value( false ) ),
                Arguments.of( "RETURN [1,2,3] as v", new ListValue( value( 1 ), value( 2 ), value( 3 ) ) ),
                Arguments.of( "RETURN ['hello'] as v", new ListValue( value( "hello" ) ) ),
                Arguments.of( "RETURN [] as v", new ListValue() ),
                Arguments.of( "RETURN {k:'hello'} as v", parameters( "k", value( "hello" ) ) ),
                Arguments.of( "RETURN {} as v", new MapValue( emptyMap() ) )
        );
    }

    @ParameterizedTest( name = "{0}" )
    @MethodSource( "typesToTest" )
    void shouldHandleType( String statement, Value expectedValue )
    {
        // When
        StatementResult cursor = session.run( statement );

        // Then
        assertThat( cursor.single().get( "v" ), equalTo( expectedValue ) );
    }
}
