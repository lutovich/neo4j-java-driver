/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.junit.jupiter.api.Test;

import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.SessionConfig;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.driver.v1.AccessMode.WRITE;

class DatabaseSelectionIT
{
    @Test
    void shouldUseDatabase()
    {
        try ( Driver driver = GraphDatabase.driver( "bolt://localhost:60927" ) )
        {
            SessionConfig config1 = SessionConfig.builder()
                    .withAccessMode( WRITE )
                    .withDatabase( "orders" )
                    .build();

            try ( Session session = driver.session( config1 ) )
            {
                StatementResult result = session.run( "CREATE (o:Order {id: 42}) RETURN o.id AS orderId" );
                Record record = result.single();
                int orderId = record.get( "orderId" ).asInt();
                assertEquals( 42, orderId );
            }

            SessionConfig config2 = SessionConfig.builder()
                    .withAccessMode( WRITE )
                    .withDatabase( "people" )
                    .build();

            try ( Session session = driver.session( config2 ) )
            {
                try ( Transaction tx = session.beginTransaction() )
                {
                    tx.run( "CREATE (:Node)" );
                    tx.run( "RETURN 1" );
                    tx.run( "CREATE (:Node1)" );
                    tx.success();
                }
            }
        }
    }
}