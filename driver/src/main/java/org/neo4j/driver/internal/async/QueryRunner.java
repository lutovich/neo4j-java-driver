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
package org.neo4j.driver.internal.async;

import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.internal.ExplicitTransaction;
import org.neo4j.driver.internal.handlers.PullAllResponseHandler;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.handlers.SessionPullAllResponseHandler;
import org.neo4j.driver.internal.handlers.TransactionPullAllResponseHandler;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.Value;

import static org.neo4j.driver.v1.Values.ofValue;

// todo: better method naming in this class and tests!
public final class QueryRunner
{
    private QueryRunner()
    {
    }

    public static InternalStatementResultCursor run( CompletionStage<Connection> connectionStage, Statement statement,
            ExplicitTransaction tx, boolean async )
    {
        String query = statement.text();
        Map<String,Value> params = statement.parameters().asMap( ofValue() );

        RunResponseHandler runHandler = new RunResponseHandler();
        PullAllResponseHandler pullAllHandler = newPullAllHandler( statement, runHandler, connectionStage, tx );

        connectionStage.thenAccept( connection ->
                connection.runAndFlush( query, params, runHandler, pullAllHandler ) );

        if ( async )
        {
            return InternalStatementResultCursor.forAsyncRun( runHandler, pullAllHandler, connectionStage );
        }
        else
        {
            return InternalStatementResultCursor.forBlockingRun( runHandler, pullAllHandler, connectionStage );
        }
    }

    private static PullAllResponseHandler newPullAllHandler( Statement statement, RunResponseHandler runHandler,
            CompletionStage<Connection> connectionStage, ExplicitTransaction tx )
    {
        if ( tx != null )
        {
            return new TransactionPullAllResponseHandler( statement, runHandler, connectionStage, tx );
        }
        return new SessionPullAllResponseHandler( statement, runHandler, connectionStage );
    }
}
