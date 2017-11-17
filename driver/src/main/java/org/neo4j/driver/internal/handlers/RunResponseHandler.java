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
package org.neo4j.driver.internal.handlers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.v1.Value;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

public class RunResponseHandler implements ResponseHandler
{
    private final CompletableFuture<List<String>> statementKeysFuture = new CompletableFuture<>();
    private final CompletableFuture<Long> resultAvailableAfterFuture = new CompletableFuture<>();

    @Override
    public void onSuccess( Map<String,Value> metadata )
    {
        statementKeysFuture.complete( extractKeys( metadata ) );
        resultAvailableAfterFuture.complete( extractResultAvailableAfter( metadata ) );
    }

    @Override
    public void onFailure( Throwable error )
    {
        statementKeysFuture.complete( emptyList() );
        resultAvailableAfterFuture.complete( 0L );
    }

    @Override
    public void onRecord( Value[] fields )
    {
        throw new UnsupportedOperationException();
    }

    public CompletionStage<List<String>> statementKeysStage()
    {
        return statementKeysFuture;
    }

    public List<String> statementKeys()
    {
        return getIfCompleted( statementKeysFuture, "StatementKeys" );
    }

    public long resultAvailableAfter()
    {
        return getIfCompleted( resultAvailableAfterFuture, "ResultAvailableAfter" );
    }

    private static List<String> extractKeys( Map<String,Value> metadata )
    {
        Value keysValue = metadata.get( "fields" );
        if ( keysValue != null )
        {
            if ( !keysValue.isEmpty() )
            {
                List<String> keys = new ArrayList<>( keysValue.size() );
                for ( Value value : keysValue.values() )
                {
                    keys.add( value.asString() );
                }

                return keys;
            }
        }
        return emptyList();
    }

    private static long extractResultAvailableAfter( Map<String,Value> metadata )
    {
        Value resultAvailableAfterValue = metadata.get( "result_available_after" );
        if ( resultAvailableAfterValue != null )
        {
            return resultAvailableAfterValue.asLong();
        }
        return -1;
    }

    private static <T> T getIfCompleted( CompletableFuture<T> future, String name )
    {
        if ( !future.isDone() || future.isCompletedExceptionally() )
        {
            throw new IllegalStateException( name + " not yet populated, RUN response did not arrive?" );
        }
        T result = future.getNow( null );
        return requireNonNull( result );
    }
}
