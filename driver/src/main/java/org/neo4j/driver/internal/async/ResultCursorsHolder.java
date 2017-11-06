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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

import static java.util.concurrent.CompletableFuture.completedFuture;

public class ResultCursorsHolder
{
    private final List<InternalStatementResultCursor> cursors = new ArrayList<>();

    public void add( InternalStatementResultCursor cursor )
    {
        Objects.requireNonNull( cursor );
        cursors.add( cursor );
    }

    public CompletionStage<Throwable> retrieveNotConsumedError()
    {
        return cursors.stream()
                .map( this::retrieveFailure )
                .reduce( completedFuture( null ), this::nonNullFailureFromEither );
    }

    private CompletionStage<Throwable> retrieveFailure( InternalStatementResultCursor cursor )
    {
        return cursor.failureAsync().exceptionally( error -> null );
    }

    private CompletionStage<Throwable> nonNullFailureFromEither( CompletionStage<Throwable> stage1,
            CompletionStage<Throwable> stage2 )
    {
        return stage1.thenCompose( value ->
        {
            if ( value != null )
            {
                return completedFuture( value );
            }
            return stage2;
        } );
    }
}
