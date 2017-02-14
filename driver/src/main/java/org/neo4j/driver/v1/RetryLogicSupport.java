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
package org.neo4j.driver.v1;

import java.util.concurrent.TimeUnit;

import org.neo4j.driver.internal.SimpleRetryLogic;

import static java.util.concurrent.TimeUnit.SECONDS;

public final class RetryLogicSupport
{
    private RetryLogicSupport()
    {
    }

    @SuppressWarnings( "unchecked" )
    public static RetryLogic<RetryDecision> defaultRetryLogic()
    {
        return simpleRetryLogic( 5, 1, SECONDS );
    }

    @SuppressWarnings( "unchecked" )
    public static RetryLogic<RetryDecision> simpleRetryLogic( int times, long delay, TimeUnit delayUnit )
    {
        // todo: cast sucks!
        return (RetryLogic) new SimpleRetryLogic( times, delayUnit.toMillis( delay ) );
    }
}
