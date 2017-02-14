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
package org.neo4j.driver.internal;

import org.neo4j.driver.v1.RetryDecision;
import org.neo4j.driver.v1.RetryLogic;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.exceptions.SessionExpiredException;

public class SimpleRetryLogic implements RetryLogic<SimpleRetryDecision>
{
    private final int times;
    private final long delayMs;

    public SimpleRetryLogic( int times, long delayMs )
    {
        this.times = times;
        this.delayMs = delayMs;
    }

    @Override
    public SimpleRetryDecision apply( Throwable error, SimpleRetryDecision previousDecision )
    {
        int attempt = previousDecision == null ? 1 : previousDecision.attempt();
        if ( attempt >= times )
        {
            return SimpleRetryDecision.THROW;
        }

        if ( error instanceof SessionExpiredException || error instanceof ServiceUnavailableException )
        {
            try
            {
                Thread.sleep( delayMs );
            }
            catch ( InterruptedException ignore )
            {
                // todo: really ignore error here?
            }

            return new SimpleRetryDecision( RetryDecision.Action.RETRY, attempt + 1 );
        }

        return SimpleRetryDecision.THROW;
    }
}
