/*
 * Copyright (c) 2002-2018 Neo4j Sweden AB [http://neo4j.com]
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

import java.util.logging.Level;

import org.neo4j.driver.internal.logging.JdkLogging;
import org.neo4j.driver.internal.logging.Slf4jLogging;

/**
 * Accessor for {@link Logger} instances.
 */
public interface Logging
{
    /**
     * Obtain a {@link Logger} instance by name.
     *
     * @param name name of a {@link Logger}
     * @return {@link Logger} instance
     */
    Logger getLog( String name );

    static Logging slf4j()
    {
        return new Slf4jLogging();
    }

    static Logging jdk( Level level )
    {
        return new JdkLogging( level );
    }
}
