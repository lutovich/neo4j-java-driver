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

package org.neo4j.driver.internal.summary;

import org.neo4j.driver.internal.async.BoltServerAddress;
import org.neo4j.driver.internal.util.ServerVersion;
import org.neo4j.driver.v1.summary.ServerInfo;

public class InternalServerInfo implements ServerInfo
{
    public static final InternalServerInfo EMPTY = new InternalServerInfo( null, null );

    private final String address;
    private final String version;

    public InternalServerInfo( BoltServerAddress address, ServerVersion version )
    {
        this.address = nullOrString( address );
        this.version = nullOrString( version );
    }

    @Override
    public String address()
    {
        return address;
    }

    @Override
    public String version()
    {
        return version;
    }

    private static String nullOrString( Object value )
    {
        return value == null ? null : value.toString();
    }
}
