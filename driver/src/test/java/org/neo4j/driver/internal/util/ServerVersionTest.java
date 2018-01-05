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
package org.neo4j.driver.internal.util;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ServerVersionTest
{
    @Test
    public void version() throws Exception
    {
        String nullVersion = null;
        assertThat( ServerVersion.version( nullVersion ), is( ServerVersion.v3_0_0 ) );
        assertThat( ServerVersion.version( "Neo4j/dev" ), is( ServerVersion.vInDev ) );
        assertThat( ServerVersion.version( "Neo4j/3.2.0" ), is( ServerVersion.v3_2_0 ) );
    }

    @Test
    public void versionShouldThrowExceptionIfServerVersionCantBeParsed()
    {
        assertThrows( IllegalArgumentException.class, () -> ServerVersion.version( "" ) );
    }

    @Test
    public void shouldHaveCorrectToString()
    {
        assertEquals( "Neo4j/dev", ServerVersion.vInDev.toString() );
        assertEquals( "Neo4j/3.0.0", ServerVersion.v3_0_0.toString() );
        assertEquals( "Neo4j/3.1.0", ServerVersion.v3_1_0.toString() );
        assertEquals( "Neo4j/3.2.0", ServerVersion.v3_2_0.toString() );
        assertEquals( "Neo4j/3.5.7", ServerVersion.version( "Neo4j/3.5.7" ).toString() );
    }
}
