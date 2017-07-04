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
package org.neo4j.driver.internal.cluster;

import org.junit.Test;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

import org.neo4j.driver.internal.net.BoltServerAddress;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class AddressSetTest
{
    @Test
    public void shouldPreserveOrderWhenAdding() throws Exception
    {
        // given
        Set<BoltServerAddress> servers = new LinkedHashSet<>( asList(
                new BoltServerAddress( "one" ),
                new BoltServerAddress( "two" ),
                new BoltServerAddress( "tre" ) ) );

        AddressSet set = new AddressSet();
        set.update( servers, new HashSet<BoltServerAddress>() );

        assertArrayEquals( new BoltServerAddress[]{
                new BoltServerAddress( "one" ),
                new BoltServerAddress( "two" ),
                new BoltServerAddress( "tre" )}, set.toArray() );

        // when
        servers.add( new BoltServerAddress( "fyr" ) );
        set.update( servers, new HashSet<BoltServerAddress>() );

        // then
        assertArrayEquals( new BoltServerAddress[]{
                new BoltServerAddress( "one" ),
                new BoltServerAddress( "two" ),
                new BoltServerAddress( "tre" ),
                new BoltServerAddress( "fyr" )}, set.toArray() );
    }

    @Test
    public void shouldPreserveOrderWhenRemoving() throws Exception
    {
        // given
        Set<BoltServerAddress> servers = new LinkedHashSet<>( asList(
                new BoltServerAddress( "one" ),
                new BoltServerAddress( "two" ),
                new BoltServerAddress( "tre" ) ) );
        AddressSet set = new AddressSet();
        set.update( servers, new HashSet<BoltServerAddress>() );

        assertArrayEquals( new BoltServerAddress[]{
                new BoltServerAddress( "one" ),
                new BoltServerAddress( "two" ),
                new BoltServerAddress( "tre" )}, set.toArray() );

        // when
        set.remove( new BoltServerAddress( "one" ) );

        // then
        assertArrayEquals( new BoltServerAddress[]{
                new BoltServerAddress( "two" ),
                new BoltServerAddress( "tre" )}, set.toArray() );
    }

    @Test
    public void shouldPreserveOrderWhenRemovingThroughUpdate() throws Exception
    {
        // given
        Set<BoltServerAddress> servers = new LinkedHashSet<>( asList(
                new BoltServerAddress( "one" ),
                new BoltServerAddress( "two" ),
                new BoltServerAddress( "tre" ) ) );
        AddressSet set = new AddressSet();
        set.update( servers, new HashSet<BoltServerAddress>() );

        assertArrayEquals( new BoltServerAddress[]{
                new BoltServerAddress( "one" ),
                new BoltServerAddress( "two" ),
                new BoltServerAddress( "tre" )}, set.toArray() );

        // when
        servers.remove( new BoltServerAddress( "one" ) );
        set.update( servers, new HashSet<BoltServerAddress>() );

        // then
        assertArrayEquals( new BoltServerAddress[]{
                new BoltServerAddress( "two" ),
                new BoltServerAddress( "tre" )}, set.toArray() );
    }

    @Test
    public void shouldRecordRemovedAddressesWhenUpdating() throws Exception
    {
        // given
        AddressSet set = new AddressSet();
        set.update(
                new HashSet<>( asList(
                        new BoltServerAddress( "one" ),
                        new BoltServerAddress( "two" ),
                        new BoltServerAddress( "tre" ) ) ),
                new HashSet<BoltServerAddress>() );

        // when
        HashSet<BoltServerAddress> removed = new HashSet<>();
        set.update(
                new HashSet<>( asList(
                        new BoltServerAddress( "one" ),
                        new BoltServerAddress( "two" ),
                        new BoltServerAddress( "fyr" ) ) ),
                removed );

        // then
        assertEquals( singleton( new BoltServerAddress( "tre" ) ), removed );
    }
}
