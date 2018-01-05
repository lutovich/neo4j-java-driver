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
package org.neo4j.driver.internal.async;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.driver.internal.async.BoltProtocolV1Util.BOLT_MAGIC_PREAMBLE;
import static org.neo4j.driver.internal.async.BoltProtocolV1Util.NO_PROTOCOL_VERSION;
import static org.neo4j.driver.internal.async.BoltProtocolV1Util.PROTOCOL_VERSION_1;
import static org.neo4j.driver.internal.async.BoltProtocolV1Util.handshakeBuf;
import static org.neo4j.driver.internal.async.BoltProtocolV1Util.handshakeString;
import static org.neo4j.driver.internal.async.BoltProtocolV1Util.writeChunkHeader;
import static org.neo4j.driver.internal.async.BoltProtocolV1Util.writeEmptyChunkHeader;
import static org.neo4j.driver.internal.async.BoltProtocolV1Util.writeMessageBoundary;
import static org.neo4j.driver.v1.util.TestUtil.assertByteBufContains;

public class BoltProtocolV1UtilTest
{
    @Test
    public void shouldReturnHandshakeBuf()
    {
        assertByteBufContains(
                handshakeBuf(),
                BOLT_MAGIC_PREAMBLE, PROTOCOL_VERSION_1, NO_PROTOCOL_VERSION, NO_PROTOCOL_VERSION, NO_PROTOCOL_VERSION
        );
    }

    @Test
    public void shouldReturnHandshakeString()
    {
        assertEquals( "[0x6060B017, 1, 0, 0, 0]", handshakeString() );
    }

    @Test
    public void shouldWriteMessageBoundary()
    {
        ByteBuf buf = Unpooled.buffer();

        buf.writeInt( 1 );
        buf.writeInt( 2 );
        buf.writeInt( 3 );
        writeMessageBoundary( buf );

        assertByteBufContains( buf, 1, 2, 3, (byte) 0, (byte) 0 );
    }

    @Test
    public void shouldWriteEmptyChunkHeader()
    {
        ByteBuf buf = Unpooled.buffer();

        writeEmptyChunkHeader( buf );
        buf.writeInt( 1 );
        buf.writeInt( 2 );
        buf.writeInt( 3 );

        assertByteBufContains( buf, (byte) 0, (byte) 0, 1, 2, 3 );
    }

    @Test
    public void shouldWriteChunkHeader()
    {
        ByteBuf buf = Unpooled.buffer();

        writeEmptyChunkHeader( buf );
        buf.writeInt( 1 );
        buf.writeInt( 2 );
        buf.writeInt( 3 );
        writeChunkHeader( buf, 0, 42 );

        assertByteBufContains( buf, (short) 42, 1, 2, 3 );
    }
}
