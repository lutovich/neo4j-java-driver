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
package org.neo4j.driver.internal.async.inbound;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.driver.internal.async.ChannelAttributes;
import org.neo4j.driver.internal.messaging.FailureMessage;
import org.neo4j.driver.internal.messaging.IgnoredMessage;
import org.neo4j.driver.internal.messaging.MessageFormat;
import org.neo4j.driver.internal.messaging.MessageFormat.Reader;
import org.neo4j.driver.internal.messaging.PackStreamMessageFormatV1;
import org.neo4j.driver.internal.messaging.RecordMessage;
import org.neo4j.driver.internal.messaging.SuccessMessage;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.Neo4jException;

import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.internal.messaging.ResetMessage.RESET;
import static org.neo4j.driver.internal.util.MessageToByteBufWriter.asByteBuf;
import static org.neo4j.driver.v1.Values.value;

public class InboundMessageHandlerTest
{
    private EmbeddedChannel channel;
    private InboundMessageDispatcher messageDispatcher;

    @BeforeEach
    public void setUp()
    {
        channel = new EmbeddedChannel();
        messageDispatcher = new InboundMessageDispatcher( channel, DEV_NULL_LOGGING );
        ChannelAttributes.setMessageDispatcher( channel, messageDispatcher );

        InboundMessageHandler handler = new InboundMessageHandler( new PackStreamMessageFormatV1(), DEV_NULL_LOGGING );
        channel.pipeline().addFirst( handler );
    }

    @AfterEach
    public void tearDown()
    {
        if ( channel != null )
        {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    public void shouldReadSuccessMessage()
    {
        ResponseHandler responseHandler = mock( ResponseHandler.class );
        messageDispatcher.queue( responseHandler );

        Map<String,Value> metadata = new HashMap<>();
        metadata.put( "key1", value( 1 ) );
        metadata.put( "key2", value( 2 ) );
        channel.writeInbound( asByteBuf( new SuccessMessage( metadata ) ) );

        verify( responseHandler ).onSuccess( metadata );
    }

    @Test
    public void shouldReadFailureMessage()
    {
        ResponseHandler responseHandler = mock( ResponseHandler.class );
        messageDispatcher.queue( responseHandler );

        channel.writeInbound( asByteBuf( new FailureMessage( "Neo.TransientError.General.ReadOnly", "Hi!" ) ) );

        ArgumentCaptor<Neo4jException> captor = ArgumentCaptor.forClass( Neo4jException.class );
        verify( responseHandler ).onFailure( captor.capture() );
        assertEquals( "Neo.TransientError.General.ReadOnly", captor.getValue().code() );
        assertEquals( "Hi!", captor.getValue().getMessage() );
    }

    @Test
    public void shouldReadRecordMessage()
    {
        ResponseHandler responseHandler = mock( ResponseHandler.class );
        messageDispatcher.queue( responseHandler );

        Value[] fields = {value( 1 ), value( 2 ), value( 3 )};
        channel.writeInbound( asByteBuf( new RecordMessage( fields ) ) );

        verify( responseHandler ).onRecord( fields );
    }

    @Test
    public void shouldReadIgnoredMessage()
    {
        ResponseHandler responseHandler = mock( ResponseHandler.class );
        messageDispatcher.queue( responseHandler );

        channel.writeInbound( asByteBuf( new IgnoredMessage() ) );
        assertEquals( 0, messageDispatcher.queuedHandlersCount() );
    }

    @Test
    public void shouldRethrowReadErrors() throws IOException
    {
        MessageFormat messageFormat = mock( MessageFormat.class );
        Reader reader = mock( Reader.class );
        RuntimeException error = new RuntimeException( "Unable to decode!" );
        doThrow( error ).when( reader ).read( any() );
        when( messageFormat.newReader( any() ) ).thenReturn( reader );

        InboundMessageHandler handler = new InboundMessageHandler( messageFormat, DEV_NULL_LOGGING );

        channel.pipeline().remove( InboundMessageHandler.class );
        channel.pipeline().addLast( handler );

        try
        {
            channel.writeInbound( asByteBuf( RESET ) );
            fail( "Exception expected" );
        }
        catch ( DecoderException e )
        {
            assertThat( e.getMessage(), startsWith( "Failed to read inbound message" ) );
        }
    }
}
