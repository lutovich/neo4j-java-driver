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
package org.neo4j.driver.internal.async;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.ReplayingDecoder;

import java.util.List;
import javax.net.ssl.SSLHandshakeException;

import org.neo4j.driver.internal.logging.ChannelActivityLogger;
import org.neo4j.driver.internal.messaging.MessageFormat;
import org.neo4j.driver.internal.messaging.PackStreamMessageFormatV1;
import org.neo4j.driver.internal.messaging.PackStreamMessageFormatV2;
import org.neo4j.driver.internal.util.ErrorUtil;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.Logging;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.SecurityException;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;

import static org.neo4j.driver.internal.async.BoltProtocolUtil.HTTP;
import static org.neo4j.driver.internal.async.BoltProtocolUtil.NO_PROTOCOL_VERSION;
import static org.neo4j.driver.internal.async.BoltProtocolUtil.PROTOCOL_VERSION_1;
import static org.neo4j.driver.internal.async.BoltProtocolUtil.PROTOCOL_VERSION_2;

public class HandshakeHandler extends ReplayingDecoder<Void>
{
    private final ChannelPipelineBuilder pipelineBuilder;
    private final ChannelPromise handshakeCompletedPromise;
    private final Logging logging;

    private boolean failed;
    private Logger log;

    public HandshakeHandler( ChannelPipelineBuilder pipelineBuilder, ChannelPromise handshakeCompletedPromise,
            Logging logging )
    {
        this.pipelineBuilder = pipelineBuilder;
        this.handshakeCompletedPromise = handshakeCompletedPromise;
        this.logging = logging;
    }

    @Override
    public void handlerAdded( ChannelHandlerContext ctx )
    {
        log = new ChannelActivityLogger( ctx.channel(), logging, getClass() );
    }

    @Override
    protected void handlerRemoved0( ChannelHandlerContext ctx )
    {
        failed = false;
        log = null;
    }

    @Override
    public void channelInactive( ChannelHandlerContext ctx )
    {
        log.debug( "Channel is inactive" );

        if ( !failed )
        {
            // channel became inactive while doing bolt handshake, not because of some previous error
            ServiceUnavailableException error = ErrorUtil.newConnectionTerminatedError();
            fail( ctx, error );
        }
    }

    @Override
    public void exceptionCaught( ChannelHandlerContext ctx, Throwable error )
    {
        if ( failed )
        {
            log.warn( "Another fatal error occurred in the pipeline", error );
        }
        else
        {
            failed = true;
            Throwable cause = transformError( error );
            fail( ctx, cause );
        }
    }

    @Override
    protected void decode( ChannelHandlerContext ctx, ByteBuf in, List<Object> out )
    {
        int serverSuggestedVersion = in.readInt();
        log.debug( "S: [Bolt Handshake] {}", serverSuggestedVersion );

        ChannelPipeline pipeline = ctx.pipeline();
        // this is a one-time handler, remove it when protocol version has been read
        pipeline.remove( this );

        switch ( serverSuggestedVersion )
        {
        case PROTOCOL_VERSION_1:
            protocolSelected( new PackStreamMessageFormatV1(), pipeline );
            break;
        case PROTOCOL_VERSION_2:
            protocolSelected( new PackStreamMessageFormatV2(), pipeline );
            break;
        case NO_PROTOCOL_VERSION:
            fail( ctx, protocolNoSupportedByServerError() );
            break;
        case HTTP:
            fail( ctx, httpEndpointError() );
            break;
        default:
            fail( ctx, protocolNoSupportedByDriverError( serverSuggestedVersion ) );
            break;
        }
    }

    private void protocolSelected( MessageFormat messageFormat, ChannelPipeline pipeline )
    {
        pipelineBuilder.build( messageFormat, pipeline, logging );
        handshakeCompletedPromise.setSuccess();
    }

    private void fail( ChannelHandlerContext ctx, Throwable error )
    {
        ctx.close().addListener( future -> handshakeCompletedPromise.tryFailure( error ) );
    }

    private static Throwable protocolNoSupportedByServerError()
    {
        return new ClientException( "The server does not support any of the protocol versions supported by " +
                                    "this driver. Ensure that you are using driver and server versions that " +
                                    "are compatible with one another." );
    }

    private static Throwable httpEndpointError()
    {
        return new ClientException(
                "Server responded HTTP. Make sure you are not trying to connect to the http endpoint " +
                "(HTTP defaults to port 7474 whereas BOLT defaults to port 7687)" );
    }

    private static Throwable protocolNoSupportedByDriverError( int suggestedProtocolVersion )
    {
        return new ClientException(
                "Protocol error, server suggested unexpected protocol version: " + suggestedProtocolVersion );
    }

    private static Throwable transformError( Throwable error )
    {
        Throwable cause = error instanceof DecoderException ? error.getCause() : error;
        if ( cause instanceof ServiceUnavailableException )
        {
            return cause;
        }
        else if ( cause instanceof SSLHandshakeException )
        {
            return new SecurityException( "Failed to establish secured connection with the server", cause );
        }
        else
        {
            return new ServiceUnavailableException( "Failed to establish connection with the server", cause );
        }
    }
}
