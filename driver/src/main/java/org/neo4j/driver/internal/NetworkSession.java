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

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.driver.internal.async.InternalStatementResultCursor;
import org.neo4j.driver.internal.async.QueryRunner;
import org.neo4j.driver.internal.logging.DelegatingLogger;
import org.neo4j.driver.internal.retry.RetryLogic;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionProvider;
import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.Logging;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.StatementResultCursor;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.TransactionWork;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.types.TypeSystem;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.neo4j.driver.internal.util.Futures.failedFuture;
import static org.neo4j.driver.internal.util.Futures.getBlocking;
import static org.neo4j.driver.v1.Values.value;

public class NetworkSession implements Session
{
    private static final String LOG_NAME = "Session";

    private final ConnectionProvider connectionProvider;
    private final AccessMode mode;
    private final RetryLogic retryLogic;
    protected final Logger logger;

    private volatile Bookmark bookmark = Bookmark.empty();
    private volatile InternalStatementResultCursor resultCursor;

    private volatile CompletionStage<ExplicitTransaction> transactionStage = completedFuture( null );
    private volatile CompletionStage<Connection> connectionStage = completedFuture( null );

    private final AtomicBoolean open = new AtomicBoolean( true );

    public NetworkSession( ConnectionProvider connectionProvider, AccessMode mode, RetryLogic retryLogic,
            Logging logging )
    {
        this.connectionProvider = connectionProvider;
        this.mode = mode;
        this.retryLogic = retryLogic;
        this.logger = new DelegatingLogger( logging.getLog( LOG_NAME ), String.valueOf( hashCode() ) );
    }

    @Override
    public StatementResult run( String statementText )
    {
        return run( statementText, Values.EmptyMap );
    }

    @Override
    public StatementResultCursor runAsync( String statementText )
    {
        return runAsync( statementText, Values.EmptyMap );
    }

    @Override
    public StatementResult run( String statementText, Map<String,Object> statementParameters )
    {
        Value params = statementParameters == null ? Values.EmptyMap : value( statementParameters );
        return run( statementText, params );
    }

    @Override
    public StatementResultCursor runAsync( String statementText, Map<String,Object> statementParameters )
    {
        Value params = statementParameters == null ? Values.EmptyMap : value( statementParameters );
        return runAsync( statementText, params );
    }

    @Override
    public StatementResult run( String statementTemplate, Record statementParameters )
    {
        Value params = statementParameters == null ? Values.EmptyMap : value( statementParameters.asMap() );
        return run( statementTemplate, params );
    }

    @Override
    public StatementResultCursor runAsync( String statementTemplate, Record statementParameters )
    {
        Value params = statementParameters == null ? Values.EmptyMap : value( statementParameters.asMap() );
        return runAsync( statementTemplate, params );
    }

    @Override
    public StatementResult run( String statementText, Value statementParameters )
    {
        return run( new Statement( statementText, statementParameters ) );
    }

    @Override
    public StatementResultCursor runAsync( String statementText, Value parameters )
    {
        return runAsync( new Statement( statementText, parameters ) );
    }

    @Override
    public StatementResult run( Statement statement )
    {
        StatementResultCursor cursor = runAsync( statement, false );
        return new InternalStatementResult( cursor );
    }

    @Override
    public StatementResultCursor runAsync( Statement statement )
    {
        return runAsync( statement, true );
    }

    @Override
    public boolean isOpen()
    {
        return open.get();
    }

    @Override
    public void close()
    {
        getBlocking( closeAsync() );
    }

    @Override
    public CompletionStage<Void> closeAsync()
    {
        if ( open.compareAndSet( true, false ) )
        {
            return waitForPreviousResultAndConsumeFailure()
                    .thenCompose( error -> releaseResources().thenApply( ignore ->
            {
                Throwable queryError = Futures.completionErrorCause( error );
                if ( queryError != null )
                {
                    // connection has been acquired and there is an unconsumed error in result cursor
                    throw new CompletionException( queryError );
                }
                else
                {
                    // either connection acquisition failed or
                    // there are no unconsumed errors in the result cursor
                    return null;
                }
            } ) );
        }
        return completedFuture( null );
    }

    @Override
    public Transaction beginTransaction()
    {
        return getBlocking( beginTransactionAsync( mode ) );
    }

    @Deprecated
    @Override
    public Transaction beginTransaction( String bookmark )
    {
        setBookmark( Bookmark.from( bookmark ) );
        return beginTransaction();
    }

    @Override
    public CompletionStage<Transaction> beginTransactionAsync()
    {
        //noinspection unchecked
        return (CompletionStage) beginTransactionAsync( mode );
    }

    @Override
    public <T> T readTransaction( TransactionWork<T> work )
    {
        return transaction( AccessMode.READ, work );
    }

    @Override
    public <T> CompletionStage<T> readTransactionAsync( TransactionWork<CompletionStage<T>> work )
    {
        return transactionAsync( AccessMode.READ, work );
    }

    @Override
    public <T> T writeTransaction( TransactionWork<T> work )
    {
        return transaction( AccessMode.WRITE, work );
    }

    @Override
    public <T> CompletionStage<T> writeTransactionAsync( TransactionWork<CompletionStage<T>> work )
    {
        return transactionAsync( AccessMode.WRITE, work );
    }

    void setBookmark( Bookmark bookmark )
    {
        if ( bookmark != null && !bookmark.isEmpty() )
        {
            this.bookmark = bookmark;
        }
    }

    @Override
    public String lastBookmark()
    {
        return bookmark == null ? null : bookmark.maxBookmarkAsString();
    }

    @Override
    public void reset()
    {
        getBlocking( resetAsync() );
    }

    private CompletionStage<Void> resetAsync()
    {
        return existingTransactionOrNull().thenAccept( tx ->
        {
            if ( tx != null )
            {
                tx.markTerminated();
            }
        } ).thenCompose( ignore -> releaseConnection() );
    }

    @Override
    public TypeSystem typeSystem()
    {
        return InternalTypeSystem.TYPE_SYSTEM;
    }

    CompletionStage<Boolean> currentConnectionIsOpen()
    {
        if ( connectionStage == null )
        {
            return completedFuture( false );
        }
        return connectionStage.handle( ( connection, error ) ->
                error == null && // no acquisition error
                connection != null && // some connection has actually been acquired
                connection.isOpen() ); // and it's still open
    }

    private <T> T transaction( AccessMode mode, TransactionWork<T> work )
    {
        // use different code path compared to async so that work is executed in the caller thread
        // caller thread will also be the one who sleeps between retries;
        // it is unsafe to execute retries in the event loop threads because this can cause a deadlock
        // event loop thread will bock and wait for itself to read some data
        return retryLogic.retry( () ->
        {
            try ( Transaction tx = getBlocking( beginTransactionAsync( mode ) ) )
            {
                try
                {
                    T result = work.execute( tx );
                    tx.success();
                    return result;
                }
                catch ( Throwable t )
                {
                    // mark transaction for failure if the given unit of work threw exception
                    // this will override any success marks that were made by the unit of work
                    tx.failure();
                    throw t;
                }
            }
        } );
    }

    private <T> CompletionStage<T> transactionAsync( AccessMode mode, TransactionWork<CompletionStage<T>> work )
    {
        return retryLogic.retryAsync( () ->
        {
            CompletableFuture<T> resultFuture = new CompletableFuture<>();
            CompletionStage<ExplicitTransaction> txFuture = beginTransactionAsync( mode );

            txFuture.whenComplete( ( tx, completionError ) ->
            {
                Throwable error = Futures.completionErrorCause( completionError );
                if ( error != null )
                {
                    resultFuture.completeExceptionally( error );
                }
                else
                {
                    executeWork( resultFuture, tx, work );
                }
            } );

            return resultFuture;
        } );
    }

    private <T> void executeWork( CompletableFuture<T> resultFuture, ExplicitTransaction tx,
            TransactionWork<CompletionStage<T>> work )
    {
        CompletionStage<T> workFuture = safeExecuteWork( tx, work );
        workFuture.whenComplete( ( result, completionError ) ->
        {
            Throwable error = Futures.completionErrorCause( completionError );
            if ( error != null )
            {
                rollbackTxAfterFailedTransactionWork( tx, resultFuture, error );
            }
            else
            {
                closeTxAfterSucceededTransactionWork( tx, resultFuture, result );
            }
        } );
    }

    private <T> CompletionStage<T> safeExecuteWork( ExplicitTransaction tx, TransactionWork<CompletionStage<T>> work )
    {
        // given work might fail in both async and sync way
        // async failure will result in a failed future being returned
        // sync failure will result in an exception being thrown
        try
        {
            return work.execute( tx );
        }
        catch ( Throwable workError )
        {
            // work threw an exception, wrap it in a future and proceed
            return failedFuture( workError );
        }
    }

    private <T> void rollbackTxAfterFailedTransactionWork( ExplicitTransaction tx, CompletableFuture<T> resultFuture,
            Throwable error )
    {
        if ( tx.isOpen() )
        {
            tx.rollbackAsync().whenComplete( ( ignore, rollbackError ) ->
            {
                if ( rollbackError != null )
                {
                    error.addSuppressed( rollbackError );
                }
                resultFuture.completeExceptionally( error );
            } );
        }
        else
        {
            resultFuture.completeExceptionally( error );
        }
    }

    private <T> void closeTxAfterSucceededTransactionWork( ExplicitTransaction tx, CompletableFuture<T> resultFuture,
            T result )
    {
        if ( tx.isOpen() )
        {
            tx.success();
            tx.closeAsync().whenComplete( ( ignore, completionError ) ->
            {
                Throwable commitError = Futures.completionErrorCause( completionError );
                if ( commitError != null )
                {
                    resultFuture.completeExceptionally( commitError );
                }
                else
                {
                    resultFuture.complete( result );
                }
            } );
        }
        else
        {
            resultFuture.complete( result );
        }
    }

    private InternalStatementResultCursor runAsync( Statement statement, boolean async )
    {
        ensureSessionIsOpen();

        CompletionStage<Connection> connectionStage = ensureNoOpenTxBeforeRunningQuery()
                .thenCompose( ignore -> acquireConnection( mode ) );

        resultCursor = async ? QueryRunner.runAsAsync( connectionStage, statement )
                             : QueryRunner.runAsBlocking( connectionStage, statement );

        return resultCursor;
    }

    private CompletionStage<ExplicitTransaction> beginTransactionAsync( AccessMode mode )
    {
        ensureSessionIsOpen();

        transactionStage = ensureNoOpenTxBeforeStartingTx()
                .thenCompose( ignore -> acquireConnection( mode ) )
                .thenCompose( connection ->
                {
                    ExplicitTransaction tx = new ExplicitTransaction( connection, NetworkSession.this );
                    return tx.beginAsync( bookmark );
                } );

        return transactionStage;
    }

    private CompletionStage<Connection> acquireConnection( AccessMode mode )
    {
        CompletionStage<Connection> currentConnectionStage = connectionStage;

        CompletionStage<Connection> newConnectionStage = waitForPreviousResultAndConsumeFailure().thenCompose( error ->
        {
            if ( error == null )
            {
                // there is no unconsumed error, so one of the following is true:
                //   1) this is first time connection is acquired in this session
                //   2) previous result has been successful and is fully consumed
                //   3) previous result failed and error has been consumed

                // return existing connection, which should've been released back to the pool by now
                return currentConnectionStage;
            }
            else
            {
                // there exists unconsumed error, re-throw it
                throw new CompletionException( error );
            }
        } ).thenCompose( existingConnection ->
        {
            if ( existingConnection != null && existingConnection.isOpen() )
            {
                // there somehow is an existing open connection, this should not happen, just a precondition
                throw new IllegalStateException( "Existing open connection detected" );
            }
            return connectionProvider.acquireConnection( mode );
        } );

        connectionStage = newConnectionStage.exceptionally( error -> null );

        return newConnectionStage;
    }

    private CompletionStage<Void> releaseResources()
    {
        return rollbackTransaction().thenCompose( ignore -> releaseConnection() );
    }

    private CompletionStage<Void> rollbackTransaction()
    {
        return existingTransactionOrNull().thenCompose( tx ->
        {
            if ( tx != null )
            {
                return tx.rollbackAsync();
            }
            return completedFuture( null );
        } ).exceptionally( error ->
        {
            Throwable cause = Futures.completionErrorCause( error );
            logger.warn( "Active transaction rolled back with an error", cause );
            return null;
        } );
    }

    private CompletionStage<Void> releaseConnection()
    {
        return existingConnectionOrNull().thenCompose( connection ->
        {
            if ( connection != null )
            {
                return connection.release();
            }
            return completedFuture( null );
        } );
    }

    private CompletionStage<Void> ensureNoOpenTxBeforeRunningQuery()
    {
        return ensureNoOpenTx( "Statements cannot be run directly on a session with an open transaction; " +
                               "either run from within the transaction or use a different session." );
    }

    private CompletionStage<Void> ensureNoOpenTxBeforeStartingTx()
    {
        return ensureNoOpenTx( "You cannot begin a transaction on a session with an open transaction; " +
                               "either run from within the transaction or use a different session." );
    }

    private CompletionStage<Void> ensureNoOpenTx( String errorMessage )
    {
        return existingTransactionOrNull().thenAccept( tx ->
        {
            if ( tx != null )
            {
                throw new ClientException( errorMessage );
            }
        } );
    }

    private CompletionStage<ExplicitTransaction> existingTransactionOrNull()
    {
        return transactionStage
                .exceptionally( error -> null ) // handle previous acquisition failures
                .thenApply( tx -> tx != null && tx.isOpen() ? tx : null );
    }

    private CompletionStage<Connection> existingConnectionOrNull()
    {
        return connectionStage.exceptionally( error -> null ); // handle previous acquisition failures
    }

    private CompletionStage<Throwable> waitForPreviousResultAndConsumeFailure()
    {
        // make sure previous result is fully consumed and connection is released back to the pool
        return resultCursor == null ? completedFuture( null )
                                    : resultCursor.failureAsync();
    }

    private void ensureSessionIsOpen()
    {
        if ( !open.get() )
        {
            throw new ClientException(
                    "No more interaction with this session are allowed as the current session is already closed. " );
        }
    }
}
