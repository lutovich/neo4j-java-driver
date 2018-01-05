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
package org.neo4j.driver.v1.integration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.driver.internal.cluster.RoutingSettings;
import org.neo4j.driver.internal.retry.RetrySettings;
import org.neo4j.driver.internal.util.ChannelTrackingDriverFactory;
import org.neo4j.driver.internal.util.FailingConnectionDriverFactory;
import org.neo4j.driver.internal.util.FakeClock;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.StatementResultCursor;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.TransactionWork;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.exceptions.SessionExpiredException;
import org.neo4j.driver.v1.exceptions.TransientException;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.neo4j.driver.v1.util.Function;
import org.neo4j.driver.v1.util.cc.Cluster;
import org.neo4j.driver.v1.util.cc.ClusterExtension;
import org.neo4j.driver.v1.util.cc.ClusterMember;
import org.neo4j.driver.v1.util.cc.ClusterMemberRole;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.internal.util.Matchers.connectionAcquisitionTimeoutError;
import static org.neo4j.driver.v1.Values.parameters;
import static org.neo4j.driver.v1.util.TestUtil.await;

@ExtendWith( ClusterExtension.class )
public class CausalClusteringIT
{
    private static final long DEFAULT_TIMEOUT_MS = 120_000;

    private ClusterExtension clusterExtension;

    @BeforeEach
    void setUp( ClusterExtension clusterExtension )
    {
        this.clusterExtension = clusterExtension;
    }

    @Test
    public void shouldExecuteReadAndWritesWhenDriverSuppliedWithAddressOfLeader() throws Exception
    {
        Cluster cluster = clusterExtension.getCluster();

        int count = executeWriteAndReadThroughBolt( cluster.leader() );

        assertEquals( 1, count );
    }

    @Test
    public void shouldExecuteReadAndWritesWhenRouterIsDiscovered() throws Exception
    {
        Cluster cluster = clusterExtension.getCluster();

        int count = executeWriteAndReadThroughBoltOnFirstAvailableAddress( cluster.anyReadReplica(), cluster.leader() );

        assertEquals( 1, count );
    }

    @Test
    public void shouldExecuteReadAndWritesWhenDriverSuppliedWithAddressOfFollower() throws Exception
    {
        Cluster cluster = clusterExtension.getCluster();

        int count = executeWriteAndReadThroughBolt( cluster.anyFollower() );

        assertEquals( 1, count );
    }

    @Test
    public void sessionCreationShouldFailIfCallingDiscoveryProcedureOnEdgeServer() throws Exception
    {
        Cluster cluster = clusterExtension.getCluster();

        ClusterMember readReplica = cluster.anyReadReplica();
        try
        {
            createDriver( readReplica.getRoutingUri() );
            fail( "Should have thrown an exception using a read replica address for routing" );
        }
        catch ( ServiceUnavailableException ex )
        {
            assertThat( ex.getMessage(), containsString( "Failed to run 'CALL dbms.cluster.routing" ) );
        }
    }

    // Ensure that Bookmarks work with single instances using a driver created using a bolt[not+routing] URI.
    @Test
    public void bookmarksShouldWorkWithDriverPinnedToSingleServer() throws Exception
    {
        Cluster cluster = clusterExtension.getCluster();
        ClusterMember leader = cluster.leader();

        try ( Driver driver = createDriver( leader.getBoltUri() ) )
        {
            String bookmark = inExpirableSession( driver, createSession(), new Function<Session,String>()
            {
                @Override
                public String apply( Session session )
                {
                    try ( Transaction tx = session.beginTransaction() )
                    {
                        tx.run( "CREATE (p:Person {name: {name} })", Values.parameters( "name", "Alistair" ) );
                        tx.success();
                    }

                    return session.lastBookmark();
                }
            } );

            assertNotNull( bookmark );

            try ( Session session = driver.session( bookmark );
                  Transaction tx = session.beginTransaction() )
            {
                Record record = tx.run( "MATCH (n:Person) RETURN COUNT(*) AS count" ).next();
                assertEquals( 1, record.get( "count" ).asInt() );
                tx.success();
            }
        }
    }

    @Test
    public void shouldUseBookmarkFromAReadSessionInAWriteSession() throws Exception
    {
        Cluster cluster = clusterExtension.getCluster();
        ClusterMember leader = cluster.leader();

        try ( Driver driver = createDriver( leader.getBoltUri() ) )
        {
            inExpirableSession( driver, createWritableSession( null ), new Function<Session,Void>()
            {
                @Override
                public Void apply( Session session )
                {
                    session.run( "CREATE (p:Person {name: {name} })", Values.parameters( "name", "Jim" ) );
                    return null;
                }
            } );

            final String bookmark;
            try ( Session session = driver.session( AccessMode.READ ) )
            {
                try ( Transaction tx = session.beginTransaction() )
                {
                    tx.run( "MATCH (n:Person) RETURN COUNT(*) AS count" ).next();
                    tx.success();
                }

                bookmark = session.lastBookmark();
            }

            assertNotNull( bookmark );

            inExpirableSession( driver, createWritableSession( bookmark ), new Function<Session,Void>()
            {
                @Override
                public Void apply( Session session )
                {
                    try ( Transaction tx = session.beginTransaction() )
                    {
                        tx.run( "CREATE (p:Person {name: {name} })", Values.parameters( "name", "Alistair" ) );
                        tx.success();
                    }

                    return null;
                }
            } );

            try ( Session session = driver.session() )
            {
                Record record = session.run( "MATCH (n:Person) RETURN COUNT(*) AS count" ).next();
                assertEquals( 2, record.get( "count" ).asInt() );
            }
        }
    }

    @Test
    public void shouldDropBrokenOldSessions() throws Exception
    {
        Cluster cluster = clusterExtension.getCluster();

        int concurrentSessionsCount = 9;
        int livenessCheckTimeoutMinutes = 2;

        Config config = Config.build()
                .withConnectionLivenessCheckTimeout( livenessCheckTimeoutMinutes, TimeUnit.MINUTES )
                .withoutEncryption()
                .toConfig();

        FakeClock clock = new FakeClock();
        ChannelTrackingDriverFactory driverFactory = new ChannelTrackingDriverFactory( clock );

        URI routingUri = cluster.leader().getRoutingUri();
        AuthToken auth = clusterExtension.getDefaultAuthToken();
        RoutingSettings routingSettings = new RoutingSettings( 1, SECONDS.toMillis( 5 ), null );
        RetrySettings retrySettings = RetrySettings.DEFAULT;

        try ( Driver driver = driverFactory.newInstance( routingUri, auth, routingSettings, retrySettings, config ) )
        {
            // create nodes in different threads using different sessions
            createNodesInDifferentThreads( concurrentSessionsCount, driver );

            // now pool contains many sessions, make them all invalid
            driverFactory.closeChannels();
            // move clock forward more than configured liveness check timeout
            clock.progress( TimeUnit.MINUTES.toMillis( livenessCheckTimeoutMinutes + 1 ) );

            // now all idle connections should be considered too old and will be verified during acquisition
            // they will appear broken because they were closed and new valid connection will be created
            try ( Session session = driver.session( AccessMode.WRITE ) )
            {
                List<Record> records = session.run( "MATCH (n) RETURN count(n)" ).list();
                assertEquals( 1, records.size() );
                assertEquals( concurrentSessionsCount, records.get( 0 ).get( 0 ).asInt() );
            }
        }
    }

    @Test
    public void beginTransactionThrowsForInvalidBookmark()
    {
        String invalidBookmark = "hi, this is an invalid bookmark";
        ClusterMember leader = clusterExtension.getCluster().leader();

        try ( Driver driver = createDriver( leader.getBoltUri() );
              Session session = driver.session( invalidBookmark ) )
        {
            try
            {
                session.beginTransaction();
                fail( "Exception expected" );
            }
            catch ( Exception e )
            {
                assertThat( e, instanceOf( ClientException.class ) );
                assertThat( e.getMessage(), containsString( invalidBookmark ) );
            }
        }
    }

    @SuppressWarnings( "deprecation" )
    @Test
    public void beginTransactionThrowsForUnreachableBookmark()
    {
        ClusterMember leader = clusterExtension.getCluster().leader();

        try ( Driver driver = createDriver( leader.getBoltUri() );
              Session session = driver.session() )
        {
            try ( Transaction tx = session.beginTransaction() )
            {
                tx.run( "CREATE ()" );
                tx.success();
            }

            String bookmark = session.lastBookmark();
            assertNotNull( bookmark );
            String newBookmark = bookmark + "0";

            try
            {
                session.beginTransaction( newBookmark );
                fail( "Exception expected" );
            }
            catch ( Exception e )
            {
                assertThat( e, instanceOf( TransientException.class ) );
                assertThat( e.getMessage(), startsWith( "Database not up to the requested version" ) );
            }
        }
    }

    @Test
    public void shouldHandleGracefulLeaderSwitch() throws Exception
    {
        Cluster cluster = clusterExtension.getCluster();
        ClusterMember leader = cluster.leader();

        try ( Driver driver = createDriver( leader.getRoutingUri() ) )
        {
            Session session1 = driver.session();
            Transaction tx1 = session1.beginTransaction();

            // gracefully stop current leader to force re-election
            cluster.stop( leader );

            tx1.run( "CREATE (person:Person {name: {name}, title: {title}})",
                    parameters( "name", "Webber", "title", "Mr" ) );
            tx1.success();

            closeAndExpectException( tx1, SessionExpiredException.class );
            session1.close();

            String bookmark = inExpirableSession( driver, createSession(), new Function<Session,String>()
            {
                @Override
                public String apply( Session session )
                {
                    try ( Transaction tx = session.beginTransaction() )
                    {
                        tx.run( "CREATE (person:Person {name: {name}, title: {title}})",
                                parameters( "name", "Webber", "title", "Mr" ) );
                        tx.success();
                    }
                    return session.lastBookmark();
                }
            } );

            try ( Session session2 = driver.session( AccessMode.READ, bookmark );
                  Transaction tx2 = session2.beginTransaction() )
            {
                Record record = tx2.run( "MATCH (n:Person) RETURN COUNT(*) AS count" ).next();
                tx2.success();
                assertEquals( 1, record.get( "count" ).asInt() );
            }
        }
    }

    @Test
    public void shouldNotServeWritesWhenMajorityOfCoresAreDead() throws Exception
    {
        Cluster cluster = clusterExtension.getCluster();
        ClusterMember leader = cluster.leader();

        try ( Driver driver = createDriver( leader.getRoutingUri() ) )
        {
            for ( ClusterMember follower : cluster.followers() )
            {
                cluster.kill( follower );
            }
            awaitLeaderToStepDown( driver );

            // now we should be unable to write because majority of cores is down
            for ( int i = 0; i < 10; i++ )
            {
                try ( Session session = driver.session( AccessMode.WRITE ) )
                {
                    session.run( "CREATE (p:Person {name: 'Gamora'})" ).consume();
                    fail( "Exception expected" );
                }
                catch ( Exception e )
                {
                    assertThat( e, instanceOf( SessionExpiredException.class ) );
                }
            }
        }
    }

    @Test
    public void shouldServeReadsWhenMajorityOfCoresAreDead() throws Exception
    {
        Cluster cluster = clusterExtension.getCluster();
        ClusterMember leader = cluster.leader();

        try ( Driver driver = createDriver( leader.getRoutingUri() ) )
        {
            String bookmark;
            try ( Session session = driver.session() )
            {
                int writeResult = session.writeTransaction( new TransactionWork<Integer>()
                {
                    @Override
                    public Integer execute( Transaction tx )
                    {
                        StatementResult result = tx.run( "CREATE (:Person {name: 'Star Lord'}) RETURN 42" );
                        return result.single().get( 0 ).asInt();
                    }
                } );

                assertEquals( 42, writeResult );
                bookmark = session.lastBookmark();
            }

            ensureNodeVisible( cluster, "Star Lord", bookmark );

            for ( ClusterMember follower : cluster.followers() )
            {
                cluster.kill( follower );
            }
            awaitLeaderToStepDown( driver );

            // now we should be unable to write because majority of cores is down
            try ( Session session = driver.session( AccessMode.WRITE ) )
            {
                session.run( "CREATE (p:Person {name: 'Gamora'})" ).consume();
                fail( "Exception expected" );
            }
            catch ( Exception e )
            {
                assertThat( e, instanceOf( SessionExpiredException.class ) );
            }

            // but we should be able to read from the remaining core or read replicas
            try ( Session session = driver.session() )
            {
                int count = session.readTransaction( new TransactionWork<Integer>()
                {
                    @Override
                    public Integer execute( Transaction tx )
                    {
                        StatementResult result = tx.run( "MATCH (:Person {name: 'Star Lord'}) RETURN count(*)" );
                        return result.single().get( 0 ).asInt();
                    }
                } );

                assertEquals( 1, count );
            }
        }
    }

    @Test
    public void shouldAcceptMultipleBookmarks() throws Exception
    {
        int threadCount = 5;
        String label = "Person";
        String property = "name";
        String value = "Alice";

        Cluster cluster = clusterExtension.getCluster();
        ClusterMember leader = cluster.leader();
        ExecutorService executor = Executors.newCachedThreadPool();

        try ( Driver driver = createDriver( leader.getRoutingUri() ) )
        {
            List<Future<String>> futures = new ArrayList<>();
            for ( int i = 0; i < threadCount; i++ )
            {
                futures.add( executor.submit( createNodeAndGetBookmark( driver, label, property, value ) ) );
            }

            List<String> bookmarks = new ArrayList<>();
            for ( Future<String> future : futures )
            {
                bookmarks.add( future.get( 10, SECONDS ) );
            }

            executor.shutdown();
            assertTrue( executor.awaitTermination( 5, SECONDS ) );

            try ( Session session = driver.session( AccessMode.READ, bookmarks ) )
            {
                int count = countNodes( session, label, property, value );
                assertEquals( count, threadCount );
            }
        }
    }

    @Test
    public void shouldNotReuseReadConnectionForWriteTransaction()
    {
        Cluster cluster = clusterExtension.getCluster();
        ClusterMember leader = cluster.leader();

        try ( Driver driver = createDriver( leader.getRoutingUri() ) )
        {
            Session session = driver.session( AccessMode.READ );

            CompletionStage<List<RecordAndSummary>> resultsStage = session.runAsync( "RETURN 42" )
                    .thenCompose( cursor1 ->
                            session.writeTransactionAsync( tx -> tx.runAsync( "CREATE (:Node1) RETURN 42" ) )
                                    .thenCompose( cursor2 -> combineCursors( cursor2, cursor1 ) ) );

            List<RecordAndSummary> results = await( resultsStage );
            assertEquals( 2, results.size() );

            RecordAndSummary first = results.get( 0 );
            RecordAndSummary second = results.get( 1 );

            // both auto-commit query and write tx should return 42
            assertEquals( 42, first.record.get( 0 ).asInt() );
            assertEquals( first.record, second.record );
            // they should not use same server
            assertNotEquals( first.summary.server().address(), second.summary.server().address() );

            CompletionStage<Integer> countStage =
                    session.readTransaction( tx -> tx.runAsync( "MATCH (n:Node1) RETURN count(n)" )
                            .thenCompose( StatementResultCursor::singleAsync ) )
                            .thenApply( record -> record.get( 0 ).asInt() );

            assertEquals( 1, await( countStage ).intValue() );

            await( session.closeAsync() );
        }
    }

    @Test
    public void shouldRespectMaxConnectionPoolSizePerClusterMember()
    {
        Cluster cluster = clusterExtension.getCluster();
        ClusterMember leader = cluster.leader();

        Config config = Config.build()
                .withMaxConnectionPoolSize( 2 )
                .withConnectionAcquisitionTimeout( 42, MILLISECONDS )
                .withLogging( DEV_NULL_LOGGING )
                .toConfig();

        try ( Driver driver = createDriver( leader.getRoutingUri(), config ) )
        {
            Session writeSession1 = driver.session( AccessMode.WRITE );
            writeSession1.beginTransaction();

            Session writeSession2 = driver.session( AccessMode.WRITE );
            writeSession2.beginTransaction();

            // should not be possible to acquire more connections towards leader because limit is 2
            Session writeSession3 = driver.session( AccessMode.WRITE );
            try
            {
                writeSession3.beginTransaction();
                fail( "Exception expected" );
            }
            catch ( ClientException e )
            {
                assertThat( e, is( connectionAcquisitionTimeoutError( 42 ) ) );
            }

            // should be possible to acquire new connection towards read server
            // it's a different machine, not leader, so different max connection pool size limit applies
            Session readSession = driver.session( AccessMode.READ );
            Record record = readSession.readTransaction( tx -> tx.run( "RETURN 1" ).single() );
            assertEquals( 1, record.get( 0 ).asInt() );
        }
    }

    @Test
    public void shouldAllowExistingTransactionToCompleteAfterDifferentConnectionBreaks()
    {
        Cluster cluster = clusterExtension.getCluster();
        ClusterMember leader = cluster.leader();

        FailingConnectionDriverFactory driverFactory = new FailingConnectionDriverFactory();
        RoutingSettings routingSettings = new RoutingSettings( 1, SECONDS.toMillis( 5 ), null );
        Config config = Config.build().toConfig();

        try ( Driver driver = driverFactory.newInstance( leader.getRoutingUri(), clusterExtension.getDefaultAuthToken(),
                routingSettings, RetrySettings.DEFAULT, config ) )
        {
            Session session1 = driver.session();
            Transaction tx1 = session1.beginTransaction();
            tx1.run( "CREATE (n:Node1 {name: 'Node1'})" ).consume();

            Session session2 = driver.session();
            Transaction tx2 = session2.beginTransaction();
            tx2.run( "CREATE (n:Node2 {name: 'Node2'})" ).consume();

            ServiceUnavailableException error = new ServiceUnavailableException( "Connection broke!" );
            driverFactory.setNextRunFailure( error );
            assertUnableToRunMoreStatementsInTx( tx2, error );

            closeTx( tx2 );
            closeTx( tx1 );

            try ( Session session3 = driver.session( session1.lastBookmark() ) )
            {
                // tx1 should not be terminated and should commit successfully
                assertEquals( 1, countNodes( session3, "Node1", "name", "Node1" ) );
                // tx2 should not commit because of a connection failure
                assertEquals( 0, countNodes( session3, "Node2", "name", "Node2" ) );
            }

            // rediscovery should happen for the new write query
            String session4Bookmark = createNodeAndGetBookmark( driver.session(), "Node3", "name", "Node3" );
            try ( Session session5 = driver.session( session4Bookmark ) )
            {
                assertEquals( 1, countNodes( session5, "Node3", "name", "Node3" ) );
            }
        }
    }

    private static void closeTx( Transaction tx )
    {
        tx.success();
        tx.close();
    }

    private static void assertUnableToRunMoreStatementsInTx( Transaction tx, ServiceUnavailableException cause )
    {
        try
        {
            tx.run( "CREATE (n:Node3 {name: 'Node3'})" ).consume();
            fail( "Exception expected" );
        }
        catch ( SessionExpiredException e )
        {
            assertEquals( cause, e.getCause() );
        }
    }

    private CompletionStage<List<RecordAndSummary>> combineCursors( StatementResultCursor cursor1,
            StatementResultCursor cursor2 )
    {
        return buildRecordAndSummary( cursor1 ).thenCombine( buildRecordAndSummary( cursor2 ),
                ( rs1, rs2 ) -> Arrays.asList( rs1, rs2 ) );
    }

    private CompletionStage<RecordAndSummary> buildRecordAndSummary( StatementResultCursor cursor )
    {
        return cursor.singleAsync().thenCompose( record ->
                cursor.summaryAsync().thenApply( summary -> new RecordAndSummary( record, summary ) ) );
    }

    private CompletionStage<Integer> sumRecordsFrom( StatementResultCursor cursor1, StatementResultCursor cursor2 )
    {
        return cursor1.singleAsync().thenCombine( cursor2.singleAsync(),
                ( record1, record2 ) -> record1.get( 0 ).asInt() + record2.get( 0 ).asInt() );
    }

    private int executeWriteAndReadThroughBolt( ClusterMember member ) throws TimeoutException, InterruptedException
    {
        try ( Driver driver = createDriver( member.getRoutingUri() ) )
        {
            return inExpirableSession( driver, createWritableSession( null ), executeWriteAndRead() );
        }
    }

    private int executeWriteAndReadThroughBoltOnFirstAvailableAddress( ClusterMember... members ) throws TimeoutException, InterruptedException
    {
        List<URI> addresses = new ArrayList<>( members.length );
        for ( ClusterMember member : members )
        {
            addresses.add( member.getRoutingUri() );
        }
        try ( Driver driver = discoverDriver( addresses ) )
        {
            return inExpirableSession( driver, createWritableSession( null ), executeWriteAndRead() );
        }
    }

    private Function<Driver,Session> createSession()
    {
        return new Function<Driver,Session>()
        {
            @Override
            public Session apply( Driver driver )
            {
                return driver.session();
            }
        };
    }

    private Function<Driver,Session> createWritableSession( final String bookmark )
    {
        return new Function<Driver,Session>()
        {
            @Override
            public Session apply( Driver driver )
            {
                return driver.session( AccessMode.WRITE, bookmark );
            }
        };
    }

    private Function<Session,Integer> executeWriteAndRead()
    {
        return new Function<Session,Integer>()
        {
            @Override
            public Integer apply( Session session )
            {
                session.run( "MERGE (n:Person {name: 'Jim'})" ).consume();
                Record record = session.run( "MATCH (n:Person) RETURN COUNT(*) AS count" ).next();
                return record.get( "count" ).asInt();
            }
        };
    }

    private <T> T inExpirableSession( Driver driver, Function<Driver,Session> acquirer, Function<Session,T> op )
            throws TimeoutException, InterruptedException
    {
        long endTime = System.currentTimeMillis() + DEFAULT_TIMEOUT_MS;

        do
        {
            try ( Session session = acquirer.apply( driver ) )
            {
                return op.apply( session );
            }
            catch ( SessionExpiredException | ServiceUnavailableException e )
            {
                // role might have changed; try again;
            }

            Thread.sleep( 500 );
        }
        while ( System.currentTimeMillis() < endTime );

        throw new TimeoutException( "Transaction did not succeed in time" );
    }

    private void ensureNodeVisible( Cluster cluster, String name, String bookmark )
    {
        for ( ClusterMember member : cluster.members() )
        {
            int count = countNodesUsingDirectDriver( member.getBoltUri(), name, bookmark );
            assertEquals( 1, count );
        }
    }

    private int countNodesUsingDirectDriver( URI boltUri, final String name, String bookmark )
    {
        try ( Driver driver = createDriver( boltUri );
              Session session = driver.session( bookmark ) )
        {
            return session.readTransaction( new TransactionWork<Integer>()
            {
                @Override
                public Integer execute( Transaction tx )
                {
                    StatementResult result = tx.run( "MATCH (:Person {name: {name}}) RETURN count(*)",
                            parameters( "name", name ) );
                    return result.single().get( 0 ).asInt();
                }
            } );
        }
    }

    private void awaitLeaderToStepDown( Driver driver )
    {
        int leadersCount;
        int followersCount;
        int readReplicasCount;
        do
        {
            try ( Session session = driver.session() )
            {
                int newLeadersCount = 0;
                int newFollowersCount = 0;
                int newReadReplicasCount = 0;
                for ( Record record : session.run( "CALL dbms.cluster.overview()" ).list() )
                {
                    ClusterMemberRole role = ClusterMemberRole.valueOf( record.get( "role" ).asString() );
                    if ( role == ClusterMemberRole.LEADER )
                    {
                        newLeadersCount++;
                    }
                    else if ( role == ClusterMemberRole.FOLLOWER )
                    {
                        newFollowersCount++;
                    }
                    else if ( role == ClusterMemberRole.READ_REPLICA )
                    {
                        newReadReplicasCount++;
                    }
                    else
                    {
                        throw new AssertionError( "Unknown role: " + role );
                    }
                }
                leadersCount = newLeadersCount;
                followersCount = newFollowersCount;
                readReplicasCount = newReadReplicasCount;
            }
        }
        while ( !(leadersCount == 0 && followersCount == 1 && readReplicasCount == 2) );
    }

    private Driver createDriver( URI boltUri )
    {
        Config config = Config.build()
                .withLogging( DEV_NULL_LOGGING )
                .toConfig();

        return createDriver( boltUri, config );
    }

    private Driver createDriver( URI boltUri, Config config )
    {
        return GraphDatabase.driver( boltUri, clusterExtension.getDefaultAuthToken(), config );
    }

    private Driver discoverDriver( List<URI> routingUris )
    {
        Config config = Config.build()
                .withLogging( DEV_NULL_LOGGING )
                .toConfig();

        return GraphDatabase.routingDriver( routingUris, clusterExtension.getDefaultAuthToken(), config );
    }

    private static void createNodesInDifferentThreads( int count, final Driver driver ) throws Exception
    {
        final CountDownLatch beforeRunLatch = new CountDownLatch( count );
        final CountDownLatch runQueryLatch = new CountDownLatch( 1 );
        final ExecutorService executor = Executors.newCachedThreadPool();

        for ( int i = 0; i < count; i++ )
        {
            executor.submit( new Callable<Void>()
            {
                @Override
                public Void call() throws Exception
                {
                    beforeRunLatch.countDown();
                    try ( Session session = driver.session( AccessMode.WRITE ) )
                    {
                        runQueryLatch.await();
                        session.run( "CREATE ()" );
                    }
                    return null;
                }
            } );
        }

        beforeRunLatch.await();
        runQueryLatch.countDown();

        executor.shutdown();
        assertTrue( executor.awaitTermination( 1, TimeUnit.MINUTES ) );
    }

    private static void closeAndExpectException( AutoCloseable closeable, Class<? extends Exception> exceptionClass )
    {
        try
        {
            closeable.close();
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( exceptionClass ) );
        }
    }

    private static int countNodes( Session session, String label, String property, String value )
    {
        return session.readTransaction( tx ->
        {
            String query = "MATCH (n:" + label + " {" + property + ": $value}) RETURN count(n)";
            StatementResult result = tx.run( query, parameters( "value", value ) );
            return result.single().get( 0 ).asInt();
        } );
    }

    private static Callable<String> createNodeAndGetBookmark( Driver driver, String label, String property,
            String value )
    {
        return () -> createNodeAndGetBookmark( driver.session(), label, property, value );
    }

    private static String createNodeAndGetBookmark( Session session, String label, String property, String value )
    {
        try ( Session localSession = session )
        {
            localSession.writeTransaction( tx ->
            {
                tx.run( "CREATE (n:" + label + ") SET n." + property + " = $value", parameters( "value", value ) );
                return null;
            } );
            return localSession.lastBookmark();
        }
    }

    private static class RecordAndSummary
    {
        final Record record;
        final ResultSummary summary;

        RecordAndSummary( Record record, ResultSummary summary )
        {
            this.record = record;
            this.summary = summary;
        }
    }
}
