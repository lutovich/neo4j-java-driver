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
package org.neo4j.driver.v1.util.cc;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.neo4j.driver.internal.util.ServerVersion;
import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.util.Neo4jRunner;
import org.neo4j.driver.v1.util.SimpleParameterResolver;

import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.neo4j.driver.v1.util.Neo4jRunner.TARGET_DIR;
import static org.neo4j.driver.v1.util.cc.CommandLineUtil.boltKitAvailable;

public class ClusterExtension extends SimpleParameterResolver
        implements BeforeAllCallback, AfterAllCallback, BeforeEachCallback, AfterEachCallback
{
    private static final Path CLUSTER_DIR = Paths.get( TARGET_DIR, "test-cluster" ).toAbsolutePath();
    private static final String PASSWORD = "test";
    private static final int INITIAL_PORT = 20_000;

    // todo: should be possible to configure (dynamically add/remove) cores and read replicas
    private static final int CORE_COUNT = 3;
    private static final int READ_REPLICA_COUNT = 2;

    @Override
    public void beforeAll( ExtensionContext context ) throws Exception
    {
        assumeTrue( boltKitAvailable(), "BoltKit cluster support unavailable" );

        stopSingleInstanceDatabase();

        if ( !SharedCluster.exists() )
        {
            SharedCluster.install( parseNeo4jVersion(),
                    CORE_COUNT, READ_REPLICA_COUNT, PASSWORD, INITIAL_PORT, CLUSTER_DIR );

            try
            {
                SharedCluster.start();
            }
            catch ( Throwable startError )
            {
                try
                {
                    SharedCluster.kill();
                }
                catch ( Throwable killError )
                {
                    startError.addSuppressed( killError );
                }
                finally
                {
                    SharedCluster.remove();
                }
                throw startError;
            }
            finally
            {
                addShutdownHookToStopCluster();
            }
        }
    }

    @Override
    public void afterAll( ExtensionContext context )
    {
        stopSharedCluster();
    }

    @Override
    public void beforeEach( ExtensionContext context )
    {
        getCluster().deleteData();
    }

    @Override
    public void afterEach( ExtensionContext context )
    {
        getCluster().startOfflineMembers();
    }

    public Cluster getCluster()
    {
        return SharedCluster.get();
    }

    public AuthToken getDefaultAuthToken()
    {
        return AuthTokens.basic( "neo4j", PASSWORD );
    }

    private static void stopSharedCluster()
    {
        if ( SharedCluster.exists() )
        {
            try
            {
                SharedCluster.stop();
            }
            finally
            {
                SharedCluster.remove();
            }
        }
    }

    private static String parseNeo4jVersion()
    {
        String[] split = Neo4jRunner.NEOCTRL_ARGS.split( "\\s+" );
        String version = split[split.length - 1];
        // if the server version is older than 3.1 series, then ignore the tests
        assumeTrue( ServerVersion.version( version ).greaterThanOrEqual( ServerVersion.v3_1_0 ),
                "Server version `" + version + "` does not support Casual Cluster" );
        return version;
    }

    private static void stopSingleInstanceDatabase() throws IOException
    {
        if ( Neo4jRunner.globalRunnerExists() )
        {
            Neo4jRunner.getOrCreateGlobalRunner().stopNeo4j();
        }
    }

    private static void addShutdownHookToStopCluster()
    {
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            @Override
            public void run()
            {
                try
                {
                    if ( SharedCluster.exists() )
                    {
                        SharedCluster.kill();
                    }
                }
                catch ( Throwable t )
                {
                    System.err.println( "Cluster stopping shutdown hook failed" );
                    t.printStackTrace();
                }
            }
        } );
    }
}
