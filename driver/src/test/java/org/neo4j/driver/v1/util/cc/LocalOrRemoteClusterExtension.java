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

import org.junit.jupiter.api.extension.ExtensionContext;

import java.net.URI;

import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.AuthTokens;

import static org.neo4j.driver.internal.DriverFactory.BOLT_ROUTING_URI_SCHEME;

public class LocalOrRemoteClusterExtension extends ClusterExtension
{
    private static final String CLUSTER_URI_SYSTEM_PROPERTY_NAME = "externalClusterUri";
    private static final String NEO4J_USER_PASSWORD_PROPERTY_NAME = "neo4jUserPassword";

    private URI clusterUri;

    public LocalOrRemoteClusterExtension()
    {
        assertValidSystemPropertiesDefined();
    }

    public URI getClusterUri()
    {
        return clusterUri;
    }

    public AuthToken getAuthToken()
    {
        if ( externalClusterExists() )
        {
            return AuthTokens.basic( "neo4j", neo4jUserPasswordFromSystemProperty() );
        }
        return getDefaultAuthToken();
    }

    @Override
    public void beforeAll( ExtensionContext context ) throws Exception
    {
        if ( externalClusterExists() )
        {
            clusterUri = externalClusterUriFromSystemProperty();
        }
        else
        {
            super.beforeAll( context );
            clusterUri = getCluster().leader().getRoutingUri();
        }
    }

    @Override
    public void afterAll( ExtensionContext context )
    {
        if ( !externalClusterExists() )
        {
            super.afterAll( context );
        }
    }

    @Override
    public void beforeEach( ExtensionContext context )
    {
        super.beforeEach( context );
    }

    @Override
    public void afterEach( ExtensionContext context )
    {
        if ( !externalClusterExists() )
        {
            super.afterEach( context );
        }
    }

    private static void assertValidSystemPropertiesDefined()
    {
        URI uri = externalClusterUriFromSystemProperty();
        String password = neo4jUserPasswordFromSystemProperty();
        if ( (uri != null && password == null) || (uri == null && password != null) )
        {
            throw new IllegalStateException(
                    "Both cluster uri and 'neo4j' user password system properties should be set. " +
                    "Uri: '" + uri + "', Password: '" + password + "'" );
        }
        if ( uri != null && !BOLT_ROUTING_URI_SCHEME.equals( uri.getScheme() ) )
        {
            throw new IllegalStateException( "Cluster uri should have bolt+routing scheme: '" + uri + "'" );
        }
    }

    private static boolean externalClusterExists()
    {
        return externalClusterUriFromSystemProperty() != null;
    }

    private static URI externalClusterUriFromSystemProperty()
    {
        String uri = System.getProperty( CLUSTER_URI_SYSTEM_PROPERTY_NAME );
        return uri == null ? null : URI.create( uri );
    }

    private static String neo4jUserPasswordFromSystemProperty()
    {
        return System.getProperty( NEO4J_USER_PASSWORD_PROPERTY_NAME );
    }
}
