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
package org.neo4j.driver.v1.tck;

import cucumber.api.CucumberOptions;
import org.junit.jupiter.api.Disabled;

import java.io.IOException;

import org.neo4j.driver.v1.util.Neo4jExtension;

/**
 * The base class to run all cucumber tests
 */
//@RunWith( DriverCucumberAdapter.class )
@CucumberOptions( features = {"target/resources/features"}, strict=true, tags={"~@db", "~@fixed_session_pool"},
        format = {"default_summary"})

@Disabled( "Cucumber does not support JUnit5 right now" )
public class DriverComplianceIT
{
    //@ClassRule
    public static Neo4jExtension neo4j = new Neo4jExtension();

    public DriverComplianceIT() throws IOException
    {
    }

}
