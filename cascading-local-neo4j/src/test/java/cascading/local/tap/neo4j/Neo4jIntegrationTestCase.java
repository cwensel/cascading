/*
 * Copyright (c) 2018-2019 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
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

package cascading.local.tap.neo4j;

import java.time.Duration;

import org.junit.ClassRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

/**
 *
 */
public abstract class Neo4jIntegrationTestCase
  {
  private static final Logger LOG = LoggerFactory.getLogger( Neo4jIntegrationTestCase.class );

  public static final int PORTAL_PORT = 7474;
  public static final int BOLT_PORT = 7687;

  @ClassRule
  public static GenericContainer neo4jRule =
    new GenericContainer( "neo4j:3.5.2" )
      .withExposedPorts( PORTAL_PORT, BOLT_PORT )
      .withLogConsumer( new Slf4jLogConsumer( LOG ) )
      .withEnv( "NEO4J_AUTH", "none" )
      .withStartupTimeout( Duration.ofMinutes( 3 ) );

  protected String getContainerHost()
    {
    return String.format( "%s:%d", neo4jRule.getContainerIpAddress(), neo4jRule.getMappedPort( 7687 ) );
    }
  }
