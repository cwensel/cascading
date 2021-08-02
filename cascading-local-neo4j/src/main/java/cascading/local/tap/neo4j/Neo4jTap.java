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

import java.io.IOException;
import java.net.URI;
import java.util.Properties;

import cascading.flow.FlowProcess;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import cascading.tuple.TupleEntrySchemeCollector;
import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;

/**
 *
 */
public class Neo4jTap extends Tap<Properties, Void, Session>
  {
  public static final String NEO_4_J_USERNAME = "neo4j.username";
  public static final String NEO_4_J_PASSWORD = "neo4j.password";

  private final Properties defaultProperties;
  private final URI identifier;

  public Neo4jTap( Properties defaultProperties, Neo4jScheme scheme, URI identifier )
    {
    super( scheme, SinkMode.UPDATE );

    this.defaultProperties = new Properties( defaultProperties );
    this.identifier = identifier;
    }

  protected Driver getDriver( FlowProcess<? extends Properties> flowProcess )
    {
    AuthToken authTokens = AuthTokens.none();

    String user = flowProcess.getStringProperty( NEO_4_J_USERNAME, defaultProperties.getProperty( NEO_4_J_USERNAME, System.getenv( "NEO4J_USER" ) ) );
    String password = flowProcess.getStringProperty( NEO_4_J_PASSWORD, defaultProperties.getProperty( NEO_4_J_PASSWORD, System.getenv( "NEO4J_PASSWORD" ) ) );

    if( user != null )
      authTokens = AuthTokens.basic( user, password );

    return GraphDatabase.driver( getIdentifier(), authTokens );
    }

  @Override
  public String getIdentifier()
    {
    return identifier.toString();
    }

  @Override
  public TupleEntryIterator openForRead( FlowProcess<? extends Properties> flowProcess, Void aVoid ) throws IOException
    {
    return null;
    }

  @Override
  public TupleEntryCollector openForWrite( FlowProcess<? extends Properties> flowProcess, Session session ) throws IOException
    {
    if( session == null )
      session = getDriver( flowProcess ).session();

    return new TupleEntrySchemeCollector<>( flowProcess, this, getScheme(), session, getIdentifier() );
    }

  @Override
  public boolean createResource( Properties conf ) throws IOException
    {
    return true;
    }

  @Override
  public boolean deleteResource( Properties conf ) throws IOException
    {
    return true;
    }

  @Override
  public boolean resourceExists( Properties conf ) throws IOException
    {
    return true;
    }

  @Override
  public long getModifiedTime( Properties conf ) throws IOException
    {
    return 0;
    }
  }
