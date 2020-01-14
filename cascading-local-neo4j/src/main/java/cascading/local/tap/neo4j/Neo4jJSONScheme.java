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
import java.util.Properties;

import cascading.flow.FlowProcess;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import com.fasterxml.jackson.databind.JsonNode;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class Neo4jJSONScheme extends Neo4jScheme
  {
  private static final Logger LOG = LoggerFactory.getLogger( Neo4jJSONScheme.class );

  private final JSONGraphSpec graphSpec;

  public Neo4jJSONScheme( Fields sinkFields, JSONGraphSpec graphSpec )
    {
    super( Fields.UNKNOWN, sinkFields );
    this.graphSpec = graphSpec;

    if( !sinkFields.isDeclarator() || sinkFields.size() > 1 )
      throw new IllegalArgumentException( "sink fields must be size one, got: " + sinkFields.print() );
    }

  @Override
  public boolean isSource()
    {
    return false;
    }

  @Override
  public void sourceConfInit( FlowProcess<? extends Properties> flowProcess, Tap<Properties, Void, Session> tap, Properties conf )
    {
    throw new UnsupportedOperationException();
    }

  @Override
  public boolean source( FlowProcess<? extends Properties> flowProcess, SourceCall<Context, Void> sourceCall ) throws IOException
    {
    throw new UnsupportedOperationException();
    }

  @Override
  public void sinkConfInit( FlowProcess<? extends Properties> flowProcess, Tap<Properties, Void, Session> tap, Properties conf )
    {

    }

  @Override
  public void sinkPrepare( FlowProcess<? extends Properties> flowProcess, SinkCall<Context, Session> sinkCall ) throws IOException
    {
    sinkCall.setContext( new Context<>( new Neo4jJSONStatement( graphSpec ) ) );
    }

  @Override
  public void sink( FlowProcess<? extends Properties> flowProcess, SinkCall<Context, Session> sinkCall ) throws IOException
    {
    Session session = sinkCall.getOutput();
    Neo4jStatement<JsonNode> statement = sinkCall.getContext().statement;
    TupleEntry entry = sinkCall.getOutgoingEntry();

    JsonNode node = (JsonNode) entry.getObject( 0 );

    session.writeTransaction( tx ->
    {
    StatementResult result = statement.runStatement( tx, node );

    if( LOG.isDebugEnabled() )
      LOG.debug( "cypher results: {}", result.summary() );

    return true;
    } );
    }
  }
