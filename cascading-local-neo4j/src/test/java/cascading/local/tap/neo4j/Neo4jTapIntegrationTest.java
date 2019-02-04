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

import java.net.URI;
import java.util.Properties;

import cascading.flow.FlowProcess;
import cascading.nested.json.JSONCoercibleType;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import org.junit.Test;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class Neo4jTapIntegrationTest extends Neo4jIntegrationTestCase
  {
  public static String[] values = new String[]
    {
      "{\"trace_id\":\"e2366267f86db8f4\", \"id\": 1, \"description\":\"Start\", \"tag\":{\"duration\":0, \"earliest_start\":0, \"earliest_finish\":0, \"latest_start\":0, \"latest_finish\":0}}",
      "{\"trace_id\":\"e2366267f86db8f4\", \"parent_id\": 1, \"id\": 2, \"description\":\"Perform needs analysis\", \"tag\":{\"duration\":10}}",
      "{\"trace_id\":\"e2366267f86db8f4\", \"parent_id\": 2, \"id\": 3, \"description\":\"Perform dependency analysis\", \"annotations\":[{\"timestamp\":1548968531962000,\"value\":\"ws\"},{\"timestamp\":1548968531963000,\"value\":\"wr\"}], \"tag\":{\"duration\":10}}",
      "{\"trace_id\":\"e2366267f86db8f4\", \"parent_id\": 3, \"id\": 4, \"description\":\"More analysis\", \"tag\":{\"role/version\": \"1.1.0\"}}"
    };

  @Test
  public void write() throws Exception
    {
    URI uri = URI.create( "bolt://" + getContainerHost() );

    Properties properties = new Properties();

    JSONGraphSpec graphSpec = new JSONGraphSpec( "Span" );

    graphSpec
      .addProperty( "trace_id", "/trace_id", null )
      .addProperty( "id", "/id", null );

    graphSpec
      .addEdge( "TRACE" )
      .addTargetLabel( "Trace" )
      .addTargetProperty( "trace_id", "/trace_id", null );

    graphSpec
      .addEdge( "PARENT" )
      .addTargetLabel( "Span" )
      .addTargetProperty( "id", "/parent_id", null );

    Fields json = new Fields( "json", JSONCoercibleType.TYPE );
    Neo4jScheme scheme = new Neo4jJSONScheme( json, graphSpec );
    Neo4jTap output = new Neo4jTap( properties, scheme, uri );

    try( TupleEntryCollector collector = output.openForWrite( FlowProcess.nullFlowProcess() ) )
      {
      for( String value : values )
        {
        collector.add( new Tuple( JSONCoercibleType.TYPE.canonical( value ) ) );
        }
      }

    Driver driver = GraphDatabase.driver( uri );

    Integer result;
    try( Session session = driver.session() )
      {
      result = session.writeTransaction( tx ->
        tx.run( "match (n:Span) return count(n)" )
          .single()
          .get( 0 )
          .asInt() );
      }

    assertEquals( 4, result.intValue() );
    }
  }
