/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow.iso;

import java.util.Set;

import cascading.flow.planner.graph.ElementGraphs;
import junit.framework.TestCase;
import org.jgrapht.Graphs;
import org.jgrapht.graph.DirectedSubgraph;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.junit.Test;

/**
 *
 */
public class FindSubGraphTest extends TestCase
  {
  @Test
  public void testFindSubGraph()
    {
    SimpleDirectedGraph<String, Object> full = new SimpleDirectedGraph<>( Object.class );

    full.addVertex( "1" );
    full.addVertex( "0" );
    full.addVertex( "a" );
    full.addVertex( "b" );
    full.addVertex( "c" );
    full.addVertex( "d" );
    full.addVertex( "e" );
    full.addEdge( "a", "b" );
    full.addEdge( "b", "c" );
    full.addEdge( "c", "d" );
    full.addEdge( "c", "e" );
    full.addEdge( "1", "0" );
    full.addEdge( "0", "c" );

    SimpleDirectedGraph<String, Object> contracted = new SimpleDirectedGraph<>( Object.class );

    contracted.addVertex( "1" );
    contracted.addVertex( "a" );
    contracted.addVertex( "c" );
    contracted.addEdge( "a", "c" );
    contracted.addEdge( "1", "c" );

    SimpleDirectedGraph<String, Object> result = new SimpleDirectedGraph<>( Object.class );

    result.addVertex( "1" );
    result.addVertex( "0" );
    result.addVertex( "a" );
    result.addVertex( "b" );
    result.addVertex( "c" );
    result.addEdge( "a", "b", full.getEdge( "a", "b" ) );
    result.addEdge( "b", "c", full.getEdge( "b", "c" ) );
    result.addEdge( "1", "0", full.getEdge( "1", "0" ) );
    result.addEdge( "0", "c", full.getEdge( "0", "c" ) );

    assertGraphs( full, contracted, result );
    }

  @Test
  public void testFindSubGraph2()
    {
    SimpleDirectedGraph<String, Object> full = new SimpleDirectedGraph<>( Object.class );

    full.addVertex( "1" );
    full.addVertex( "0" );
    full.addVertex( "a" );
    full.addVertex( "b" );
    full.addVertex( "c" );
    full.addVertex( "d" );
    full.addVertex( "e" );
    full.addEdge( "a", "b" );
    full.addEdge( "b", "c" );
    full.addEdge( "c", "d" );
    full.addEdge( "c", "e" );
    full.addEdge( "1", "0" );
    full.addEdge( "0", "c" );

    SimpleDirectedGraph<String, Object> contracted = new SimpleDirectedGraph<>( Object.class );

    contracted.addVertex( "a" );
    contracted.addVertex( "c" );
    contracted.addEdge( "a", "c" );

    SimpleDirectedGraph<String, Object> result = new SimpleDirectedGraph<>( Object.class );

    result.addVertex( "a" );
    result.addVertex( "b" );
    result.addVertex( "c" );
    result.addEdge( "a", "b", full.getEdge( "a", "b" ) );
    result.addEdge( "b", "c", full.getEdge( "b", "c" ) );

    assertGraphs( full, contracted, result );
    }

  @Test
  public void testFindSubGraph3()
    {
    SimpleDirectedGraph<String, Object> full = new SimpleDirectedGraph<>( Object.class );

    full.addVertex( "A" );
    full.addVertex( "1" );
    full.addVertex( "0" );
    full.addVertex( "a" );
    full.addVertex( "b" );
    full.addVertex( "c" );
    full.addVertex( "d" );
    full.addVertex( "e" );
    full.addEdge( "A", "a" );
    full.addEdge( "A", "1" );
    full.addEdge( "a", "b" );
    full.addEdge( "b", "c" );
    full.addEdge( "c", "d" );
    full.addEdge( "c", "e" );
    full.addEdge( "1", "0" );
    full.addEdge( "0", "c" );

    SimpleDirectedGraph<String, Object> contracted = new SimpleDirectedGraph<>( Object.class );

    contracted.addVertex( "A" );
    contracted.addVertex( "1" );
    contracted.addVertex( "c" );
    contracted.addEdge( "A", "1" );
    contracted.addEdge( "A", "c" );

    SimpleDirectedGraph<String, Object> result = new SimpleDirectedGraph<>( Object.class );

    result.addVertex( "A" );
    result.addVertex( "1" );
    result.addVertex( "a" );
    result.addVertex( "b" );
    result.addVertex( "c" );
    result.addEdge( "A", "1", full.getEdge( "A", "1" ) );
    result.addEdge( "A", "a", full.getEdge( "A", "a" ) );
    result.addEdge( "a", "b", full.getEdge( "a", "b" ) );
    result.addEdge( "b", "c", full.getEdge( "b", "c" ) );

    assertGraphs( full, contracted, result );
    }

  @Test
  public void testFindSubGraph4()
    {
    SimpleDirectedGraph<String, Object> full = new SimpleDirectedGraph<>( Object.class );

    full.addVertex( "B" );
    full.addVertex( "A" );
    full.addVertex( "2" );
    full.addVertex( "1" );
    full.addVertex( "0" );
    full.addVertex( "a" );
    full.addVertex( "b" );
    full.addVertex( "c" );
    full.addVertex( "d" );
    full.addVertex( "e" );
    full.addEdge( "B", "A" );
    full.addEdge( "A", "a" );
    full.addEdge( "A", "2" );
    full.addEdge( "a", "b" );
    full.addEdge( "b", "c" );
    full.addEdge( "c", "d" );
    full.addEdge( "c", "e" );
    full.addEdge( "2", "1" );
    full.addEdge( "1", "0" );
    full.addEdge( "0", "c" );

    SimpleDirectedGraph<String, Object> contracted = new SimpleDirectedGraph<>( Object.class );

    contracted.addVertex( "B" );
    contracted.addVertex( "1" );
    contracted.addVertex( "c" );
    contracted.addEdge( "1", "c" );
    contracted.addEdge( "B", "c" );

    SimpleDirectedGraph<String, Object> result = new SimpleDirectedGraph<>( Object.class );

    result.addVertex( "B" );
    result.addVertex( "A" );
    result.addVertex( "1" );
    result.addVertex( "0" );
    result.addVertex( "a" );
    result.addVertex( "b" );
    result.addVertex( "c" );
    result.addEdge( "B", "A", full.getEdge( "B", "A" ) );
    result.addEdge( "1", "0", full.getEdge( "1", "0" ) );
    result.addEdge( "0", "c", full.getEdge( "0", "c" ) );
    result.addEdge( "A", "a", full.getEdge( "A", "a" ) );
    result.addEdge( "a", "b", full.getEdge( "a", "b" ) );
    result.addEdge( "b", "c", full.getEdge( "b", "c" ) );

    assertGraphs( full, contracted, result );
    }

  private void assertGraphs( SimpleDirectedGraph<String, Object> full, SimpleDirectedGraph<String, Object> contracted, SimpleDirectedGraph<String, Object> result )
    {
//    Set<String> vertices = ElementGraphs.findClosureViaBiConnected( full, contracted );
    Set<String> vertices = ElementGraphs.findClosureViaFloydWarshall( full, contracted ).getLhs();
//    Set<String> vertices = ElementGraphs.findClosureViaKShortest( full, contracted );

    DirectedSubgraph<String, Object> subgraph = new DirectedSubgraph<>( full, vertices, null );

//    System.out.println( "subgraph = " + subgraph );

    SimpleDirectedGraph<String, Object> clone = new SimpleDirectedGraph<>( Object.class );

    Graphs.addGraph( clone, subgraph );

    assertEquals( result, clone );
    }

  }
