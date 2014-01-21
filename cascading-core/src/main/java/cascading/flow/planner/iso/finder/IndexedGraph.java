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

package cascading.flow.planner.iso.finder;

import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.GraphDelegator;

/**
 *
 */
class IndexedGraph<Graph extends DirectedGraph<Node, Edge>, Node, Edge> extends GraphDelegator
  {
  private Graph delegate;
  private Object[] index;
  private Map<Node, Integer> reverse;

  private Map<Integer, Set<Integer>> successors = new HashMap<>();
  private Map<Integer, Set<Integer>> predecessors = new HashMap<>();
  private int count;
  private Iterator iterator;

  public IndexedGraph( Graph graph )
    {
    this( null, graph );
    }

  public IndexedGraph( SearchOrder searchOrder, Graph graph )
    {
    super( graph );
    delegate = graph;

    index = new Object[ vertexSet().size() ];
    count = -1;
    iterator = getIterator( searchOrder );
    reverse = new IdentityHashMap<>( index.length );
    }

  public Graph getDelegate()
    {
    return delegate;
    }

  public boolean containsEdge( int lhsVertex, int rhsVertex )
    {
    return getDelegate().containsEdge( getVertex( lhsVertex ), getVertex( rhsVertex ) );
    }

  public Set<Integer> getSuccessors( int vertex )
    {
    Set<Integer> results = successors.get( vertex );

    if( results != null )
      return results;

    results = new HashSet<>();

    Set<Edge> edges = getDelegate().outgoingEdgesOf( getVertex( vertex ) );

    for( Edge edge : edges )
      {
      Object result = getEdgeTarget( edge );
      Integer value = getIndex( result );

      if( value != null )
        results.add( value );
      }

    successors.put( vertex, results );

    return results;
    }

  public Set<Integer> getPredecessors( int vertex )
    {
    Set<Integer> results = predecessors.get( vertex );

    if( results != null )
      return results;

    results = new HashSet<>();

    Set<Edge> edges = getDelegate().incomingEdgesOf( getVertex( vertex ) );

    for( Edge edge : edges )
      {
      Object result = getEdgeSource( edge );
      Integer value = getIndex( result );

      if( value != null )
        results.add( value );
      }

    predecessors.put( vertex, results );

    return results;
    }

  private Integer getIndex( Object result )
    {
    Integer index = reverse.get( result );

    if( index != null )
      return index;

    while( iterator.hasNext() )
      {
      if( getVertex( count + 1 ) == result )
        break;
      }

    return count;
    }

  private Iterator<Node> getIterator( SearchOrder searchOrder )
    {
    return SearchOrder.getNodeIterator( searchOrder, getDelegate() );
    }

  public Node getVertex( int vertex )
    {
    while( count < vertex && iterator.hasNext() )
      {
      count++;
      index[ count ] = iterator.next();
      reverse.put( (Node) index[ count ], count );
      }

    return (Node) index[ vertex ];
    }

  public Edge getEdge( int lhsVertex, int rhsVertex )
    {
    return (Edge) getDelegate().getEdge( getVertex( lhsVertex ), getVertex( rhsVertex ) );
    }

  @Override
  public Set<Edge> getAllEdges( Object sourceVertex, Object targetVertex )
    {
    return getDelegate().getAllEdges( getVertex( (int) sourceVertex ), getVertex( ( (int) targetVertex ) ) );
    }
  }
