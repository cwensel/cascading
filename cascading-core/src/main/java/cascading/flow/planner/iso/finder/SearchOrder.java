/*
 * Copyright (c) 2007-2016 Concurrent, Inc. All Rights Reserved.
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

import java.util.Iterator;

import cascading.flow.planner.graph.Extent;
import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.EdgeReversedGraph;
import org.jgrapht.traverse.BreadthFirstIterator;
import org.jgrapht.traverse.DepthFirstIterator;
import org.jgrapht.traverse.TopologicalOrderIterator;

/**
 *
 */
public enum SearchOrder
  {
    Depth, Breadth, Topological, ReverseDepth( true ), ReverseBreadth( true ), ReverseTopological( true );

  final boolean isReversed;

  SearchOrder()
    {
    isReversed = false;
    }

  SearchOrder( boolean isReversed )
    {
    this.isReversed = isReversed;
    }

  public boolean isReversed()
    {
    return isReversed;
    }

  public static <Node, Graph extends DirectedGraph> Iterator<Node> getNodeIterator( SearchOrder searchOrder, Graph graph )
    {
    if( searchOrder == null )
      return new TopologicalOrderIterator( graph ); // faster than getVertexSet().iterator()

    Node node = null;

    if( graph.containsVertex( Extent.head ) )
      {
      if( !searchOrder.isReversed() )
        node = (Node) Extent.head;
      else
        node = (Node) Extent.tail;
      }

    switch( searchOrder )
      {
      case Depth:
        return new DepthFirstIterator( graph, node );
      case Breadth:
        return new BreadthFirstIterator( graph, node );
      case Topological:
        return new TopologicalOrderIterator( graph ); // TODO: uses a equality based hashmap internally, will fail if relying on identity
      case ReverseDepth:
        return new DepthFirstIterator( new EdgeReversedGraph( graph ), node );
      case ReverseBreadth:
        return new BreadthFirstIterator( new EdgeReversedGraph( graph ), node );
      case ReverseTopological:
        return new TopologicalOrderIterator( new EdgeReversedGraph( graph ) ); // TODO: uses a equality based hashmap internally, will fail if relying on identity
      }

    throw new IllegalStateException( "unknown order: " + searchOrder );
    }
  }
