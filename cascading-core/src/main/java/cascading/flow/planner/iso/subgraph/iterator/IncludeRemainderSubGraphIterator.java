/*
 * Copyright (c) 2016-2017 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 * Copyright (c) 2007-2017 Xplenty, Inc. All Rights Reserved.
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

package cascading.flow.planner.iso.subgraph.iterator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import cascading.flow.FlowElement;
import cascading.flow.planner.Scope;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.graph.ElementGraphs;
import cascading.flow.planner.graph.ElementMaskSubGraph;
import cascading.flow.planner.graph.Extent;
import cascading.flow.planner.iso.ElementAnnotation;
import cascading.flow.planner.iso.subgraph.SubGraphIterator;
import cascading.util.EnumMultiMap;
import cascading.util.Pair;
import org.jgrapht.GraphPath;

import static cascading.flow.planner.graph.ElementGraphs.*;
import static cascading.util.Util.createIdentitySet;

/**
 *
 */
public class IncludeRemainderSubGraphIterator implements SubGraphIterator
  {
  SubGraphIterator parentIterator;
  boolean multiEdge;

  Set<FlowElement> maskedElements = createIdentitySet();
  Set<Scope> maskedScopes = new HashSet<>();

  {
  // creates consistent results across SubGraphIterators
  maskedElements.add( Extent.head );
  maskedElements.add( Extent.tail );
  }

  public IncludeRemainderSubGraphIterator( SubGraphIterator parentIterator, boolean multiEdge )
    {
    this.parentIterator = parentIterator;
    this.multiEdge = multiEdge;
    }

  @Override
  public ElementGraph getElementGraph()
    {
    return parentIterator.getElementGraph();
    }

  @Override
  public EnumMultiMap getAnnotationMap( ElementAnnotation[] annotations )
    {
    return parentIterator.getAnnotationMap( annotations );
    }

  @Override
  public boolean hasNext()
    {
    return parentIterator.hasNext();
    }

  @Override
  public ElementGraph next()
    {
    ElementGraph next = parentIterator.next();

    if( parentIterator.hasNext() )
      {
      // hide these elements from the remainder graph
      maskedElements.addAll( next.vertexSet() );
      maskedScopes.addAll( next.edgeSet() ); // catches case with no elements on path

      return next;
      }

    ElementGraph elementGraph = parentIterator.getElementGraph();

    if( !multiEdge ) // no effect on mergepipes/tez
      {
      maskedElements.removeAll( next.vertexSet() );
      maskedScopes.removeAll( next.edgeSet() );
      }
    else
      {
      // this is experimental, but was intended to allow capture of multiple edges between two
      // nodes
      maskedElements.addAll( next.vertexSet() );
      maskedScopes.addAll( next.edgeSet() );

      // if there is branching in the root graph, common ancestors could be masked out
      // here we iterate all paths for all remaining paths

      for( FlowElement maskedElement : new ArrayList<>( maskedElements ) )
        {
        if( !maskedScopes.containsAll( elementGraph.edgesOf( maskedElement ) ) )
          maskedElements.remove( maskedElement );
        }
      }

    // previously source/sink pairs captured in prior partitions
    Set<Pair<FlowElement, FlowElement>> pairs = getPairs();

    ElementMaskSubGraph maskSubGraph = new ElementMaskSubGraph( elementGraph, maskedElements, maskedScopes );

    // remaining source/sink pairs we need to traverse
    Set<FlowElement> sources = findSources( maskSubGraph, FlowElement.class );
    Set<FlowElement> sinks = findSinks( maskSubGraph, FlowElement.class );

    for( FlowElement source : sources )
      {
      for( FlowElement sink : sinks )
        {
        if( pairs.contains( new Pair<>( source, sink ) ) )
          continue;

        List<GraphPath<FlowElement, Scope>> paths = getAllShortestPathsBetween( elementGraph, source, sink );

        for( GraphPath<FlowElement, Scope> path : paths )
          {
          maskedElements.removeAll( path.getVertexList() );

          Collection<Scope> edgeList = path.getEdgeList();

          if( multiEdge )
            edgeList = ElementGraphs.getAllMultiEdgesBetween( edgeList, elementGraph );

          maskedScopes.removeAll( edgeList );
          }
        }
      }

    // new graph since the prior made a copy of the masked vertices/edges
    return new ElementMaskSubGraph( elementGraph, maskedElements, maskedScopes );
    }

  protected Set<Pair<FlowElement, FlowElement>> getPairs()
    {
    Set<Pair<FlowElement, FlowElement>> pairs = Collections.emptySet();

    if( parentIterator instanceof UniquePathSubGraphIterator )
      pairs = ( (UniquePathSubGraphIterator) parentIterator ).getPairs();

    return pairs;
    }

  @Override
  public void remove()
    {
    parentIterator.remove();
    }
  }
