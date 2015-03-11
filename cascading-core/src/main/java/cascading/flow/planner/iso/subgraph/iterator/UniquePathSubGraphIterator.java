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

package cascading.flow.planner.iso.subgraph.iterator;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import cascading.flow.FlowElement;
import cascading.flow.planner.Scope;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.graph.ElementSubGraph;
import cascading.flow.planner.iso.ElementAnnotation;
import cascading.flow.planner.iso.subgraph.SubGraphIterator;
import cascading.util.EnumMultiMap;
import org.jgrapht.GraphPath;

import static cascading.flow.planner.graph.ElementGraphs.*;
import static cascading.util.Util.getFirst;
import static org.jgrapht.Graphs.getPathVertexList;

/**
 *
 */
public class UniquePathSubGraphIterator implements SubGraphIterator
  {
  SubGraphIterator subGraphIterator;
  ElementGraph current = null;
  private Iterator<GraphPath<FlowElement, Scope>> pathsIterator;

  public UniquePathSubGraphIterator( SubGraphIterator subGraphIterator )
    {
    this.subGraphIterator = subGraphIterator;
    }

  @Override
  public ElementGraph getElementGraph()
    {
    return subGraphIterator.getElementGraph();
    }

  @Override
  public EnumMultiMap getAnnotationMap( ElementAnnotation[] annotations )
    {
    return subGraphIterator.getAnnotationMap( annotations ); // unsure we need to narrow results
    }

  @Override
  public boolean hasNext()
    {
    if( pathsIterator == null )
      advance();

    if( current == null || pathsIterator == null )
      return false;

    boolean hasNextPath = pathsIterator.hasNext();

    if( hasNextPath )
      return true;

    return subGraphIterator.hasNext();
    }

  private void advance()
    {
    if( current == null )
      {
      if( !subGraphIterator.hasNext() )
        return;

      current = subGraphIterator.next();
      pathsIterator = null;
      }

    if( pathsIterator == null )
      {
      Set<FlowElement> sources = findSources( current, FlowElement.class );
      Set<FlowElement> sinks = findSinks( current, FlowElement.class );

      if( sources.size() > 1 || sinks.size() > 1 )
        throw new IllegalArgumentException( "only supports single source and single sink graphs" );

      pathsIterator = getAllShortestPathsBetween( current, getFirst( sources ), getFirst( sinks ) ).iterator();
      }
    }

  @Override
  public ElementGraph next()
    {
    if( !pathsIterator.hasNext() )
      {
      current = null;
      pathsIterator = null;

      advance();

      return next();
      }

    GraphPath<FlowElement, Scope> path = pathsIterator.next();
    List<FlowElement> vertexList = getPathVertexList( path );
    List<Scope> edgeList = path.getEdgeList();

    return new ElementSubGraph( current, vertexList, edgeList );
    }

  @Override
  public void remove()
    {
    subGraphIterator.remove();
    }
  }
