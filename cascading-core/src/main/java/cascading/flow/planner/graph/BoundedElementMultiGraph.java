/*
 * Copyright (c) 2007-2009 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Cascading is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Cascading is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Cascading.  If not, see <http://www.gnu.org/licenses/>.
 */

package cascading.flow.planner.graph;

import java.util.Set;

import cascading.flow.FlowElement;
import cascading.flow.planner.ElementGraphs;
import cascading.flow.planner.Scope;
import cascading.util.MultiMap;

import static cascading.flow.planner.Extent.head;
import static cascading.flow.planner.Extent.tail;

/**
 *
 */
public class BoundedElementMultiGraph extends ElementMultiGraph
  {
  public BoundedElementMultiGraph( ElementGraph parentElementGraph, ElementGraph subElementGraph, MultiMap annotations )
    {
    copyFrom( subElementGraph );

    addParentAnnotations( parentElementGraph );

    getAnnotations().addAll( annotations );

    bindHeadAndTail( parentElementGraph, subElementGraph );
    }

  public BoundedElementMultiGraph( ElementGraph elementGraph )
    {
    super( elementGraph );
    }

  private void addParentAnnotations( ElementGraph parentElementGraph )
    {
    if( !( parentElementGraph instanceof AnnotatedGraph ) || !( (AnnotatedGraph) parentElementGraph ).hasAnnotations() )
      return;

    Set<FlowElement> vertexSet = vertexSet();

    MultiMap parentAnnotations = ( (AnnotatedGraph) parentElementGraph ).getAnnotations();

    Set<Enum> allKeys = parentAnnotations.getAllKeys();

    for( Enum annotation : allKeys )
      {
      Set<FlowElement> flowElements = parentAnnotations.getValues( annotation );

      for( FlowElement flowElement : flowElements )
        {
        if( vertexSet.contains( flowElement ) )
          getAnnotations().addAll( annotation, flowElements );
        }
      }
    }

  protected void bindHeadAndTail( ElementGraph parentElementGraph, ElementGraph subElementGraph )
    {
    Set<FlowElement> sources = ElementGraphs.findSources( subElementGraph, FlowElement.class );
    Set<FlowElement> sinks = ElementGraphs.findSinks( subElementGraph, FlowElement.class );

    addVertex( head );
    addVertex( tail );

    Set<FlowElement> parentElements = parentElementGraph.vertexSet();

    for( FlowElement source : sources )
      {
      if( !parentElements.contains( source ) )
        continue;

      Set<Scope> scopes = parentElementGraph.incomingEdgesOf( source );

      for( Scope scope : scopes )
        addEdge( head, source, scope );
      }

    for( FlowElement sink : sinks )
      {
      if( !parentElements.contains( sink ) )
        continue;

      Set<Scope> scopes = parentElementGraph.outgoingEdgesOf( sink );

      for( Scope scope : scopes )
        addEdge( sink, tail, scope );
      }
    }
  }
