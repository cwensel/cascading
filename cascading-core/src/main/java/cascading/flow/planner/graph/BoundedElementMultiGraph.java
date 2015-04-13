/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow.planner.graph;

import java.util.Set;

import cascading.flow.FlowElement;
import cascading.flow.planner.Scope;
import cascading.util.EnumMultiMap;

import static cascading.flow.planner.graph.ElementGraphs.directed;
import static cascading.flow.planner.graph.Extent.head;
import static cascading.flow.planner.graph.Extent.tail;

/**
 *
 */
public class BoundedElementMultiGraph extends ElementMultiGraph
  {
  public BoundedElementMultiGraph( ElementGraph parentElementGraph, ElementGraph subElementGraph, EnumMultiMap annotations )
    {
    graph = new DirectedMultiGraph( directed( subElementGraph ) );

    addParentAnnotations( parentElementGraph );

    getAnnotations().addAll( annotations );

    bindHeadAndTail( parentElementGraph, subElementGraph );
    }

  @Override
  protected void addParentAnnotations( ElementGraph parentElementGraph )
    {
    if( !( parentElementGraph instanceof AnnotatedGraph ) || !( (AnnotatedGraph) parentElementGraph ).hasAnnotations() )
      return;

    Set<FlowElement> vertexSet = vertexSet();

    EnumMultiMap parentAnnotations = ( (AnnotatedGraph) parentElementGraph ).getAnnotations();

    Set<Enum> allKeys = parentAnnotations.getKeys();

    for( Enum annotation : allKeys )
      {
      Set<FlowElement> flowElements = (Set<FlowElement>) parentAnnotations.getValues( annotation );

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
