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

package cascading.flow.planner.process;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import cascading.flow.FlowElement;
import cascading.flow.FlowElements;
import cascading.flow.planner.Scope;
import cascading.flow.planner.graph.AnnotatedGraph;
import cascading.flow.planner.graph.ElementGraph;
import cascading.tuple.Fields;
import cascading.util.Util;

/**
 *
 */
public class ProcessEdge<Process extends ProcessModel> implements Serializable
  {
  FlowElement flowElement;
  Set<Integer> outgoingOrdinals; // ordinals entering this edge exiting the source process
  Set<Integer> incomingOrdinals; // ordinals exiting the edge into the sink process

  Map<Integer, Fields> resolvedKeyFields;
  Map<Integer, Fields> resolvedSortFields;
  Map<Integer, Fields> resolvedValueFields;

  Set<Enum> sinkAnnotations = Collections.emptySet();
  Set<Enum> sourceAnnotations = Collections.emptySet();

  public ProcessEdge( Process sourceProcess, FlowElement flowElement, Process sinkProcess )
    {
    this( sourceProcess.getElementGraph(), flowElement, sinkProcess.getElementGraph() );
    }

  public ProcessEdge( ElementGraph sourceElementGraph, FlowElement flowElement, ElementGraph sinkElementGraph )
    {
    this.flowElement = flowElement;
    this.outgoingOrdinals = createOrdinals( sourceElementGraph.outgoingEdgesOf( flowElement ) );
    this.incomingOrdinals = createOrdinals( sinkElementGraph.incomingEdgesOf( flowElement ) );

    setResolvedFields( sourceElementGraph, flowElement, sinkElementGraph );

    if( sourceElementGraph instanceof AnnotatedGraph && ( (AnnotatedGraph) sourceElementGraph ).hasAnnotations() )
      this.sourceAnnotations = ( (AnnotatedGraph) sourceElementGraph ).getAnnotations().getKeysFor( flowElement );

    if( sinkElementGraph instanceof AnnotatedGraph && ( (AnnotatedGraph) sinkElementGraph ).hasAnnotations() )
      this.sinkAnnotations = ( (AnnotatedGraph) sinkElementGraph ).getAnnotations().getKeysFor( flowElement );
    }

  private void setResolvedFields( ElementGraph sourceElementGraph, FlowElement flowElement, ElementGraph sinkElementGraph )
    {
    Set<Scope> outgoingScopes = sourceElementGraph.outgoingEdgesOf( flowElement ); // resolved fields
    Scope resolvedScope = Util.getFirst( outgoingScopes ); // only need first

    Set<Scope> incomingScopes = sinkElementGraph.incomingEdgesOf( flowElement );

    resolvedKeyFields = new HashMap<>();
    resolvedSortFields = new HashMap<>();
    resolvedValueFields = new HashMap<>();

    for( Scope incomingScope : incomingScopes )
      {
      int ordinal = incomingScope.getOrdinal();

      if( resolvedScope.getKeySelectors() != null )
        {
        Fields value = resolvedScope.getKeySelectors().get( incomingScope.getName() );

        if( value != null )
          resolvedKeyFields.put( ordinal, value );
        }

      if( resolvedScope.getSortingSelectors() != null )
        {
        Fields value = resolvedScope.getSortingSelectors().get( incomingScope.getName() );

        if( value != null )
          resolvedSortFields.put( ordinal, value );
        }

      resolvedValueFields.put( ordinal, incomingScope.getIncomingSpliceFields() );
      }
    }

  private Set<Integer> createOrdinals( Set<Scope> scopes )
    {
    Set<Integer> ordinals = new TreeSet<>();

    for( Scope scope : scopes )
      ordinals.add( scope.getOrdinal() );

    return ordinals;
    }

  public FlowElement getFlowElement()
    {
    return flowElement;
    }

  public String getID()
    {
    return FlowElements.id( flowElement );
    }

  public Set<Integer> getIncomingOrdinals()
    {
    return incomingOrdinals;
    }

  public Set<Integer> getOutgoingOrdinals()
    {
    return outgoingOrdinals;
    }

  public Map<Integer, Fields> getResolvedKeyFields()
    {
    return resolvedKeyFields;
    }

  public Map<Integer, Fields> getResolvedSortFields()
    {
    return resolvedSortFields;
    }

  public Map<Integer, Fields> getResolvedValueFields()
    {
    return resolvedValueFields;
    }

  public Set<Enum> getSinkAnnotations()
    {
    return sinkAnnotations;
    }

  public Set<Enum> getSourceAnnotations()
    {
    return sourceAnnotations;
    }
  }
