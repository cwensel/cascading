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
import java.util.HashSet;
import java.util.Set;

import cascading.flow.FlowElement;
import cascading.flow.FlowElements;
import cascading.flow.planner.Scope;
import cascading.flow.planner.graph.AnnotatedGraph;
import cascading.flow.planner.graph.ElementGraph;

/**
 *
 */
public class ProcessEdge<Process extends ProcessModel> implements Serializable
  {
  FlowElement flowElement;
  Set<Integer> outgoingOrdinals; // ordinals entering this edge exiting the source process
  Set<Integer> incomingOrdinals; // ordinals exiting the edge into the sink process
  Set<Enum> sinkAnnotations = Collections.emptySet();
  Set<Enum> sourceAnnotations = Collections.emptySet();

  public ProcessEdge( Process sourceProcess, FlowElement flowElement, Process sinkProcess )
    {
    this.flowElement = flowElement;

    ElementGraph sinkElementGraph = sinkProcess.getElementGraph();
    ElementGraph sourceElementGraph = sourceProcess.getElementGraph();

    this.incomingOrdinals = createOrdinals( sinkElementGraph.incomingEdgesOf( flowElement ) );
    this.outgoingOrdinals = createOrdinals( sourceElementGraph.outgoingEdgesOf( flowElement ) );

    if( sinkElementGraph instanceof AnnotatedGraph && ( (AnnotatedGraph) sinkElementGraph ).hasAnnotations() )
      this.sinkAnnotations = ( (AnnotatedGraph) sinkElementGraph ).getAnnotations().getKeysFor( flowElement );

    if( sourceElementGraph instanceof AnnotatedGraph && ( (AnnotatedGraph) sourceElementGraph ).hasAnnotations() )
      this.sourceAnnotations = ( (AnnotatedGraph) sourceElementGraph ).getAnnotations().getKeysFor( flowElement );
    }

  private Set<Integer> createOrdinals( Set<Scope> scopes )
    {
    Set<Integer> ordinals = new HashSet<>();

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

  public Set<Enum> getSinkAnnotations()
    {
    return sinkAnnotations;
    }

  public Set<Enum> getSourceAnnotations()
    {
    return sourceAnnotations;
    }
  }
