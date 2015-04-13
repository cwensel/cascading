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

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import cascading.flow.FlowElement;
import cascading.flow.planner.Scope;
import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DirectedSubgraph;

import static cascading.flow.planner.graph.ElementGraphs.directed;
import static cascading.util.Util.createIdentitySet;

/**
 *
 */
public class ElementSubGraph extends BaseElementGraph implements ElementGraph
  {
  private final ElementGraph elementGraph;
  private final Collection<FlowElement> flowElements;
  private final Collection<Scope> scopes;

  public ElementSubGraph( ElementGraph elementGraph, Collection<FlowElement> flowElements, Collection<Scope> scopes )
    {
    this.graph = new DirectedSubGraph( directed( elementGraph ), createIdentitySet( flowElements ), createIdentitySet( scopes ) );

    this.elementGraph = elementGraph;
    this.flowElements = flowElements;
    this.scopes = scopes;
    }

  public ElementSubGraph( ElementGraph elementGraph, Collection<FlowElement> flowElements )
    {
    this.graph = new DirectedSubGraph( directed( elementGraph ), new HashSet<>( flowElements ), null );

    this.elementGraph = elementGraph;
    this.flowElements = flowElements;
    this.scopes = null;
    }

  public ElementSubGraph( ElementSubGraph graph )
    {
    this( graph.elementGraph, graph.flowElements, graph.scopes );
    }

  @Override
  public ElementGraph copyElementGraph()
    {
    return new ElementSubGraph( this );
    }

  private class DirectedSubGraph extends DirectedSubgraph<FlowElement, Scope>
    {
    public DirectedSubGraph( DirectedGraph<FlowElement, Scope> base, Set<FlowElement> vertexSubset, Set<Scope> edgeSubset )
      {
      super( base, vertexSubset, edgeSubset );
      }
    }
  }
