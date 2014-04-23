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

package cascading.flow.planner.graph;

import java.util.Collection;
import java.util.HashSet;

import cascading.flow.FlowElement;
import cascading.flow.planner.ElementGraphs;
import cascading.flow.planner.Scope;
import org.jgrapht.graph.DirectedSubgraph;

/**
 *
 */
public class ElementSubGraph extends DirectedSubgraph<FlowElement, Scope> implements ElementGraph
  {
  private final ElementGraph elementGraph;
  private final Collection<FlowElement> flowElements;
  private final Collection<Scope> scopes;

  public ElementSubGraph( ElementGraph elementGraph, Collection<FlowElement> flowElements, Collection<Scope> scopes )
    {
    super( elementGraph, new HashSet<>( flowElements ), new HashSet<>( scopes ) );

    this.elementGraph = elementGraph;
    this.flowElements = flowElements;
    this.scopes = scopes;
    }

  public ElementSubGraph( ElementGraph elementGraph, Collection<FlowElement> flowElements )
    {
    super( elementGraph, new HashSet<>( flowElements ), null );

    this.elementGraph = elementGraph;
    this.flowElements = flowElements;
    this.scopes = null;
    }

  public ElementSubGraph( ElementSubGraph graph )
    {
    this( graph.elementGraph, graph.flowElements, graph.scopes );
    }

  @Override
  public ElementGraph copyGraph()
    {
    return new ElementSubGraph( this );
    }

  @Override
  public void writeDOT( String filename )
    {
    ElementGraphs.printElementGraph( filename, this, null );
    }
  }
