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

package cascading.flow.planner.graph;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;

import cascading.flow.FlowElement;
import cascading.flow.planner.Scope;
import org.jgrapht.Graph;
import org.jgrapht.graph.MaskSubgraph;

import static cascading.flow.planner.graph.ElementGraphs.directed;
import static cascading.util.Util.createIdentitySet;

/**
 *
 */
public class ElementMaskSubGraph extends BaseElementGraph implements ElementGraph
  {
  private ElementGraph elementGraph;
  private VertexMask vertexMask;
  private EdgeMask edgeMask;

  private static class VertexMask implements Predicate<FlowElement>
    {
    Set<FlowElement> maskedElements = createIdentitySet();

    public VertexMask( Collection<FlowElement> flowElements )
      {
      if( flowElements != null )
        maskedElements.addAll( flowElements );
      }

    @Override
    public boolean test( FlowElement flowElement )
      {
      return maskedElements.contains( flowElement );
      }
    }

  private static class EdgeMask implements Predicate<Scope>
    {
    Set<Scope> maskedScopes = new HashSet<>();

    public EdgeMask( Collection<Scope> scopes )
      {
      if( scopes != null )
        maskedScopes.addAll( scopes );
      }

    @Override
    public boolean test( Scope scope )
      {
      return maskedScopes.contains( scope );
      }
    }

  public ElementMaskSubGraph( ElementGraph elementGraph, FlowElement... maskedFlowElements )
    {
    this( elementGraph, new VertexMask( Arrays.asList( maskedFlowElements ) ), new EdgeMask( null ) );
    }

  public ElementMaskSubGraph( ElementGraph elementGraph, Collection<FlowElement> maskedFlowElements )
    {
    this( elementGraph, new VertexMask( maskedFlowElements ), new EdgeMask( null ) );
    }

  public ElementMaskSubGraph( ElementGraph elementGraph, Collection<FlowElement> maskedFlowElements, Collection<Scope> maskedScopes )
    {
    this( elementGraph, new VertexMask( maskedFlowElements ), new EdgeMask( maskedScopes ) );
    }

  public ElementMaskSubGraph( ElementMaskSubGraph graph )
    {
    this( graph.elementGraph, graph.vertexMask, graph.edgeMask );
    }

  protected ElementMaskSubGraph( ElementGraph elementGraph, VertexMask vertexMask, EdgeMask edgeMask )
    {
    this.graph = new DirectedMaskSubGraph( directed( elementGraph ), vertexMask, edgeMask );

    this.elementGraph = elementGraph;
    this.vertexMask = vertexMask;
    this.edgeMask = edgeMask;
    }

  @Override
  public ElementGraph copyElementGraph()
    {
    return new ElementMaskSubGraph( ElementMaskSubGraph.this );
    }

  private class DirectedMaskSubGraph extends MaskSubgraph<FlowElement, Scope>
    {
    public DirectedMaskSubGraph( Graph<FlowElement, Scope> base, VertexMask vertexMask, EdgeMask edgeMask )
      {
      super( base, vertexMask, edgeMask );
      }
    }
  }
