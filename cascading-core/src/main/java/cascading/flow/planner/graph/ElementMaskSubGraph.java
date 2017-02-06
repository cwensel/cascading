/*
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

import cascading.flow.FlowElement;
import cascading.flow.planner.Scope;
import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DirectedMaskSubgraph;
import org.jgrapht.graph.MaskFunctor;

import static cascading.flow.planner.graph.ElementGraphs.directed;
import static cascading.util.Util.createIdentitySet;

/**
 *
 */
public class ElementMaskSubGraph extends BaseElementGraph implements ElementGraph
  {
  private ElementGraph elementGraph;
  private FlowElementMaskFunctor mask;

  private static class FlowElementMaskFunctor implements MaskFunctor<FlowElement, Scope>
    {
    Set<FlowElement> maskedElements = createIdentitySet();
    Set<Scope> maskedScopes = new HashSet<>();

    public FlowElementMaskFunctor( Collection<FlowElement> flowElements, Collection<Scope> scopes )
      {
      this( flowElements );

      if( scopes != null )
        maskedScopes.addAll( scopes );
      }

    public FlowElementMaskFunctor( Collection<FlowElement> flowElements )
      {
      if( flowElements != null )
        maskedElements.addAll( flowElements );
      }

    @Override
    public boolean isEdgeMasked( Scope scope )
      {
      return maskedScopes.contains( scope );
      }

    @Override
    public boolean isVertexMasked( FlowElement flowElement )
      {
      return maskedElements.contains( flowElement );
      }
    }

  public ElementMaskSubGraph( ElementGraph elementGraph, FlowElement... maskedFlowElements )
    {
    this( elementGraph, new FlowElementMaskFunctor( Arrays.asList( maskedFlowElements ) ) );
    }

  public ElementMaskSubGraph( ElementGraph elementGraph, Collection<FlowElement> maskedFlowElements )
    {
    this( elementGraph, new FlowElementMaskFunctor( maskedFlowElements ) );
    }

  public ElementMaskSubGraph( ElementGraph elementGraph, Collection<FlowElement> maskedFlowElements, Collection<Scope> maskedScopes )
    {
    this( elementGraph, new FlowElementMaskFunctor( maskedFlowElements, maskedScopes ) );
    }

  public ElementMaskSubGraph( ElementMaskSubGraph graph )
    {
    this( graph.elementGraph, graph.mask );
    }

  protected ElementMaskSubGraph( ElementGraph elementGraph, FlowElementMaskFunctor flowElementMaskFunctor )
    {
    this.graph = new DirectedMaskSubGraph( directed( elementGraph ), flowElementMaskFunctor );

    this.elementGraph = elementGraph;
    this.mask = flowElementMaskFunctor;
    }

  @Override
  public ElementGraph copyElementGraph()
    {
    return new ElementMaskSubGraph( ElementMaskSubGraph.this );
    }

  private class DirectedMaskSubGraph extends DirectedMaskSubgraph<FlowElement, Scope>
    {
    public DirectedMaskSubGraph( DirectedGraph<FlowElement, Scope> base, MaskFunctor<FlowElement, Scope> mask )
      {
      super( base, mask );
      }
    }
  }
