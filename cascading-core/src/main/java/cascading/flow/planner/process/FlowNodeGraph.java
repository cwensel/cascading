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

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cascading.flow.FlowElement;
import cascading.flow.FlowNode;
import cascading.flow.planner.BaseFlowNode;
import cascading.flow.planner.BaseFlowNodeFactory;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.graph.FlowElementGraph;

import static cascading.util.Util.createIdentitySet;

/**
 *
 */
public class FlowNodeGraph extends BaseProcessGraph<FlowNode>
  {
  public static final FlowNodeComparator FLOW_NODE_COMPARATOR = new FlowNodeComparator();

  /**
   * Class FlowNodeComparator provides a consistent tie breaker when ordering nodes topologically.
   * <p/>
   * This should have no effect on submission and execution priority as all FlowNodes are submitted simultaneously.
   */
  public static class FlowNodeComparator implements Comparator<FlowNode>
    {
    @Override
    public int compare( FlowNode lhs, FlowNode rhs )
      {
      // larger graph first
      int lhsSize = lhs.getElementGraph().vertexSet().size();
      int rhsSize = rhs.getElementGraph().vertexSet().size();
      int result = ( lhsSize < rhsSize ) ? -1 : ( ( lhsSize == rhsSize ) ? 0 : 1 );

      if( result != 0 )
        return result;

      // more inputs second
      lhsSize = lhs.getSourceElements().size();
      rhsSize = rhs.getSourceElements().size();

      return ( lhsSize < rhsSize ) ? -1 : ( ( lhsSize == rhsSize ) ? 0 : 1 );
      }
    }

  public FlowNodeGraph()
    {
    }

  public FlowNodeGraph( FlowElementGraph flowElementGraph, List<? extends ElementGraph> nodeSubGraphs )
    {
    this( new BaseFlowNodeFactory(), flowElementGraph, nodeSubGraphs );
    }

  public FlowNodeGraph( FlowNodeFactory flowNodeFactory, List<? extends ElementGraph> nodeSubGraphs )
    {
    this( flowNodeFactory, null, nodeSubGraphs );
    }

  public FlowNodeGraph( FlowNodeFactory flowNodeFactory, FlowElementGraph flowElementGraph, List<? extends ElementGraph> nodeSubGraphs )
    {
    this( flowNodeFactory, flowElementGraph, nodeSubGraphs, Collections.<ElementGraph, List<? extends ElementGraph>>emptyMap() );
    }

  public FlowNodeGraph( FlowNodeFactory flowNodeFactory, FlowElementGraph flowElementGraph, List<? extends ElementGraph> nodeSubGraphs, Map<ElementGraph, List<? extends ElementGraph>> pipelineSubGraphsMap )
    {
    buildGraph( flowNodeFactory, flowElementGraph, nodeSubGraphs, pipelineSubGraphsMap );

    // consistently sets ordinal of node based on topological dependencies and tie breaking by the given Comparator
    Iterator<FlowNode> iterator = getOrderedTopologicalIterator();

    int ordinal = 0;
    int size = vertexSet().size();

    while( iterator.hasNext() )
      {
      BaseFlowNode next = (BaseFlowNode) iterator.next();

      next.setOrdinal( ordinal );
      next.setName( flowNodeFactory.makeFlowNodeName( next, size, ordinal ) );

      ordinal++;
      }
    }

  protected void buildGraph( FlowNodeFactory flowNodeFactory, FlowElementGraph flowElementGraph, List<? extends ElementGraph> nodeSubGraphs, Map<ElementGraph, List<? extends ElementGraph>> pipelineSubGraphsMap )
    {
    if( pipelineSubGraphsMap == null )
      pipelineSubGraphsMap = Collections.emptyMap();

    for( ElementGraph nodeSubGraph : nodeSubGraphs )
      {
      List<? extends ElementGraph> pipelineGraphs = pipelineSubGraphsMap.get( nodeSubGraph );

      FlowNode flowNode = flowNodeFactory.createFlowNode( flowElementGraph, nodeSubGraph, pipelineGraphs );

      addVertex( flowNode );
      }

    bindEdges();
    }

  public Set<FlowElement> getFlowElementsFor( Enum annotation )
    {
    Set<FlowElement> results = createIdentitySet();

    for( FlowNode flowNode : vertexSet() )
      results.addAll( flowNode.getFlowElementsFor( annotation ) );

    return results;
    }

  public Iterator<FlowNode> getOrderedTopologicalIterator()
    {
    return super.getOrderedTopologicalIterator( FLOW_NODE_COMPARATOR );
    }
  }
