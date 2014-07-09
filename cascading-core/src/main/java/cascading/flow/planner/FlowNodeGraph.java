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

package cascading.flow.planner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cascading.flow.FlowElement;
import cascading.flow.planner.graph.ElementGraph;

/**
 *
 */
public class FlowNodeGraph extends ProcessGraph<FlowNode>
  {
  private FlowElementGraph flowElementGraph;

  public FlowNodeGraph( FlowElementGraph flowElementGraph, List<? extends ElementGraph> nodeSubGraphs )
    {
    this( flowElementGraph, nodeSubGraphs, Collections.<ElementGraph, List<? extends ElementGraph>>emptyMap() );
    }

  public FlowNodeGraph( FlowElementGraph flowElementGraph, List<? extends ElementGraph> nodeSubGraphs, Map<ElementGraph, List<? extends ElementGraph>> pipelineSubGraphsMap )
    {
    this.flowElementGraph = flowElementGraph;
    buildGraph( flowElementGraph, nodeSubGraphs, pipelineSubGraphsMap );
    }

  protected void buildGraph( FlowElementGraph flowElementGraph, List<? extends ElementGraph> nodeSubGraphs, Map<ElementGraph, List<? extends ElementGraph>> pipelineSubGraphsMap )
    {
    int count = 0;
    for( ElementGraph nodeSubGraph : nodeSubGraphs )
      {
      List<? extends ElementGraph> pipelineGraphs = pipelineSubGraphsMap.get( nodeSubGraph );
      FlowNode flowNode = new FlowNode( count++, "node", flowElementGraph, nodeSubGraph, pipelineGraphs );

      addVertex( flowNode );
      }

    bindEdges();
    }

  public Set<FlowElement> getFlowElementsFor( Enum annotation )
    {
    Set<FlowElement> results = new HashSet<>();

    for( FlowNode flowNode : vertexSet() )
      results.addAll( flowNode.getFlowElementsFor( annotation ) );

    return results;
    }

  public FlowNodeGraph promotePipelines()
    {
    List<ElementGraph> nodeSubGraphs = new ArrayList<>();
    Iterator<FlowNode> iterator = getTopologicalIterator();

    while( iterator.hasNext() )
      {
      FlowNode flowNode = iterator.next();

      List<? extends ElementGraph> pipelineGraphs = flowNode.getPipelineGraphs();

      if( pipelineGraphs == null )
        nodeSubGraphs.add( flowNode.getElementGraph() );
      else
        nodeSubGraphs.addAll( pipelineGraphs );
      }

    return new FlowNodeGraph( flowElementGraph, nodeSubGraphs );
    }
  }
