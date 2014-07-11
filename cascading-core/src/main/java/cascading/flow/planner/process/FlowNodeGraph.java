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

package cascading.flow.planner.process;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cascading.flow.FlowElement;
import cascading.flow.FlowNode;
import cascading.flow.planner.FlowPlanner;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.graph.FlowElementGraph;

/**
 *
 */
public class FlowNodeGraph extends ProcessGraph<FlowNode>
  {
  public FlowNodeGraph( FlowPlanner<?, ?> flowPlanner, FlowElementGraph flowElementGraph, List<? extends ElementGraph> nodeSubGraphs )
    {
    this( flowPlanner, flowElementGraph, nodeSubGraphs, Collections.<ElementGraph, List<? extends ElementGraph>>emptyMap() );
    }

  public FlowNodeGraph( FlowPlanner<?, ?> flowPlanner, FlowElementGraph flowElementGraph, List<? extends ElementGraph> nodeSubGraphs, Map<ElementGraph, List<? extends ElementGraph>> pipelineSubGraphsMap )
    {
    buildGraph( flowPlanner, flowElementGraph, nodeSubGraphs, pipelineSubGraphsMap );
    }

  protected void buildGraph( FlowPlanner<?, ?> flowPlanner, FlowElementGraph flowElementGraph, List<? extends ElementGraph> nodeSubGraphs, Map<ElementGraph, List<? extends ElementGraph>> pipelineSubGraphsMap )
    {
    int count = 0;
    for( ElementGraph nodeSubGraph : nodeSubGraphs )
      {
      List<? extends ElementGraph> pipelineGraphs = pipelineSubGraphsMap.get( nodeSubGraph );

      FlowNode flowNode = flowPlanner.createFlowNode( nodeSubGraphs.size(), count++, flowElementGraph, nodeSubGraph, pipelineGraphs );

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
  }
