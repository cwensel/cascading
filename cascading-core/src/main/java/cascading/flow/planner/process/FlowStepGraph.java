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

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import cascading.flow.FlowElement;
import cascading.flow.FlowStep;
import cascading.flow.planner.BaseFlowStep;
import cascading.flow.planner.FlowPlanner;
import cascading.flow.planner.graph.AnnotatedDecoratedElementGraph;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.graph.FlowElementGraph;
import cascading.util.EnumMultiMap;

public class FlowStepGraph extends BaseProcessGraph<FlowStep>
  {
  public FlowStepGraph()
    {
    }

  public FlowStepGraph( FlowPlanner<?, ?> flowPlanner, FlowElementGraph flowElementGraph, Map<ElementGraph, List<? extends ElementGraph>> nodeSubGraphsMap, Map<ElementGraph, List<? extends ElementGraph>> pipelineSubGraphsMap )
    {
    buildGraph( flowPlanner, flowElementGraph, nodeSubGraphsMap, pipelineSubGraphsMap );

    Iterator<FlowStep> iterator = getTopologicalIterator();

    int ordinal = 0;
    int size = vertexSet().size();

    while( iterator.hasNext() )
      {
      BaseFlowStep flowStep = (BaseFlowStep) iterator.next();

      flowStep.setOrdinal( ordinal++ );
      flowStep.setName( flowPlanner.makeFlowStepName( flowStep, size, flowStep.getOrdinal() ) );
      }
    }

  protected void buildGraph( FlowPlanner<?, ?> flowPlanner, FlowElementGraph flowElementGraph, Map<ElementGraph, List<? extends ElementGraph>> nodeSubGraphsMap, Map<ElementGraph, List<? extends ElementGraph>> pipelineSubGraphsMap )
    {
    for( ElementGraph stepSubGraph : nodeSubGraphsMap.keySet() )
      {
      List<? extends ElementGraph> nodeSubGraphs = nodeSubGraphsMap.get( stepSubGraph );
      FlowNodeGraph flowNodeGraph = createFlowNodeGraph( flowPlanner, flowElementGraph, pipelineSubGraphsMap, nodeSubGraphs );

      EnumMultiMap<FlowElement> annotations = flowNodeGraph.getAnnotations();

      // pull up annotations
      if( !annotations.isEmpty() )
        stepSubGraph = new AnnotatedDecoratedElementGraph( stepSubGraph, annotations );

      FlowStep flowStep = flowPlanner.createFlowStep( stepSubGraph, flowNodeGraph );

      addVertex( flowStep );
      }

    bindEdges();
    }

  protected FlowNodeGraph createFlowNodeGraph( FlowPlanner<?, ?> flowPlanner, FlowElementGraph flowElementGraph, Map<ElementGraph, List<? extends ElementGraph>> pipelineSubGraphsMap, List<? extends ElementGraph> nodeSubGraphs )
    {
    return new FlowNodeGraph( flowPlanner, flowElementGraph, nodeSubGraphs, pipelineSubGraphsMap );
    }
  }
