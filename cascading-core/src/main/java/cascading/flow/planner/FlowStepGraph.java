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

import java.util.List;
import java.util.Map;

import cascading.flow.FlowStep;
import cascading.flow.planner.graph.ElementGraph;

public class FlowStepGraph extends ProcessGraph<FlowStep>
  {
  private transient String tracePath;

  public FlowStepGraph()
    {
    }

  public FlowStepGraph( String tracePath, FlowPlanner<?, ?> flowPlanner, FlowElementGraph flowElementGraph, Map<ElementGraph, List<ElementGraph>> nodeSubGraphsMap, Map<ElementGraph, List<ElementGraph>> pipelineSubGraphsMap )
    {
    this.tracePath = tracePath;
    buildGraph( flowPlanner, flowElementGraph, nodeSubGraphsMap, pipelineSubGraphsMap );
    }

  protected void buildGraph( FlowPlanner<?, ?> flowPlanner, FlowElementGraph flowElementGraph, Map<ElementGraph, List<ElementGraph>> nodeSubGraphsMap, Map<ElementGraph, List<ElementGraph>> pipelineSubGraphsMap )
    {
    int totalSteps = nodeSubGraphsMap.size();
    int stepCount = 0;

    for( ElementGraph stepSubGraph : nodeSubGraphsMap.keySet() )
      {
      List<ElementGraph> nodeSubGraphs = nodeSubGraphsMap.get( stepSubGraph );

      writePlan( stepCount, stepSubGraph, nodeSubGraphs, pipelineSubGraphsMap );

      FlowNodeGraph flowNodeGraph = createFlowNodeGraph( flowElementGraph, pipelineSubGraphsMap, nodeSubGraphs );

      FlowStep flowStep = flowPlanner.createFlowStep( totalSteps, stepCount++, stepSubGraph, flowNodeGraph );

      addVertex( flowStep );
      }

    bindEdges();
    }

  protected FlowNodeGraph createFlowNodeGraph( FlowElementGraph flowElementGraph, Map<ElementGraph, List<ElementGraph>> pipelineSubGraphsMap, List<ElementGraph> nodeSubGraphs )
    {
    return new FlowNodeGraph( flowElementGraph, nodeSubGraphs, pipelineSubGraphsMap );
    }

  private void writePlan( int stepCount, ElementGraph stepSubGraph, List<ElementGraph> nodeSubGraphs, Map<ElementGraph, List<ElementGraph>> pipelineSubGraphsMap )
    {
    if( getTracePath() == null )
      return;

    String rootPath = getTracePath() + "/steps";
    String stepGraphName = String.format( "%s/%04d-step-sub-graph.dot", rootPath, stepCount );

    stepSubGraph.writeDOT( stepGraphName );

    for( int i = 0; i < nodeSubGraphs.size(); i++ )
      {
      ElementGraph nodeGraph = nodeSubGraphs.get( i );
      String nodeGraphName = String.format( "%s/%04d-%04d-step-node-sub-graph.dot", rootPath, stepCount, i );

      nodeGraph.writeDOT( nodeGraphName );

      List<ElementGraph> pipelineGraphs = pipelineSubGraphsMap.get( nodeGraph );

      if( pipelineGraphs == null )
        continue;

      for( int j = 0; j < pipelineGraphs.size(); j++ )
        {
        ElementGraph pipelineGraph = pipelineGraphs.get( j );

        String pipelineGraphName = String.format( "%s/%04d-%04d-%04d-step-node-pipeline-sub-graph.dot", rootPath, stepCount, i, j );

        pipelineGraph.writeDOT( pipelineGraphName );
        }

      }
    }

  private String getTracePath()
    {
    return tracePath;
    }
  }
