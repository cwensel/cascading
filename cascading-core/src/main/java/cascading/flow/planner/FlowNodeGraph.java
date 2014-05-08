/*
 * Copyright (c) 2007-2009 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Cascading is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Cascading is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Cascading.  If not, see <http://www.gnu.org/licenses/>.
 */

package cascading.flow.planner;

import java.util.HashSet;
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
  public FlowNodeGraph( FlowElementGraph flowElementGraph, List<ElementGraph> nodeSubGraph, Map<ElementGraph, List<ElementGraph>> pipelineSubGraphsMap )
    {
    buildGraph( flowElementGraph, nodeSubGraph, pipelineSubGraphsMap );
    }

  protected void buildGraph( FlowElementGraph flowElementGraph, List<ElementGraph> nodeSubGraphs, Map<ElementGraph, List<ElementGraph>> pipelineSubGraphsMap )
    {
    int count = 0;
    for( ElementGraph nodeSubGraph : nodeSubGraphs )
      {
      List<ElementGraph> pipelineGraphs = pipelineSubGraphsMap.get( nodeSubGraph );
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
  }
