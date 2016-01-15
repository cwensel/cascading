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

package cascading.flow.planner;

import java.util.List;

import cascading.flow.FlowNode;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.graph.FlowElementGraph;
import cascading.flow.planner.process.FlowNodeFactory;

/**
 *
 */
public class BaseFlowNodeFactory implements FlowNodeFactory
  {
  @Override
  public FlowNode createFlowNode( FlowElementGraph flowElementGraph, ElementGraph nodeSubGraph, List<? extends ElementGraph> pipelineGraphs )
    {
    return new BaseFlowNode( flowElementGraph, nodeSubGraph, pipelineGraphs );
    }

  @Override
  public String makeFlowNodeName( FlowNode flowNode, int size, int ordinal )
    {
    return String.format( "(%d/%d)", ordinal + 1, size );
    }
  }
