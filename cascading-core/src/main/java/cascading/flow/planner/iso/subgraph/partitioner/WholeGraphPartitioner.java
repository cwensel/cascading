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

package cascading.flow.planner.iso.subgraph.partitioner;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import cascading.flow.FlowElement;
import cascading.flow.planner.PlannerContext;
import cascading.flow.planner.graph.ElementDirectedGraph;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.graph.ElementMaskSubGraph;
import cascading.flow.planner.graph.Extent;
import cascading.flow.planner.iso.subgraph.GraphPartitioner;
import cascading.flow.planner.iso.subgraph.Partitions;
import cascading.util.EnumMultiMap;

/**
 *
 */
public class WholeGraphPartitioner extends GraphPartitioner
  {
  public WholeGraphPartitioner()
    {
    }

  public Partitions partition( PlannerContext plannerContext, ElementGraph elementGraph, Collection<FlowElement> excludes )
    {
    Map<ElementGraph, EnumMultiMap> annotatedSubGraphs = new LinkedHashMap<>();

    // need a safe copy
    if( elementGraph.containsVertex( Extent.head ) )
      elementGraph = new ElementMaskSubGraph( elementGraph, Extent.head, Extent.tail );

    annotatedSubGraphs.put( new ElementDirectedGraph( elementGraph ), new EnumMultiMap() );

    return new Partitions( this, elementGraph, annotatedSubGraphs );
    }
  }
