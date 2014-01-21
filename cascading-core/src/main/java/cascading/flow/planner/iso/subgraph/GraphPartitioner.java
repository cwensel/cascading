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

package cascading.flow.planner.iso.subgraph;

import java.util.ArrayList;
import java.util.List;

import cascading.flow.planner.FlowElementGraph;
import cascading.flow.planner.PlannerContext;
import cascading.flow.planner.graph.ElementDirectedGraph;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.graph.ElementMaskSubGraph;
import cascading.flow.planner.iso.expression.ExpressionGraph;

/**
 *
 */
public class GraphPartitioner
  {
  ExpressionGraph contractionGraph;
  ExpressionGraph expressionGraph;

  protected GraphPartitioner()
    {
    }

  public GraphPartitioner( ExpressionGraph contractionGraph, ExpressionGraph expressionGraph )
    {
    this.contractionGraph = contractionGraph;
    this.expressionGraph = expressionGraph;
    }

  public ExpressionGraph getContractionGraph()
    {
    return contractionGraph;
    }

  public ExpressionGraph getExpressionGraph()
    {
    return expressionGraph;
    }

  public Partitions partition( PlannerContext plannerContext, FlowElementGraph elementGraph )
    {
    List<ElementGraph> subGraphs = new ArrayList<>();

    if( expressionGraph == null )
      {
      // need a safe copy
      subGraphs.add( new ElementDirectedGraph( new ElementMaskSubGraph( elementGraph, FlowElementGraph.head, FlowElementGraph.tail ) ) );

      return new Partitions( this, null, elementGraph, subGraphs );
      }

    SubGraphIterator stepIterator = new SubGraphIterator( plannerContext, contractionGraph, expressionGraph, elementGraph );

    while( stepIterator.hasNext() )
      subGraphs.add( stepIterator.next() );

    // todo: add all iterator matches so they can be checkpointed
    //       remove iterator writeDOT methods
    return new Partitions( this, stepIterator, elementGraph, subGraphs );
    }
  }
