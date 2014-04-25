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

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import cascading.flow.FlowElement;
import cascading.flow.planner.Extent;
import cascading.flow.planner.PlannerContext;
import cascading.flow.planner.graph.ElementDirectedGraph;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.graph.ElementMaskSubGraph;
import cascading.flow.planner.graph.ElementSubGraph;
import cascading.flow.planner.iso.expression.ExpressionGraph;
import cascading.flow.planner.iso.finder.Match;

/**
 *
 */
public class GraphPartitioner
  {
  ElementAnnotation annotation;
  ExpressionGraph contractionGraph;
  ExpressionGraph expressionGraph;

  protected GraphPartitioner()
    {
    }

  public GraphPartitioner( ExpressionGraph contractionGraph, ExpressionGraph expressionGraph  )
    {
    this.contractionGraph = contractionGraph;
    this.expressionGraph = expressionGraph;
    }

  public GraphPartitioner( ElementAnnotation annotation, ExpressionGraph contractionGraph, ExpressionGraph expressionGraph )
    {
    this.annotation = annotation;
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

  public Partitions partition( PlannerContext plannerContext, ElementGraph elementGraph )
    {
    Map<ElementGraph, Map<Enum, Set<FlowElement>>> annotatedSubGraphs = new LinkedHashMap<>();

    if( expressionGraph == null )
      {
      // need a safe copy
      if( elementGraph.containsVertex( Extent.head ) )
        elementGraph = new ElementMaskSubGraph( elementGraph, Extent.head, Extent.tail );

      annotatedSubGraphs.put( new ElementDirectedGraph( elementGraph ), Collections.<Enum, Set<FlowElement>>emptyMap() );

      return new Partitions( this, elementGraph, annotatedSubGraphs );
      }

    SubGraphIterator stepIterator = new SubGraphIterator( plannerContext, contractionGraph, expressionGraph, elementGraph );

    int count = 0;
    while( stepIterator.hasNext() )
      {
      ElementSubGraph next = stepIterator.next();

      Map<Enum, Set<FlowElement>> annotations = Collections.emptyMap();

      if( annotation != null )
        {
        Match match = stepIterator.getContractedMatches().get( count++ );

        annotations = new HashMap<>();

        annotations.put( annotation.annotation, match.getCapturedElements( annotation.capture ) );
        }

      annotatedSubGraphs.put( next, annotations );
      }

    return new Partitions( this, stepIterator, elementGraph, annotatedSubGraphs );
    }
  }
