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
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.iso.ElementAnnotation;
import cascading.flow.planner.iso.expression.ExpressionGraph;
import cascading.flow.planner.iso.subgraph.GraphPartitioner;
import cascading.flow.planner.iso.subgraph.Partitions;
import cascading.flow.planner.iso.subgraph.SubGraphIterator;
import cascading.flow.planner.iso.subgraph.iterator.ExpressionSubGraphIterator;
import cascading.util.EnumMultiMap;

/**
 *
 */
public class ExpressionGraphPartitioner extends GraphPartitioner
  {
  protected ExpressionGraph contractionGraph;
  protected ExpressionGraph expressionGraph;
  protected ElementAnnotation[] annotations = new ElementAnnotation[ 0 ];

  public ExpressionGraphPartitioner( ExpressionGraph contractionGraph, ExpressionGraph expressionGraph, ElementAnnotation... annotations )
    {
    this.contractionGraph = contractionGraph;
    this.expressionGraph = expressionGraph;
    this.annotations = annotations;
    }

  public ExpressionGraph getContractionGraph()
    {
    return contractionGraph;
    }

  public ExpressionGraph getExpressionGraph()
    {
    return expressionGraph;
    }

  public ElementAnnotation[] getAnnotations()
    {
    return annotations;
    }

  public void setAnnotations( ElementAnnotation[] annotations )
    {
    this.annotations = annotations;
    }

  @Override
  public Partitions partition( PlannerContext plannerContext, ElementGraph elementGraph, Collection<FlowElement> excludes )
    {
    Map<ElementGraph, EnumMultiMap> annotatedSubGraphs = new LinkedHashMap<>();

    ExpressionSubGraphIterator expressionIterator = new ExpressionSubGraphIterator( plannerContext, contractionGraph, expressionGraph, elementGraph, excludes );

    SubGraphIterator stepIterator = wrapIterator( expressionIterator );

    while( stepIterator.hasNext() )
      {
      ElementGraph next = stepIterator.next();
      EnumMultiMap annotationMap = stepIterator.getAnnotationMap( annotations );

      annotatedSubGraphs.put( next, annotationMap );
      }

    return new Partitions( this, elementGraph, expressionIterator.getContractedGraph(), expressionIterator.getMatches(), annotatedSubGraphs );
    }

  protected SubGraphIterator wrapIterator( ExpressionSubGraphIterator expressionIterator )
    {
    return expressionIterator;
    }
  }
