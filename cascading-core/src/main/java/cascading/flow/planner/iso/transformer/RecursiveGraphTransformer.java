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

package cascading.flow.planner.iso.transformer;

import cascading.flow.planner.PlannerContext;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.iso.expression.ExpressionGraph;
import cascading.flow.planner.iso.finder.GraphFinder;
import cascading.flow.planner.iso.finder.Match;

/**
 *
 */
public abstract class RecursiveGraphTransformer<E extends ElementGraph> extends GraphTransformer<E, E>
  {
  private final GraphFinder finder;
  private final ExpressionGraph expressionGraph;
  private final boolean findAllPrimaries;

  protected RecursiveGraphTransformer( ExpressionGraph expressionGraph )
    {
    this.expressionGraph = expressionGraph;
    this.finder = new GraphFinder( expressionGraph );

    this.findAllPrimaries = expressionGraph.supportsNonRecursiveMatch();
    }

  @Override
  public Transform<E> transform( PlannerContext plannerContext, E rootGraph )
    {
    Transform<E> transform = new Transform<>( plannerContext, this, expressionGraph, rootGraph );

    E result = transform( transform, rootGraph );

    transform.setEndGraph( result );

    return transform;
    }

  protected E transform( Transform<E> transform, E graph )
    {
    ElementGraph prepared = prepareForMatch( transform, graph );

    if( prepared == null )
      return graph;

    // for trivial cases, disable recursion and capture all primaries initially
    Match match = findAllPrimaries ? finder.findAllMatches( transform.getPlannerContext(), prepared ) : finder.findFirstMatch( transform.getPlannerContext(), prepared );

    if( !transformGraphInPlaceUsing( transform, graph, match ) )
      return graph;

    transform.addRecursionTransform( graph );

    if( findAllPrimaries )
      return graph;

    return transform( transform, graph );
    }

  protected ElementGraph prepareForMatch( Transform<E> transform, E graph )
    {
    return graph;
    }

  protected abstract boolean transformGraphInPlaceUsing( Transform<E> transform, E graph, Match match );
  }
