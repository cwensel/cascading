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

import java.util.Collections;
import java.util.Set;

import cascading.flow.FlowElement;
import cascading.flow.planner.PlannerContext;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.iso.expression.ExpressionGraph;
import cascading.flow.planner.iso.finder.GraphFinder;
import cascading.flow.planner.iso.finder.Match;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public abstract class RecursiveGraphTransformer<E extends ElementGraph> extends GraphTransformer<E, E>
  {
  private static final Logger LOG = LoggerFactory.getLogger( RecursiveGraphTransformer.class );
  public static final int TRANSFORM_RECURSION_DEPTH_MAX = 1000;

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
  public Transformed<E> transform( PlannerContext plannerContext, E rootGraph )
    {
    Transformed<E> transformed = new Transformed<>( plannerContext, this, expressionGraph, rootGraph );

    E result = transform( transformed, rootGraph, 0 );

    transformed.setEndGraph( result );

    return transformed;
    }

  protected E transform( Transformed<E> transformed, E graph, int depth )
    {
    if( depth == TRANSFORM_RECURSION_DEPTH_MAX )
      {
      LOG.info( "!!! transform recursion ending, reached depth: {}", depth );
      return graph;
      }

    if( LOG.isDebugEnabled() )
      LOG.debug( "preparing match within: {}", this.getClass().getSimpleName() );

    ElementGraph prepared = prepareForMatch( transformed, graph );

    if( LOG.isDebugEnabled() )
      LOG.debug( "completed match within: {}, with result: {}", this.getClass().getSimpleName(), prepared != null );

    if( prepared == null )
      return graph;

    Set<FlowElement> exclusions = addExclusions( graph );

    Match match;

    if( LOG.isDebugEnabled() )
      LOG.debug( "performing match within: {}, using recursion: {}", this.getClass().getSimpleName(), !findAllPrimaries );

    // for trivial cases, disable recursion and capture all primaries initially
    if( findAllPrimaries )
      match = finder.findAllMatches( transformed.getPlannerContext(), prepared, exclusions );
    else
      match = finder.findFirstMatch( transformed.getPlannerContext(), prepared, exclusions );

    if( LOG.isDebugEnabled() )
      LOG.debug( "completed match within: {}", this.getClass().getSimpleName() );

    if( LOG.isDebugEnabled() )
      LOG.debug( "performing transform in place within: {}", this.getClass().getSimpleName() );

    boolean transformResult = transformGraphInPlaceUsing( transformed, graph, match );

    if( LOG.isDebugEnabled() )
      LOG.debug( "completed transform in place within: {}, with result: {}", this.getClass().getSimpleName(), transformResult );

    if( !transformResult )
      return graph;

    transformed.addRecursionTransform( graph );

    if( findAllPrimaries )
      return graph;

    return transform( transformed, graph, ++depth );
    }

  protected Set<FlowElement> addExclusions( E graph )
    {
    return Collections.emptySet();
    }

  protected ElementGraph prepareForMatch( Transformed<E> transformed, E graph )
    {
    return graph;
    }

  protected abstract boolean transformGraphInPlaceUsing( Transformed<E> transformed, E graph, Match match );
  }
