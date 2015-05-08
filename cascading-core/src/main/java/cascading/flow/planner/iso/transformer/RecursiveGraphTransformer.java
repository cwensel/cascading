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

package cascading.flow.planner.iso.transformer;

import java.util.Collections;
import java.util.Set;

import cascading.flow.FlowElement;
import cascading.flow.planner.PlannerContext;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.iso.expression.ExpressionGraph;
import cascading.flow.planner.iso.finder.GraphFinder;
import cascading.flow.planner.iso.finder.Match;
import cascading.util.ProcessLogger;

/**
 *
 */
public abstract class RecursiveGraphTransformer<E extends ElementGraph> extends GraphTransformer<E, E>
  {
  /**
   * Graphs must be transformed iteratively. In order to offset rules that may cause a loop, the recursion depth
   * must be tracked.
   * <p/>
   * Some complex graphs may require a very deep search, so this value may need to be increased.
   * <p/>
   * During debugging, the depth may need to be shallow so that the trace logs can be written to disk before an
   * OOME causes the planner to exit.
   * <p/>
   * This property may be either set at the planner ){@link cascading.flow.FlowConnector} or system level
   * {@link System#getProperties()}.
   */
  public static final String TRANSFORM_RECURSION_DEPTH_MAX = "cascading.planner.transformer.recursion.depth.max";
  public static final int DEFAULT_TRANSFORM_RECURSION_DEPTH_MAX = 1000;

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
    int maxDepth = plannerContext.getIntProperty( TRANSFORM_RECURSION_DEPTH_MAX, DEFAULT_TRANSFORM_RECURSION_DEPTH_MAX );

    Transformed<E> transformed = new Transformed<>( plannerContext, this, expressionGraph, rootGraph );

    E result = transform( plannerContext.getLogger(), transformed, rootGraph, maxDepth, 0 );

    transformed.setEndGraph( result );

    return transformed;
    }

  protected E transform( ProcessLogger processLogger, Transformed<E> transformed, E graph, int maxDepth, int currentDepth )
    {
    if( currentDepth == maxDepth )
      {
      processLogger.logInfo( "!!! transform recursion ending, reached depth: {}", currentDepth );
      return graph;
      }

    if( processLogger.isDebugEnabled() )
      processLogger.logDebug( "preparing match within: {}", this.getClass().getSimpleName() );

    ElementGraph prepared = prepareForMatch( processLogger, transformed, graph );

    if( processLogger.isDebugEnabled() )
      processLogger.logDebug( "completed match within: {}, with result: {}", this.getClass().getSimpleName(), prepared != null );

    if( prepared == null )
      return graph;

    Set<FlowElement> exclusions = addExclusions( graph );

    Match match;

    if( processLogger.isDebugEnabled() )
      processLogger.logDebug( "performing match within: {}, using recursion: {}", this.getClass().getSimpleName(), !findAllPrimaries );

    // for trivial cases, disable recursion and capture all primaries initially
    // if prepareForMatch returns a sub-graph, find all matches in the sub-graph, but we do not exit the recursion
    if( findAllPrimaries )
      match = finder.findAllMatches( transformed.getPlannerContext(), prepared, exclusions );
    else
      match = finder.findFirstMatch( transformed.getPlannerContext(), prepared, exclusions );

    if( processLogger.isDebugEnabled() )
      processLogger.logDebug( "completed match within: {}", this.getClass().getSimpleName() );

    if( processLogger.isDebugEnabled() )
      processLogger.logDebug( "performing transform in place within: {}", this.getClass().getSimpleName() );

    boolean transformResult = transformGraphInPlaceUsing( transformed, graph, match );

    if( processLogger.isDebugEnabled() )
      processLogger.logDebug( "completed transform in place within: {}, with result: {}", this.getClass().getSimpleName(), transformResult );

    if( !transformResult )
      return graph;

    transformed.addRecursionTransform( graph );

    if( !requiresRecursiveSearch() )
      return graph;

    return transform( processLogger, transformed, graph, maxDepth, ++currentDepth );
    }

  /**
   * By default, prepareForMatch returns the same graph, but sub-classes may return a sub-graph, one of many
   * requiring sub-sequent matches.
   * <p/>
   * if we are searching the whole graph, there is no need to perform a recursion against the new transformed graph
   */
  protected boolean requiresRecursiveSearch()
    {
    return !findAllPrimaries;
    }

  protected Set<FlowElement> addExclusions( E graph )
    {
    return Collections.emptySet();
    }

  protected ElementGraph prepareForMatch( ProcessLogger processLogger, Transformed<E> transformed, E graph )
    {
    return graph;
    }

  protected abstract boolean transformGraphInPlaceUsing( Transformed<E> transformed, E graph, Match match );
  }
