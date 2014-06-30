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

package cascading.flow.planner.iso.finder;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import cascading.flow.FlowElement;
import cascading.flow.planner.PlannerContext;
import cascading.flow.planner.Scope;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.iso.expression.ElementCapture;
import cascading.flow.planner.iso.expression.ElementExpression;
import cascading.flow.planner.iso.expression.ExpressionGraph;
import cascading.flow.planner.iso.expression.ScopeExpression;
import cascading.util.EnumMultiMap;
import cascading.util.Pair;
import cascading.util.Util;
import org.jgrapht.graph.DirectedMultigraph;

/**
 *
 */
public class GraphFinder
  {
  ExpressionGraph matchExpression;

  public GraphFinder( ExpressionGraph matchExpression, ElementCapture... captures )
    {
    if( matchExpression == null )
      throw new IllegalArgumentException( "expressionGraph may not be null" );

    this.matchExpression = matchExpression;
    }

  public ExpressionGraph getMatchExpression()
    {
    return matchExpression;
    }

  public Match findFirstMatch( ElementGraph elementGraph )
    {
    return findFirstMatch( new PlannerContext(), elementGraph );
    }

  public Match findFirstMatch( PlannerContext plannerContext, ElementGraph elementGraph )
    {
    return findFirstMatch( new FinderContext(), plannerContext, elementGraph );
    }

  public Match findFirstMatch( PlannerContext plannerContext, ElementGraph elementGraph, Set<FlowElement> exclusions )
    {
    return findFirstMatch( new FinderContext( exclusions ), plannerContext, elementGraph );
    }

  protected Match findFirstMatch( FinderContext finderContext, PlannerContext plannerContext, ElementGraph elementGraph )
    {
    Map<ElementExpression, FlowElement> mapping = findMapping( finderContext, plannerContext, elementGraph );

    return new Match( matchExpression, elementGraph, mapping, mapping.values(), getAllEdges( plannerContext, elementGraph, mapping ) );
    }

  public Match findAllMatches( ElementGraph elementGraph )
    {
    return findAllMatches( new PlannerContext(), elementGraph );
    }

  public Match findAllMatches( PlannerContext plannerContext, ElementGraph elementGraph )
    {
    return findAllMatches( plannerContext, elementGraph, Collections.<FlowElement>emptySet() );
    }

  public Match findAllMatches( PlannerContext plannerContext, ElementGraph elementGraph, Set<FlowElement> exclusions )
    {
    Set<ElementExpression> elementExpressions = matchExpression.getDelegate().vertexSet();

    if( elementExpressions.size() != 1 )
      throw new IllegalStateException( "may not search multiple matches against multi-node expression: " + matchExpression );

    ElementExpression expression = Util.getFirst( elementExpressions );

    if( expression.getCapture() != ElementCapture.Primary )
      throw new IllegalStateException( "capture on expression must be Primary: " + expression );

    Set<FlowElement> foundElements = new LinkedHashSet<>();

    // no evidence elementGraph.vertexSet().iterator(); is faster without modifying jgrapht
    Iterator<FlowElement> iterator = SearchOrder.getNodeIterator( matchExpression.getSearchOrder(), elementGraph );

    while( iterator.hasNext() )
      {
      FlowElement flowElement = iterator.next();

      if( exclusions.contains( flowElement ) )
        continue;

      if( expression.applies( plannerContext, elementGraph, flowElement ) )
        foundElements.add( flowElement );
      }

    // we are only capturing Primary distinguished elements
    return new Match( matchExpression, elementGraph, null, foundElements, Collections.<Scope>emptySet() )
    {
    @Override
    public Set<FlowElement> getCapturedElements( ElementCapture... captures )
      {
      if( !Arrays.asList( captures ).contains( ElementCapture.Primary ) )
        return Collections.emptySet();

      return (Set<FlowElement>) this.foundElements;
      }
    };
    }

  public Match findAllMatchesOnPrimary( ElementGraph elementGraph )
    {
    return findAllMatchesOnPrimary( new PlannerContext(), elementGraph );
    }

  public Match findAllMatchesOnPrimary( PlannerContext plannerContext, ElementGraph elementGraph )
    {
    return findMatchesOnPrimary( new FinderContext(), plannerContext, elementGraph, false );
    }

  public Match findMatchesOnPrimary( PlannerContext plannerContext, ElementGraph elementGraph, boolean firstOnly, Set<FlowElement> excludes )
    {
    return findMatchesOnPrimary( new FinderContext( excludes ), plannerContext, elementGraph, firstOnly );
    }

  public Match findAllMatchesOnPrimary( PlannerContext plannerContext, ElementGraph elementGraph, Set<FlowElement> excludes )
    {
    return findMatchesOnPrimary( new FinderContext( excludes ), plannerContext, elementGraph, false );
    }

  protected Match findMatchesOnPrimary( FinderContext finderContext, PlannerContext plannerContext, ElementGraph elementGraph, boolean firstOnly )
    {
    Match match = null;

    EnumMultiMap<FlowElement> captureMap = new EnumMultiMap<>();

    while( true )
      {
      Match current = findFirstMatch( finderContext, plannerContext, elementGraph );

      if( !current.foundMatch() )
        break;

      captureMap.addAll( current.getCaptureMap() );

      Set<FlowElement> anchoredElements = current.getCapturedElements( ElementCapture.Primary );

      // should never capture new primary elements in subsequent searches
      if( finderContext.getRequiredElements().isEmpty() )
        finderContext.getRequiredElements().addAll( anchoredElements );

      match = current;

      Map<ElementExpression, FlowElement> vertexMapping = current.getVertexMapping();

      finderContext.getMatchedElements().addAll( vertexMapping.values() );
      finderContext.getMatchedScopes().addAll( getAllEdges( plannerContext, elementGraph, vertexMapping ) );

      if( firstOnly ) // we are not rotating around the primary capture
        break;

      Set<FlowElement> includedElements = current.getIncludedElements();

      if( includedElements.isEmpty() )
        break;

      // should only ignore edges, not elements
      finderContext.getIgnoredElements().addAll( includedElements );
      }

    // TODO: must capture all vertex mappings in order to see all Secondary and Included elements for annotations

    // this only returns the last mapping, but does capture the Primary matches as they are required across all matches
    Map<ElementExpression, FlowElement> mapping = match == null ? null : match.getVertexMapping();

    return new Match( matchExpression, elementGraph, mapping, finderContext.getMatchedElements(), finderContext.getMatchedScopes(), captureMap );
    }

  public Map<ScopeExpression, Set<Scope>> getEdgeMapping( PlannerContext plannerContext, ElementGraph elementGraph, Map<ElementExpression, FlowElement> vertexMapping )
    {
    Map<ScopeExpression, Set<Scope>> edgeMapping = new HashMap<>();

    DirectedMultigraph<ElementExpression, ScopeExpression> delegate = matchExpression.getDelegate();
    for( ScopeExpression scopeExpression : delegate.edgeSet() )
      {
      ElementExpression lhs = delegate.getEdgeSource( scopeExpression );
      ElementExpression rhs = delegate.getEdgeTarget( scopeExpression );

      FlowElement lhsElement = vertexMapping.get( lhs );
      FlowElement rhsElement = vertexMapping.get( rhs );

      Set<Scope> edges = elementGraph.getAllEdges( lhsElement, rhsElement );

      if( edges != null )
        edgeMapping.put( scopeExpression, edges );
      }

    return edgeMapping;
    }

  public Set<Scope> getAllEdges( PlannerContext plannerContext, ElementGraph elementGraph, Map<ElementExpression, FlowElement> vertexMapping )
    {
    Set<Scope> scopes = new HashSet<>();

    for( Set<Scope> set : getEdgeMapping( plannerContext, elementGraph, vertexMapping ).values() )
      scopes.addAll( set );

    return scopes;
    }

  public Map<ElementExpression, FlowElement> findMapping( PlannerContext plannerContext, ElementGraph elementGraph )
    {
    return findMapping( new FinderContext(), plannerContext, elementGraph );
    }

  protected Map<ElementExpression, FlowElement> findMapping( FinderContext finderContext, PlannerContext plannerContext, ElementGraph elementGraph )
    {
    State state = new State( finderContext, plannerContext, matchExpression.getSearchOrder(), matchExpression.getDelegate(), elementGraph );

    Map<Integer, Integer> vertexMap = new LinkedHashMap<>();

    boolean match = match( state, vertexMap );

    if( !match )
      return Collections.emptyMap();

    Map<ElementExpression, FlowElement> result = new LinkedHashMap<>();

    for( Map.Entry<Integer, Integer> entry : vertexMap.entrySet() )
      result.put( state.getMatcherNode( entry.getKey() ), state.getElementNode( entry.getValue() ) );

    return result;
    }

  /**
   * Returns {@code true} if the graphs being matched by this state are
   * isomorphic.
   */
  private boolean match( State state, Map<Integer, Integer> vertexMap )
    {
    if( state.isGoal() )
      return true;

    if( state.isDead() )
      return false;

    int n1 = State.NULL_NODE;
    int n2 = State.NULL_NODE;
    Pair<Integer, Integer> next;
    boolean found = false;

    while( !found && ( next = state.nextPair( n1, n2 ) ) != null )
      {
      n1 = next.getLhs();
      n2 = next.getRhs();

      if( state.isFeasiblePair( n1, n2 ) )
        {
        State copy = state.copy();
        copy.addPair( n1, n2 );
        found = match( copy, vertexMap );

        // If we found a mapping, fill the vertex mapping state
        if( found )
          {
          for( Map.Entry<Integer, Integer> entry : copy.getVertexMapping().entrySet() )
            {
            if( vertexMap.containsKey( entry.getKey() ) && !vertexMap.get( entry.getKey() ).equals( entry.getValue() ) )
              throw new IllegalStateException( "duplicate key with differing values" );
            }

          vertexMap.putAll( copy.getVertexMapping() );
          }
        else
          {
          copy.backTrack();
          }
        }
      }

    return found;
    }
  }
