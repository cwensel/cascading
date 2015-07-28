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

package cascading.flow.planner.iso.finder;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import cascading.flow.FlowElement;
import cascading.flow.planner.Scope;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.graph.ElementGraphs;
import cascading.flow.planner.graph.ElementSubGraph;
import cascading.flow.planner.iso.expression.ElementCapture;
import cascading.flow.planner.iso.expression.ElementExpression;
import cascading.flow.planner.iso.expression.ExpressionGraph;
import cascading.util.EnumMultiMap;
import cascading.util.Util;

/**
 *
 */
public class Match
  {
  protected final ExpressionGraph matchGraph;
  protected final ElementGraph elementGraph;
  protected final Map<ElementExpression, FlowElement> vertexMapping;
  protected final Collection<FlowElement> foundElements;
  protected final Collection<Scope> foundScopes;
  protected EnumMultiMap<FlowElement> captureMap;

  private ElementSubGraph matchedGraph;

  public Match( ExpressionGraph matchGraph, ElementGraph elementGraph, Map<ElementExpression, FlowElement> vertexMapping, Collection<FlowElement> foundElements, Collection<Scope> foundScopes )
    {
    this( matchGraph, elementGraph, vertexMapping, foundElements, foundScopes, null );
    }

  public Match( ExpressionGraph matchGraph, ElementGraph elementGraph, Map<ElementExpression, FlowElement> vertexMapping, Collection<FlowElement> foundElements, Collection<Scope> foundScopes, EnumMultiMap<FlowElement> captureMap )
    {
    this.matchGraph = matchGraph;
    this.elementGraph = elementGraph;
    this.vertexMapping = vertexMapping == null ? Collections.<ElementExpression, FlowElement>emptyMap() : vertexMapping;
    this.foundElements = foundElements;
    this.foundScopes = foundScopes;
    this.captureMap = captureMap;
    }

  public ElementGraph getElementGraph()
    {
    return elementGraph;
    }

  public ExpressionGraph getMatchGraph()
    {
    return matchGraph;
    }

  public boolean foundMatch()
    {
    return !vertexMapping.values().isEmpty();
    }

  public Map<ElementExpression, FlowElement> getVertexMapping()
    {
    return vertexMapping;
    }

  public Collection<FlowElement> getFoundElements()
    {
    return foundElements;
    }

  public ElementSubGraph getMatchedGraph()
    {
    if( matchedGraph == null )
      matchedGraph = new ElementSubGraph( elementGraph, foundElements, foundScopes );

    return matchedGraph;
    }

  public Set<FlowElement> getIncludedElements()
    {
    return getCapturedElements( ElementCapture.Include );
    }

  public Set<FlowElement> getCapturedElements( ElementCapture... captures )
    {
    return getCaptureMap().getAllValues( captures );
    }

  public EnumMultiMap<FlowElement> getCaptureMap()
    {
    if( captureMap != null )
      return captureMap;

    captureMap = new EnumMultiMap<>();

    Map<FlowElement, ElementExpression> reversed = new LinkedHashMap<>();

    if( Util.reverseMap( vertexMapping, reversed ) )
      throw new IllegalStateException( "duplicates found in mapping" );

    // returns a Set ordered topologically by the matched graph. retains this first, this second ordering for simple cases
    Iterator<FlowElement> iterator = ElementGraphs.getTopologicalIterator( getMatchedGraph() );

    while( iterator.hasNext() )
      {
      FlowElement next = iterator.next();
      ElementExpression elementExpression = reversed.get( next );

      // matchedGraph may be a super-set of what's in the mapping, so elementExpression may be null
      if( elementExpression == null )
        continue;

      captureMap.addAll( elementExpression.getCapture(), next );
      }

    return captureMap;
    }

  @Override
  public String toString()
    {
    final StringBuilder sb = new StringBuilder( "Match{" );
//    sb.append( "matcherGraph=" ).append( matcherGraph );
//    sb.append( ", mapping=" ).append( mapping );
    sb.append( getMatchedGraph() );
    sb.append( '}' );
    return sb.toString();
    }
  }
