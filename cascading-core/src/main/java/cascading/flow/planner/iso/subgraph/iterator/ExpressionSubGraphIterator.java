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

package cascading.flow.planner.iso.subgraph.iterator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import cascading.flow.FlowElement;
import cascading.flow.planner.PlannerContext;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.iso.ElementAnnotation;
import cascading.flow.planner.iso.expression.ElementCapture;
import cascading.flow.planner.iso.expression.ExpressionGraph;
import cascading.flow.planner.iso.finder.GraphFinder;
import cascading.flow.planner.iso.finder.Match;
import cascading.flow.planner.iso.subgraph.SubGraphIterator;
import cascading.flow.planner.iso.transformer.ContractedTransformer;
import cascading.flow.planner.iso.transformer.Transformed;
import cascading.util.EnumMultiMap;

import static cascading.flow.planner.graph.ElementGraphs.asSubGraph;

/**
 *
 */
public class ExpressionSubGraphIterator implements SubGraphIterator
  {
  private final PlannerContext plannerContext;
  private final ElementGraph elementGraph;

  private ContractedTransformer contractedTransformer;
  private GraphFinder graphFinder;

  private Set<FlowElement> elementExcludes = new HashSet<>();
  private ElementGraph contractedGraph;
  private Transformed<ElementGraph> contractedTransformed;

  private boolean firstOnly = false; // false will continue to accumulate around the primary
  private Match match;

  private List<Match> matches = new ArrayList<>();

  int count = 0;

  public ExpressionSubGraphIterator( ExpressionGraph matchExpression, ElementGraph elementGraph )
    {
    this( new PlannerContext(), matchExpression, elementGraph );
    }

  public ExpressionSubGraphIterator( PlannerContext plannerContext, ExpressionGraph matchExpression, ElementGraph elementGraph )
    {
    this( plannerContext, null, matchExpression, elementGraph );
    }

  public ExpressionSubGraphIterator( PlannerContext plannerContext, ExpressionGraph contractionExpression, ExpressionGraph matchExpression, ElementGraph elementGraph )
    {
    this( plannerContext, contractionExpression, matchExpression, false, elementGraph );
    }

  public ExpressionSubGraphIterator( PlannerContext plannerContext, ExpressionGraph contractionExpression, ExpressionGraph matchExpression, ElementGraph elementGraph, Collection<FlowElement> elementExcludes )
    {
    this( plannerContext, contractionExpression, matchExpression, false, elementGraph, elementExcludes );
    }

  public ExpressionSubGraphIterator( PlannerContext plannerContext, ExpressionGraph contractionExpression, ExpressionGraph matchExpression, boolean firstOnly, ElementGraph elementGraph )
    {
    this( plannerContext, contractionExpression, matchExpression, firstOnly, elementGraph, null );
    }

  public ExpressionSubGraphIterator( PlannerContext plannerContext, ExpressionGraph contractionExpression, ExpressionGraph matchExpression, boolean firstOnly, ElementGraph elementGraph, Collection<FlowElement> elementExcludes )
    {
    this.plannerContext = plannerContext;
    this.firstOnly = firstOnly;
    this.elementGraph = elementGraph;

    if( elementExcludes != null )
      this.elementExcludes.addAll( elementExcludes );

    if( contractionExpression != null )
      contractedTransformer = new ContractedTransformer( contractionExpression );
    else
      contractedGraph = elementGraph;

    graphFinder = new GraphFinder( matchExpression );
    }

  @Override
  public ElementGraph getElementGraph()
    {
    return elementGraph;
    }

  public List<Match> getMatches()
    {
    return matches;
    }

  public ElementGraph getContractedGraph()
    {
    if( contractedGraph == null )
      {
      contractedTransformed = contractedTransformer.transform( plannerContext, elementGraph );
      contractedGraph = contractedTransformed.getEndGraph();
      }

    return contractedGraph;
    }

  public Match getLastMatch()
    {
    if( matches.isEmpty() )
      return null;

    return matches.get( count - 1 );
    }

  @Override
  public EnumMultiMap getAnnotationMap( ElementAnnotation[] annotations )
    {
    EnumMultiMap annotationsMap = new EnumMultiMap();

    if( annotations.length == 0 )
      return annotationsMap;

    Match match = getLastMatch();

    for( ElementAnnotation annotation : annotations )
      annotationsMap.addAll( annotation.getAnnotation(), match.getCapturedElements( annotation.getCapture() ) );

    return annotationsMap;
    }

  @Override
  public boolean hasNext()
    {
    if( match == null )
      {
      match = graphFinder.findMatchesOnPrimary( plannerContext, getContractedGraph(), firstOnly, elementExcludes );

      if( match.foundMatch() )
        {
        matches.add( match );
        elementExcludes.addAll( match.getCapturedElements( ElementCapture.Primary ) ); // idempotent
        count++;
        }
      }

    return match.foundMatch();
    }

  @Override
  public ElementGraph next()
    {
    try
      {
      if( !hasNext() )
        throw new NoSuchElementException();

      ElementGraph contractedMatchedGraph = match.getMatchedGraph();

      Set<FlowElement> excludes = new HashSet<>( getContractedGraph().vertexSet() );

      excludes.removeAll( contractedMatchedGraph.vertexSet() );

      return asSubGraph( elementGraph, contractedMatchedGraph, excludes );
      }
    finally
      {
      match = null;
      }
    }

  @Override
  public void remove()
    {
    throw new UnsupportedOperationException();
    }
  }
