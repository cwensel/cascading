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
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import cascading.flow.FlowElement;
import cascading.flow.planner.ElementGraphs;
import cascading.flow.planner.PlannerContext;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.graph.ElementSubGraph;
import cascading.flow.planner.iso.expression.ElementExpression;
import cascading.flow.planner.iso.expression.ExpressionGraph;
import cascading.flow.planner.iso.finder.GraphFinder;
import cascading.flow.planner.iso.finder.Match;
import cascading.flow.planner.iso.transformer.ContractedTransformer;
import cascading.flow.planner.iso.transformer.Transform;

/**
 *
 */
public class SubGraphIterator implements Iterator<ElementSubGraph>
  {
  private final PlannerContext plannerContext;
  private final ElementGraph flowElementGraph;

  private ContractedTransformer contractedTransformer;
  private GraphFinder graphFinder;

  private Set<FlowElement> excludes = new HashSet<>();
  private ElementGraph contractedGraph;
  private Transform<ElementGraph> contractedTransform;

  private boolean firstOnly = false; // false will continue to accumulate around the primary
  private Match match;

  private List<Match> matches = new ArrayList<>();

  int count = 0;

  public SubGraphIterator( ExpressionGraph matchExpression, ElementGraph elementGraph )
    {
    this( new PlannerContext(), matchExpression, elementGraph );
    }

  public SubGraphIterator( PlannerContext plannerContext, ExpressionGraph matchExpression, ElementGraph elementGraph )
    {
    this( plannerContext, null, matchExpression, elementGraph );
    }

  public SubGraphIterator( PlannerContext plannerContext, ExpressionGraph contractionExpression, ExpressionGraph matchExpression, ElementGraph elementGraph )
    {
    this( plannerContext, contractionExpression, matchExpression, false, elementGraph );
    }

  public SubGraphIterator( PlannerContext plannerContext, ExpressionGraph contractionExpression, ExpressionGraph matchExpression, ElementGraph elementGraph, Collection<FlowElement> excludes )
    {
    this( plannerContext, contractionExpression, matchExpression, false, elementGraph, excludes );
    }

  public SubGraphIterator( PlannerContext plannerContext, ExpressionGraph contractionExpression, ExpressionGraph matchExpression, boolean firstOnly, ElementGraph elementGraph )
    {
    this( plannerContext, contractionExpression, matchExpression, firstOnly, elementGraph, null );
    }

  public SubGraphIterator( PlannerContext plannerContext, ExpressionGraph contractionExpression, ExpressionGraph matchExpression, boolean firstOnly, ElementGraph elementGraph, Collection<FlowElement> excludes )
    {
    this.plannerContext = plannerContext;
    this.firstOnly = firstOnly;
    this.flowElementGraph = elementGraph;

    if( excludes != null )
      this.excludes.addAll( excludes );

    if( contractionExpression != null )
      contractedTransformer = new ContractedTransformer( contractionExpression );
    else
      contractedGraph = elementGraph;

    graphFinder = new GraphFinder( matchExpression );
    }

  public List<Match> getContractedMatches()
    {
    return matches;
    }

  public ElementGraph getContractedGraph()
    {
    if( contractedGraph == null )
      {
      contractedTransform = contractedTransformer.transform( plannerContext, flowElementGraph );
      contractedGraph = contractedTransform.getEndGraph();
      }

    return contractedGraph;
    }

  @Override
  public boolean hasNext()
    {
    if( match == null )
      {
      match = graphFinder.findMatchesOnPrimary( plannerContext, getContractedGraph(), firstOnly, excludes );

      if( match.foundMatch() )
        {
        matches.add( match );
        excludes.addAll( match.getCapturedElements( ElementExpression.Capture.Primary ) ); // idempotent
        count++;
        }
      }

    return match.foundMatch();
    }

  @Override
  public ElementSubGraph next()
    {
    try
      {
      if( !hasNext() )
        throw new NoSuchElementException();

      ElementSubGraph contractedMatchedGraph = match.getMatchedGraph();

      return ElementGraphs.asSubGraph( flowElementGraph, contractedMatchedGraph );
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
