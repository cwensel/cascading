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

import cascading.flow.planner.ElementGraphs;
import cascading.flow.planner.PlannerContext;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.graph.ElementSubGraph;
import cascading.flow.planner.iso.expression.ExpressionGraph;
import cascading.flow.planner.iso.finder.GraphFinder;
import cascading.flow.planner.iso.finder.Match;

/**
 *
 */
public class SubGraphTransformer extends GraphTransformer<ElementGraph, ElementSubGraph>
  {
  private GraphTransformer graphTransformer;
  private ExpressionGraph subGraphMatcher;
  private GraphFinder subGraphFinder;
  private boolean findAllPrimaries = false;

  public SubGraphTransformer( GraphTransformer graphTransformer, ExpressionGraph subGraphMatcher )
    {
    this.graphTransformer = graphTransformer;
    this.subGraphMatcher = subGraphMatcher;
    this.subGraphFinder = new GraphFinder( subGraphMatcher );
    this.findAllPrimaries = subGraphMatcher.supportsNonRecursiveMatch();
    }

  public Transform<ElementSubGraph> transform( PlannerContext plannerContext, ElementGraph rootGraph )
    {
    Transform<ElementSubGraph> transform = new Transform<>( plannerContext, this, subGraphMatcher, rootGraph );

    Transform contractedTransform = graphTransformer.transform( plannerContext, rootGraph ); // contracted graph transform

    transform.addChildTransform( contractedTransform );

    // apply contracted sub-graph matcher to get the bounded sub-graph of the original graph
    ElementGraph contractedGraph = contractedTransform.getEndGraph();

    Match match = findAllPrimaries ? subGraphFinder.findAllMatches( plannerContext, contractedGraph ) : subGraphFinder.findFirstMatch( plannerContext, contractedGraph );

    if( !match.foundMatch() )
      return transform;

    ElementGraph contractedSubGraph = match.getMatchedGraph();

    ElementSubGraph resultSubGraph = asSubGraphOf( rootGraph, contractedSubGraph ); // the bounded sub-graph of the rootGraph

    transform.setEndGraph( resultSubGraph );

    return transform;
    }

  protected ElementSubGraph asSubGraphOf( ElementGraph rootGraph, ElementGraph contractedSubGraph )
    {
    return ElementGraphs.asSubGraph( rootGraph, contractedSubGraph );
    }
  }