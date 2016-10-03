/*
 * Copyright (c) 2016 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 * Copyright (c) 2007-2016 Concurrent, Inc. All Rights Reserved.
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
import cascading.flow.planner.graph.ElementGraphs;
import cascading.flow.planner.graph.ElementSubGraph;
import cascading.flow.planner.iso.expression.ExpressionGraph;
import cascading.flow.planner.iso.finder.GraphFinder;
import cascading.flow.planner.iso.finder.Match;
import cascading.flow.planner.rule.TransformException;

/**
 * Class SubGraphTransformer will return a bounded sub-graph after matching a sub-graph within a contracted graph.
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

  public Transformed<ElementSubGraph> transform( PlannerContext plannerContext, ElementGraph rootGraph )
    {
    Transformed<ElementSubGraph> transformed = new Transformed<>( plannerContext, this, subGraphMatcher, rootGraph );

    try
      {
      Transformed contractedTransformed = graphTransformer.transform( plannerContext, rootGraph ); // contracted graph transform

      transformed.addChildTransform( contractedTransformed );

      // apply contracted sub-graph matcher to get the bounded sub-graph of the original graph
      ElementGraph contractedGraph = contractedTransformed.getEndGraph();

      Match match = findAllPrimaries ? subGraphFinder.findAllMatches( plannerContext, contractedGraph ) : subGraphFinder.findFirstMatch( plannerContext, contractedGraph );

      if( !match.foundMatch() )
        return transformed;

      ElementGraph contractedSubGraph = match.getMatchedGraph();

      ElementSubGraph resultSubGraph = asSubGraphOf( rootGraph, contractedSubGraph ); // the bounded sub-graph of the rootGraph

      transformed.setEndGraph( resultSubGraph );

      return transformed;
      }
    catch( Throwable throwable )
      {
      throw new TransformException( throwable, transformed );
      }
    }

  protected ElementSubGraph asSubGraphOf( ElementGraph rootGraph, ElementGraph contractedSubGraph )
    {
    return ElementGraphs.asSubGraph( rootGraph, contractedSubGraph, null );
    }
  }
