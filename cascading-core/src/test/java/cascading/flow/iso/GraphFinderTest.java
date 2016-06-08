/*
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

package cascading.flow.iso;

import cascading.CascadingTestCase;
import cascading.flow.iso.graph.HashJoinSameSourceGraph;
import cascading.flow.iso.graph.JoinAroundJoinRightMostGraph;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.iso.expression.ElementCapture;
import cascading.flow.planner.iso.expression.ExpressionGraph;
import cascading.flow.planner.iso.expression.FlowElementExpression;
import cascading.flow.planner.iso.expression.ScopeExpression;
import cascading.flow.planner.iso.expression.TypeExpression;
import cascading.flow.planner.iso.finder.GraphFinder;
import cascading.flow.planner.iso.finder.Match;
import cascading.flow.planner.iso.transformer.ContractedTransformer;
import cascading.flow.planner.rule.expressiongraph.NoGroupTapExpressionGraph;
import cascading.flow.planner.rule.expressiongraph.SyncPipeExpressionGraph;
import cascading.pipe.HashJoin;
import cascading.tap.Tap;
import org.junit.Test;

/**
 *
 */
public class GraphFinderTest extends CascadingTestCase
  {
  @Test
  public void testFind()
    {
    ElementGraph graph = new HashJoinSameSourceGraph();

    graph = new ContractedTransformer( new SyncPipeExpressionGraph() ).transform( graph ).getEndGraph();

    FlowElementExpression SHARED_TAP = new FlowElementExpression( Tap.class, TypeExpression.Topo.SplitOnly );
    FlowElementExpression SHARED_HASHJOIN = new FlowElementExpression( HashJoin.class );

    ExpressionGraph expressionGraph = new ExpressionGraph()
      .arcs( SHARED_TAP, SHARED_HASHJOIN )
      .arcs( SHARED_TAP, SHARED_HASHJOIN );

    GraphFinder graphFinder = new GraphFinder( expressionGraph );

    Match match = graphFinder.findFirstMatch( graph );

    match.getMatchedGraph().writeDOT( getPlanPath() + "/match.dot" );
    }

  @Test
  public void testFind2()
    {
    ElementGraph graph = new HashJoinSameSourceGraph();

    graph = new ContractedTransformer( new SyncPipeExpressionGraph() ).transform( graph ).getEndGraph();

    FlowElementExpression sharedTap = new FlowElementExpression( Tap.class, TypeExpression.Topo.SplitOnly );
    FlowElementExpression sharedHashJoin = new FlowElementExpression( HashJoin.class );

    ExpressionGraph expressionGraph = new ExpressionGraph()
      .arc( sharedTap, ScopeExpression.ALL, sharedHashJoin );

    GraphFinder graphFinder = new GraphFinder( expressionGraph );

    Match match = graphFinder.findFirstMatch( graph );

    match.getMatchedGraph().writeDOT( getPlanPath() + "/match.dot" );
    }

  @Test
  public void testFindAllOnPrimary()
    {
//    ElementGraph graph = new HashJoinSameSourceGraph();
    ElementGraph graph = new JoinAroundJoinRightMostGraph();
    graph.writeDOT( getPlanPath() + "/full.dot" );

//    graph = new ContractedTransform( new SyncPipeExpressionGraph() ).transform( graph ).getEndGraph();
    graph = new ContractedTransformer( new NoGroupTapExpressionGraph() ).transform( graph ).getEndGraph();

    graph.writeDOT( getPlanPath() + "/contracted.dot" );

    ExpressionGraph expressionGraph = new ExpressionGraph()
      .arc(
        new FlowElementExpression( Tap.class ),
        ScopeExpression.ALL,
        new FlowElementExpression( ElementCapture.Primary, HashJoin.class )
      );

    GraphFinder graphFinder = new GraphFinder( expressionGraph );

    Match match = graphFinder.findAllMatchesOnPrimary( graph );

    match.getMatchedGraph().writeDOT( getPlanPath() + "/match.dot" );
    }

  @Test
  public void testFindAllMatched()
    {
//    ElementGraph graph = new HashJoinSameSourceGraph();
    ElementGraph graph = new JoinAroundJoinRightMostGraph();
    graph.writeDOT( getPlanPath() + "/full.dot" );

//    graph = new ContractedTransform( new SyncPipeExpressionGraph() ).transform( graph ).getEndGraph();
    graph = new ContractedTransformer( new NoGroupTapExpressionGraph() ).transform( graph ).getEndGraph();

    graph.writeDOT( getPlanPath() + "/contracted.dot" );

    ExpressionGraph expressionGraph = new ExpressionGraph( new FlowElementExpression( ElementCapture.Primary, Tap.class ) );

    GraphFinder graphFinder = new GraphFinder( expressionGraph );

    Match match = graphFinder.findAllMatches( graph );

    match.getMatchedGraph().writeDOT( getPlanPath() + "/match.dot" );
    }
  }
