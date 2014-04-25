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

package cascading.flow.iso;

import cascading.CascadingTestCase;
import cascading.flow.FlowElement;
import cascading.flow.iso.expression.TestCheckpointExpression;
import cascading.flow.iso.expression.TestConsecutiveTapsExpressionGraph;
import cascading.flow.iso.expression.TestGroupGroupExpression;
import cascading.flow.iso.expression.TestHashJoinBlockingHashJoinExpression;
import cascading.flow.iso.expression.TestHashJoinSameSourceExpression;
import cascading.flow.iso.expression.TestMalformedJoinExpressionGraph;
import cascading.flow.iso.expression.TestNoGroupTapExpressionGraph;
import cascading.flow.iso.graph.HashJoinMergeIntoHashJoinStreamedStreamedMergeGraph;
import cascading.flow.iso.graph.HashJoinSameSourceGraph;
import cascading.flow.iso.graph.HashJoinsIntoMerge;
import cascading.flow.iso.graph.JoinAroundJoinRightMostGraph;
import cascading.flow.iso.graph.JoinAroundJoinRightMostGraphSwapped;
import cascading.flow.iso.graph.LoneGroupAssertionGraph;
import cascading.flow.iso.graph.StandardElementGraph;
import cascading.flow.planner.ElementGraphs;
import cascading.flow.planner.FlowElementGraph;
import cascading.flow.planner.PlannerContext;
import cascading.flow.planner.PlannerException;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.graph.ElementSubGraph;
import cascading.flow.planner.iso.assertion.Assertion;
import cascading.flow.planner.iso.expression.NoGroupTapExpressionGraph;
import cascading.flow.planner.iso.expression.TapGroupTapExpressionGraph;
import cascading.flow.planner.iso.subgraph.SubGraphIterator;
import cascading.flow.planner.iso.transformer.ContractedTransformer;
import cascading.flow.planner.iso.transformer.ElementFactory;
import cascading.flow.planner.iso.transformer.RemoveBranchGraphTransformer;
import cascading.flow.planner.iso.transformer.Transform;
import cascading.flow.planner.rule.PlanPhase;
import cascading.flow.planner.rule.RuleAssert;
import cascading.flow.planner.rule.RuleExec;
import cascading.flow.planner.rule.RuleExpression;
import cascading.flow.planner.rule.RuleRegistry;
import cascading.flow.planner.rule.RuleResult;
import cascading.flow.planner.rule.expression.BufferAfterEveryExpression;
import cascading.flow.planner.rule.expression.LoneGroupExpression;
import cascading.flow.planner.rule.transformer.RemoveNoOpPipeTransformer;
import cascading.flow.planner.rule.transformer.RuleTempTapInsertionTransformer;
import cascading.pipe.Pipe;
import org.junit.Test;

/**
 *
 */
public class IsomorphismTest extends CascadingTestCase
  {

  @Test
  public void testElementGraphs()
    {
    StandardElementGraph standardElementGraph = new StandardElementGraph();
    assertEquals( 5, ElementGraphs.findAllGroups( standardElementGraph ).size() );
    assertEquals( 3, ElementGraphs.findSources( standardElementGraph ).size() );
    assertEquals( 2, ElementGraphs.findSinks( standardElementGraph ).size() );

    standardElementGraph.writeDOT( getPlanPath() + "/standard-before.dot" );

    Pipe pipe = ElementGraphs.findFirstPipeNamed( standardElementGraph, "remove" );

    int size = standardElementGraph.vertexSet().size();
    ElementGraphs.removeBranchContaining( standardElementGraph, pipe );

    standardElementGraph.writeDOT( getPlanPath() + "/standard-after.dot" );

    assertEquals( size - 3, standardElementGraph.vertexSet().size() );
    }

  @Test
  public void testSubGraphIterator()
    {
    RuleRegistry ruleRegistry = new RuleRegistry();

    ruleRegistry.addElementFactory( RuleTempTapInsertionTransformer.TEMP_TAP, new NonTapFactory() );

    PlannerContext plannerContext = new PlannerContext( ruleRegistry, null, null, null, null );

    ruleRegistry.addRule( new RuleTempTapInsertionTransformer( PlanPhase.PreResolveAssembly, new TestCheckpointExpression() ) );
//    ruleRegistry.addRule( new RuleContractedTransform( PlanPhase.PreResolve, new NoOpPipeExpression() ) );

    FlowElementGraph flowElementGraph = new RuleExec( ruleRegistry ).executePhase( PlanPhase.PreResolveAssembly, plannerContext, new RuleResult(), new StandardElementGraph() );

    SubGraphIterator iterator = new SubGraphIterator(
      new PlannerContext(),
      new NoGroupTapExpressionGraph(),
      new TapGroupTapExpressionGraph(),
      flowElementGraph
    );

    while( iterator.hasNext() )
      assertNotNull( iterator.next() );

    }

  @Test
  public void testSubGraphIterator2()
    {
    RuleRegistry ruleRegistry = new RuleRegistry();

    PlannerContext plannerContext = new PlannerContext( ruleRegistry );

    ruleRegistry.addRule( new RemoveNoOpPipeTransformer() );

    FlowElementGraph elementGraph = new HashJoinMergeIntoHashJoinStreamedStreamedMergeGraph();
//    FlowElementGraph elementGraph = new HashJoinAroundHashJoinLeftMostGraph();
    FlowElementGraph flowElementGraph = new RuleExec( ruleRegistry ).executePhase( PlanPhase.PreResolveAssembly, plannerContext, new RuleResult(), elementGraph );

    flowElementGraph.writeDOT( getPlanPath() + "/mergejoin.dot" );

    ContractedTransformer transformer = new ContractedTransformer( new TestNoGroupTapExpressionGraph() );

    Transform<ElementGraph> transform = transformer.transform( plannerContext, flowElementGraph );

    transform.writeDOTs( getPlanPath() + "/transform/" );

    SubGraphIterator iterator = new SubGraphIterator(
      new PlannerContext(),
      new TestNoGroupTapExpressionGraph(),
      new TestConsecutiveTapsExpressionGraph(),
      false,
      flowElementGraph
    );

    RemoveBranchGraphTransformer removeTransformer = new RemoveBranchGraphTransformer( new TestMalformedJoinExpressionGraph() );

    int count = 0;
    while( iterator.hasNext() )
      {
      ElementSubGraph next = iterator.next();
      assertNotNull( next );
      next.writeDOT( getPlanPath() + "/pipeline/" + count + "-graph.dot" );

      Transform<ElementGraph> result = removeTransformer.transform( next );

      result.getEndGraph().writeDOT( getPlanPath() + "/pipeline/" + count + "-cleaned-graph.dot" );

      count++;
      }

    }

  @Test
  public void testSubGraphIteratorHashJoinsIntoMerge()
    {
    runSubGraphIteratorRotate( new HashJoinsIntoMerge(), 2 );
    }

  @Test
  public void testSubGraphIteratorMergeIntoJoin()
    {
    runSubGraphIteratorRotate( new HashJoinMergeIntoHashJoinStreamedStreamedMergeGraph(), 2 );
    }

  private void runSubGraphIteratorRotate( FlowElementGraph elementGraph, int numSubGraphs )
    {
    RuleRegistry ruleRegistry = new RuleRegistry();

    PlannerContext plannerContext = new PlannerContext( ruleRegistry );

    ruleRegistry.addRule( new RemoveNoOpPipeTransformer() );

    FlowElementGraph flowElementGraph = new RuleExec( ruleRegistry ).executePhase( PlanPhase.PreResolveAssembly, plannerContext, new RuleResult(), elementGraph );

    flowElementGraph.writeDOT( getPlanPath() + "/node.dot" );

    SubGraphIterator iterator = new SubGraphIterator(
      new PlannerContext(),
      new TestNoGroupTapExpressionGraph(),
      new TestConsecutiveTapsExpressionGraph(),
      false,
      flowElementGraph
    );

    iterator.getContractedGraph().writeDOT( getPlanPath() + "/node-contracted.dot" );

    int count = 0;
    while( iterator.hasNext() && count < 10 )
      {
      ElementSubGraph next = iterator.next();
      assertNotNull( next );
      next.writeDOT( getPlanPath() + "/pipeline/" + count + "-graph.dot" );

      count++;
      }

    assertEquals( "wrong number of sub-graphs", numSubGraphs, count );
    }

  @Test
  public void testRuleEngine()
    {
    RuleRegistry ruleRegistry = new RuleRegistry();

    ruleRegistry.addElementFactory( RuleTempTapInsertionTransformer.TEMP_TAP, new NonTapFactory() );

    PlannerContext plannerContext = new PlannerContext( ruleRegistry, null, null, null, null );

    ruleRegistry.addRule( new RuleAssert( PlanPhase.PreResolveAssembly, new LoneGroupExpression(), "lone group assertion" ) );
    ruleRegistry.addRule( new RuleTempTapInsertionTransformer( PlanPhase.PreResolveAssembly, new TestGroupGroupExpression() ) );

    try
      {
      new RuleExec( ruleRegistry ).executePhase( PlanPhase.PreResolveAssembly, plannerContext, new RuleResult(), new LoneGroupAssertionGraph() );
      fail();
      }
    catch( PlannerException exception )
      {
      // do nothing
      }

    new RuleExec( ruleRegistry ).executePhase( PlanPhase.PreResolveAssembly, plannerContext, new RuleResult(), new HashJoinSameSourceGraph() );
    }

  @Test
  public void testLoneGroupAssertion()
    {
    assertion( new LoneGroupAssertionGraph(), new LoneGroupExpression() );
    }

  @Test
  public void testStandardIsoTransform()
    {
    transform( new StandardElementGraph(), new TestGroupGroupExpression() );
    }

  @Test
  public void testHashJoinSameSourceGraphTransform()
    {
    transform( new HashJoinSameSourceGraph(), new TestHashJoinSameSourceExpression() );
    }

  @Test
  public void testJoinAroundJoinRightMostGraphTransform()
    {
    transform( new JoinAroundJoinRightMostGraph(), new TestHashJoinBlockingHashJoinExpression() );
    }

  @Test
  public void testJoinAroundJoinRightMostGraphTransformUsingSameSourceRule()
    {
    transform( new JoinAroundJoinRightMostGraph(), new TestHashJoinSameSourceExpression() );
    }

  @Test
  public void testJoinAroundJoinRightMostGraphSwappedTransform()
    {
    transform( new JoinAroundJoinRightMostGraphSwapped(), new TestHashJoinBlockingHashJoinExpression() );
    }

  @Test
  public void testNoPipeExpressionTransform()
    {
    transform( new JoinAroundJoinRightMostGraphSwapped(), new BufferAfterEveryExpression() );
    }

  private Assertion assertion( FlowElementGraph flowElementGraph, RuleExpression ruleExpression )
    {
    RuleRegistry ruleRegistry = new RuleRegistry();

    ruleRegistry.addElementFactory( RuleTempTapInsertionTransformer.TEMP_TAP, new NonTapFactory() );

    PlannerContext plannerContext = new PlannerContext( ruleRegistry, null, null, null, null );

    Assertion assertion = new RuleAssert( PlanPhase.PreResolveAssembly, ruleExpression, "message" ).assertion( plannerContext, flowElementGraph );

//    assertion.getMatched().writeDOT( getTestOutputRoot() + "/dots/assertion.dot" );

    return assertion;
    }

  private ElementGraph transform( ElementGraph flowElementGraph, RuleExpression ruleExpression )
    {
    RuleRegistry ruleRegistry = new RuleRegistry();

    ruleRegistry.addElementFactory( RuleTempTapInsertionTransformer.TEMP_TAP, new NonTapFactory() );

    PlannerContext plannerContext = new PlannerContext( ruleRegistry, null, null, null, null );

    RuleTempTapInsertionTransformer ruleTempTapInsertionTransformer = new RuleTempTapInsertionTransformer( PlanPhase.PreResolveAssembly, ruleExpression );
    Transform<ElementGraph> insertionTransform = ruleTempTapInsertionTransformer.transform( plannerContext, flowElementGraph );

    insertionTransform.writeDOTs( getPlanPath() );

    return insertionTransform.getEndGraph();
    }

  private static class NonTapFactory implements ElementFactory
    {
    @Override
    public FlowElement create( ElementGraph graph, FlowElement flowElement )
      {
      return new NonTap();
      }
    }
  }
