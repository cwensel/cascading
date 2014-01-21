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
import cascading.flow.iso.expression.TestGroupGroupExpression;
import cascading.flow.iso.expression.TestHashJoinBlockingHashJoinExpression;
import cascading.flow.iso.expression.TestHashJoinSameSourceExpression;
import cascading.flow.iso.graph.HashJoinSameSourceGraph;
import cascading.flow.iso.graph.JoinAroundJoinRightMostGraph;
import cascading.flow.iso.graph.JoinAroundJoinRightMostGraphSwapped;
import cascading.flow.iso.graph.LoneGroupAssertionGraph;
import cascading.flow.iso.graph.StandardElementGraph;
import cascading.flow.planner.ElementGraphs;
import cascading.flow.planner.FlowElementGraph;
import cascading.flow.planner.PlannerContext;
import cascading.flow.planner.PlannerException;
import cascading.flow.planner.iso.assertion.Assertion;
import cascading.flow.planner.iso.expression.NoGroupTapExpressionGraph;
import cascading.flow.planner.iso.expression.TapGroupTapExpressionGraph;
import cascading.flow.planner.iso.subgraph.SubGraphIterator;
import cascading.flow.planner.iso.transformer.ElementFactory;
import cascading.flow.planner.iso.transformer.Transform;
import cascading.flow.planner.rule.PlanPhase;
import cascading.flow.planner.rule.RuleAssert;
import cascading.flow.planner.rule.RuleExec;
import cascading.flow.planner.rule.RuleExpression;
import cascading.flow.planner.rule.RuleRegistry;
import cascading.flow.planner.rule.RuleResult;
import cascading.flow.planner.rule.expression.BufferAfterEveryExpression;
import cascading.flow.planner.rule.expression.LoneGroupExpression;
import cascading.flow.planner.rule.transformer.RuleTempTapInsertionTransformer;
import org.junit.Test;

/**
 *
 */
public class IsomorphismTest extends CascadingTestCase
  {

  @Test
  public void testElementGraphs()
    {
    assertEquals( 5, ElementGraphs.findAllGroups( new StandardElementGraph() ).size() );
    assertEquals( 3, ElementGraphs.findSources( new StandardElementGraph() ).size() );
    assertEquals( 2, ElementGraphs.findSinks( new StandardElementGraph() ).size() );
    }

  @Test
  public void testSubGraphIterator()
    {
    RuleRegistry ruleRegistry = new RuleRegistry();

    ruleRegistry.addElementFactory( RuleTempTapInsertionTransformer.TEMP_TAP, new NonTapFactory() );

    PlannerContext plannerContext = new PlannerContext( ruleRegistry, null, null, null, null );

    ruleRegistry.addRule( new RuleTempTapInsertionTransformer( PlanPhase.PreResolveElements, new TestCheckpointExpression() ) );
//    ruleRegistry.addRule( new RuleContractedTransform( PlanPhase.PreResolve, new NoOpPipeExpression() ) );

    FlowElementGraph flowElementGraph = new RuleExec( ruleRegistry ).executePhase( PlanPhase.PreResolveElements, plannerContext, new RuleResult(), new StandardElementGraph() );

    SubGraphIterator iterator = new SubGraphIterator(
      new PlannerContext(),
      new NoGroupTapExpressionGraph(),
      new TapGroupTapExpressionGraph(),
      flowElementGraph );

    iterator.enableSubGraphCheckpoints( getPlanPath() );

    while( iterator.hasNext() )
      assertNotNull( iterator.next() );

    }

  @Test
  public void testRuleEngine()
    {
    RuleRegistry ruleRegistry = new RuleRegistry();

    ruleRegistry.addElementFactory( RuleTempTapInsertionTransformer.TEMP_TAP, new NonTapFactory() );

    PlannerContext plannerContext = new PlannerContext( ruleRegistry, null, null, null, null );

    ruleRegistry.addRule( new RuleAssert( PlanPhase.PreResolveElements, new LoneGroupExpression(), "lone group assertion" ) );
    ruleRegistry.addRule( new RuleTempTapInsertionTransformer( PlanPhase.PreResolveElements, new TestGroupGroupExpression() ) );

    try
      {
      new RuleExec( ruleRegistry ).executePhase( PlanPhase.PreResolveElements, plannerContext, new RuleResult(), new LoneGroupAssertionGraph() );
      fail();
      }
    catch( PlannerException exception )
      {
      // do nothing
      }

    new RuleExec( ruleRegistry ).executePhase( PlanPhase.PreResolveElements, plannerContext, new RuleResult(), new HashJoinSameSourceGraph() );
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

    Assertion assertion = new RuleAssert( PlanPhase.PreResolveElements, ruleExpression, "message" ).assertion( plannerContext, flowElementGraph );

//    assertion.getMatched().writeDOT( getTestOutputRoot() + "/dots/assertion.dot" );

    return assertion;
    }

  private FlowElementGraph transform( FlowElementGraph flowElementGraph, RuleExpression ruleExpression )
    {
    RuleRegistry ruleRegistry = new RuleRegistry();

    ruleRegistry.addElementFactory( RuleTempTapInsertionTransformer.TEMP_TAP, new NonTapFactory() );

    PlannerContext plannerContext = new PlannerContext( ruleRegistry, null, null, null, null );

    RuleTempTapInsertionTransformer ruleTempTapInsertionTransformer = new RuleTempTapInsertionTransformer( PlanPhase.PreResolveElements, ruleExpression );
    Transform<FlowElementGraph> insertionTransform = ruleTempTapInsertionTransformer.transform( plannerContext, flowElementGraph );

    insertionTransform.writeDOTs( getPlanPath() );

    return insertionTransform.getEndGraph();
    }

  private static class NonTapFactory implements ElementFactory
    {
    @Override
    public FlowElement create( FlowElementGraph graph, FlowElement flowElement )
      {
      return new NonTap();
      }
    }
  }
