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

package cascading.flow.planner.rule;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import cascading.flow.BaseFlow;
import cascading.flow.FlowElement;
import cascading.flow.planner.BoundElementGraph;
import cascading.flow.planner.FlowElementGraph;
import cascading.flow.planner.PlannerContext;
import cascading.flow.planner.PlannerException;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.iso.assertion.Assertion;
import cascading.flow.planner.iso.assertion.GraphAssert;
import cascading.flow.planner.iso.subgraph.Partitions;
import cascading.flow.planner.iso.transformer.GraphTransformer;
import cascading.flow.planner.iso.transformer.Transform;
import cascading.pipe.Pipe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cascading.flow.planner.rule.PlanPhase.Start;

/**
 *
 */
public class RuleExec
  {
  private static final Logger LOG = LoggerFactory.getLogger( RuleExec.class );

  private static final int ELEMENT_THRESHOLD = 600;
  final TraceWriter traceWriter = new TraceWriter();

  RuleRegistry registry;

  public RuleExec( RuleRegistry registry )
    {
    this.registry = registry;
    }

  public void enableTransformTracing( String dotPath )
    {
    this.traceWriter.transformTracePath = dotPath;
    }

  public RuleResult exec( PlannerContext plannerContext, FlowElementGraph flowElementGraph )
    {
    int size = flowElementGraph.vertexSet().size();
    boolean logAsInfo = size >= ELEMENT_THRESHOLD;

    if( logAsInfo )
      LOG.info( "elements in graph: {}, info logging threshold: {}, logging planner execution status", size, ELEMENT_THRESHOLD );

    long beginExec = System.currentTimeMillis();

    RuleResult ruleResult = new RuleResult();

    for( PlanPhase phase : PlanPhase.values() )
      {
      long beginPhase = System.currentTimeMillis();

      logPhase( logAsInfo, "starting rule phase: {}", phase );

      FlowElementGraph elementGraph = ruleResult.getPreviousElementPhaseResults( phase );

      switch( phase )
        {
        case Start:
          ruleResult.setElementsPhaseResult( Start, flowElementGraph );
          break;

        case PrePartitionElements:
        case PartitionElements:
        case PreResolveElements:
        case PostResolveElements:
          executePhase( phase, plannerContext, ruleResult, elementGraph );
          break;

        case ResolveElements:
          resolveElements( phase, plannerContext, ruleResult, elementGraph );
          break;

        case PartitionSteps:
        case PartitionNodes:
        case PipelineNodes:
          executePhase( phase, plannerContext, ruleResult, elementGraph );
          break;

        case Complete:
          break;
        }

      long endPhase = System.currentTimeMillis();

      ruleResult.setPhaseDuration( phase, beginPhase, endPhase );

      logPhase( logAsInfo, "ending rule phase: {}, duration: {} sec", phase, ( endPhase - beginPhase ) / 1000 );
      }

    long endExec = System.currentTimeMillis();

    ruleResult.setDuration( beginExec, endExec );

    logPhase( logAsInfo, "completed planner duration: {} sec", ( endExec - beginExec ) / 1000 );

    return ruleResult;
    }

  private void resolveElements( PlanPhase phase, PlannerContext plannerContext, RuleResult ruleResult, FlowElementGraph elementGraph )
    {
    elementGraph.resolveFields();
    ( (BaseFlow) plannerContext.getFlow() ).updateSchemes( elementGraph );

    elementGraph = new FlowElementGraph( elementGraph ); // forces a re-hash in graph

    ruleResult.setElementsPhaseResult( phase, elementGraph );
    }

  private void logPhase( boolean logAsInfo, String message, Object... items )
    {
    if( logAsInfo )
      LOG.info( message, items );
    else
      LOG.debug( message, items );
    }

  private static class PhaseContext
    {
    int transformRuleNum = 0;
    int stepPartitionRuleNum = 0;
    int nodePartitionsRuleNum = 0;
    int nodePipelinesRuleNum = 0;
    FlowElementGraph currentElementGraph;
    RuleResult ruleResult;
    }

  public FlowElementGraph executePhase( PlanPhase phase, PlannerContext plannerContext, RuleResult ruleResult, FlowElementGraph flowElementGraph )
    {
    LOG.debug( "executing plan phase: {}", phase );

    LinkedList<Rule> rules = registry.getRulesFor( phase );

    traceWriter.writePlan( flowElementGraph, phase.ordinal() + "-" + phase + "-init.dot" );

    PhaseContext context = new PhaseContext();

    context.ruleResult = ruleResult;
    context.currentElementGraph = flowElementGraph;

    try
      {
      for( Rule rule : rules )
        {
        long begin = System.currentTimeMillis();

        switch( phase.getStage() )
          {
          case Terminal:
            break;

          case Elements:
            handleElementsPhase( plannerContext, context, phase, rule );
            break;

          case Steps:
            handleStepsPhase( plannerContext, context, phase, rule );
            break;

          case Nodes:
            handleNodesPhase( plannerContext, context, phase, rule );
            break;
          }

        long end = System.currentTimeMillis();

        ruleResult.setRuleDuration( phase, rule, begin, end );
        }

      return flowElementGraph;
      }
    finally
      {
      LOG.debug( "completed plan phase: {}", phase );
      writePhaseResultPlan( phase, ruleResult, flowElementGraph );
      }
    }

  private void writePhaseResultPlan( PlanPhase phase, RuleResult ruleResult, FlowElementGraph flowElementGraph )
    {
    if( phase.isElements() )
      traceWriter.writePlan( flowElementGraph, phase.ordinal() + "-" + phase + "-result.dot" );
    else if( phase.isSteps() )
      traceWriter.writePlan( ruleResult.getStepSubGraphs(), phase, "result" );
    else if( phase.isNodes() && phase == PlanPhase.PartitionNodes )
      traceWriter.writePlan( ruleResult.getNodeSubGraphs(), phase, "result" );
    else if( phase.isNodes() && phase == PlanPhase.PipelineNodes )
      traceWriter.writePlan( ruleResult.getNodeSubGraphs(), ruleResult.getNodePipelineGraphs(), phase, "result" );
    }

  private void handleElementsPhase( PlannerContext plannerContext, PhaseContext context, PlanPhase phase, Rule rule )
    {
    if( rule instanceof GraphTransformer )
      performTransform( phase, context, plannerContext, (GraphTransformer) rule );
    else if( rule instanceof GraphAssert )
      performAssertion( plannerContext, context, (GraphAssert) rule );
    }

  private void handleStepsPhase( PlannerContext plannerContext, PhaseContext context, PlanPhase phase, Rule rule )
    {
    testRulePartitioner( rule );

    List<ElementGraph> stepSubGraphs = performStepPartitions( phase, context.stepPartitionRuleNum++, plannerContext, context.currentElementGraph, (RulePartitioner) rule );

    context.ruleResult.addStepSubGraphs( stepSubGraphs );
    }

  private void handleNodesPhase( PlannerContext plannerContext, PhaseContext context, PlanPhase phase, Rule rule )
    {
    switch( phase )
      {
      case PartitionNodes:
        testRulePartitioner( rule );

        List<ElementGraph> stepSubGraphs = context.ruleResult.getStepSubGraphs();
        Map<ElementGraph, List<ElementGraph>> stepNodeSubGraphs = performNodePartitions( phase, context.nodePartitionsRuleNum++, plannerContext, context.currentElementGraph, stepSubGraphs, (RulePartitioner) rule );

        context.ruleResult.addNodeSubGraphs( stepNodeSubGraphs );
        break;

      case PipelineNodes:
        testRulePartitioner( rule );

        Map<ElementGraph, List<ElementGraph>> nodeSubGraphs = context.ruleResult.getNodeSubGraphs();
        Map<ElementGraph, List<ElementGraph>> nodePipelines = performNodePipelineIsolation( phase, context.nodePipelinesRuleNum++, plannerContext, context.currentElementGraph, nodeSubGraphs, (RulePartitioner) rule );

        context.ruleResult.addNodePipelineGraphs( nodePipelines );
        break;

      case PostPipelineNodes:

        break;

      default:
        throw new IllegalStateException( "unknown phase: " + phase );
      }
    }

  private static void testRulePartitioner( Rule rule )
    {
    if( !( rule instanceof RulePartitioner ) )
      throw new PlannerException( "unexpected rule: " + rule.getRuleName() );
    }

  private void performAssertion( PlannerContext plannerContext, PhaseContext context, GraphAssert rule )
    {
    LOG.debug( "applying assertion: {}", ( (Rule) rule ).getRuleName() );

    Assertion assertion = rule.assertion( plannerContext, context.currentElementGraph );

    FlowElement primary = assertion.getFirstPrimary();

    if( primary == null )
      return;

    throw new PlannerException( (Pipe) assertion.getFirstPrimary(), assertion.getMessage() );
    }

  private void performTransform( PlanPhase phase, PhaseContext context, PlannerContext plannerContext, GraphTransformer transformer )
    {
    LOG.debug( "applying transform: {}", ( (Rule) transformer ).getRuleName() );

    Transform transform = transformer.transform( plannerContext, context.currentElementGraph );

    traceWriter.writePlan( phase, context.transformRuleNum++, transform );

    FlowElementGraph endGraph = (FlowElementGraph) transform.getEndGraph();

    if( endGraph != null )
      context.currentElementGraph = endGraph;

    context.ruleResult.setElementsPhaseResult( phase, context.currentElementGraph );
    }

  private List<ElementGraph> performStepPartitions( PlanPhase phase, int partitionRuleNum, PlannerContext plannerContext, FlowElementGraph elementGraph, RulePartitioner partitioner )
    {
    Partitions partition = partitioner.partition( plannerContext, elementGraph );

    traceWriter.writePlan( phase, partitionRuleNum, partition );

    return convert( elementGraph, partition );
    }

  private Map<ElementGraph, List<ElementGraph>> performNodePartitions( PlanPhase phase, int partitionRuleNum, PlannerContext plannerContext, FlowElementGraph elementGraph, List<ElementGraph> stepSubGraphs, RulePartitioner partitioner )
    {
    Map<ElementGraph, List<ElementGraph>> nodeSubGraphs = new LinkedHashMap<>();

    int stepCount = 0;

    for( ElementGraph stepSubGraph : stepSubGraphs )
      {
      Partitions partition = partitioner.partition( plannerContext, stepSubGraph );

      traceWriter.writePlan( phase, partitionRuleNum, stepCount++, partition );

      List<ElementGraph> results = convert( elementGraph, partition );

      nodeSubGraphs.put( stepSubGraph, results );
      }

    return nodeSubGraphs;
    }

  private Map<ElementGraph, List<ElementGraph>> performNodePipelineIsolation( PlanPhase phase, int pipelineRuleNum, PlannerContext plannerContext, FlowElementGraph elementGraph, Map<ElementGraph, List<ElementGraph>> nodeSubGraphs, RulePartitioner partitioner )
    {
    Map<ElementGraph, List<ElementGraph>> nodePipeLines = new LinkedHashMap<>();

    int nodeCount = 0;

    for( Map.Entry<ElementGraph, List<ElementGraph>> entry : nodeSubGraphs.entrySet() )
      {
      int pipelineCount = 0;
      for( ElementGraph nodeSubGraph : entry.getValue() )
        {
        Partitions partition = partitioner.partition( plannerContext, nodeSubGraph );

        traceWriter.writePlan( phase, pipelineRuleNum, nodeCount, pipelineCount++, partition );

        List<ElementGraph> results = convert( elementGraph, partition );

        nodePipeLines.put( nodeSubGraph, results );
        }

      nodeCount++;
      }

    return nodePipeLines;
    }

  private List<ElementGraph> convert( FlowElementGraph currentElementGraph, Partitions partition )
    {
    List<ElementGraph> results = new ArrayList<>( partition.getSubGraphs().size() );

    for( ElementGraph subGraph : partition.getSubGraphs() )
      results.add( new BoundElementGraph( currentElementGraph, subGraph ) );

    return results;
    }
  }
