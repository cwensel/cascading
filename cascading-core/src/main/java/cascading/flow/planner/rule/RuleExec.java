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
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cascading.flow.BaseFlow;
import cascading.flow.FlowElement;
import cascading.flow.planner.FlowElementGraph;
import cascading.flow.planner.PlannerContext;
import cascading.flow.planner.PlannerException;
import cascading.flow.planner.graph.AnnotatedGraph;
import cascading.flow.planner.graph.BoundedElementMultiGraph;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.iso.assertion.Asserted;
import cascading.flow.planner.iso.assertion.GraphAssert;
import cascading.flow.planner.iso.subgraph.Partitions;
import cascading.flow.planner.iso.transformer.GraphTransformer;
import cascading.flow.planner.iso.transformer.Transformed;
import cascading.util.EnumMultiMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cascading.flow.planner.rule.PlanPhase.Start;
import static java.lang.String.format;

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

      FlowElementGraph elementGraph = ruleResult.getPreviousAssemblyResults( phase );

      switch( phase )
        {
        case Start:
          ruleResult.setAssemblyResults( Start, flowElementGraph );
          break;

        case PreBalanceAssembly:
        case BalanceAssembly:
        case PostBalanceAssembly:
        case PreResolveAssembly:
        case PostResolveAssembly:
          executePhase( phase, plannerContext, ruleResult, elementGraph );
          break;

        case ResolveAssembly:
          resolveElements( phase, plannerContext, ruleResult, elementGraph );
          break;

        case PartitionSteps:
        case PartitionNodes:
        case PartitionPipelines:
        case PostPipelines:
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
    if( !registry.enabledResolveElements() )
      {
      ruleResult.setAssemblyResults( phase, elementGraph );
      return;
      }

    elementGraph.resolveFields();
    ( (BaseFlow) plannerContext.getFlow() ).updateSchemes( elementGraph );

    elementGraph = new FlowElementGraph( elementGraph ); // forces a re-hash in graph

    ruleResult.setAssemblyResults( phase, elementGraph );
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
    Map<PlanPhase.Stage, Set<Rule>> counts = new EnumMap<>( PlanPhase.Stage.class );

    FlowElementGraph currentElementGraph;
    RuleResult ruleResult;

    int addRule( Rule rule )
      {
      return addRule( rule.getRulePhase().getStage(), rule );
      }

    int addRule( PlanPhase.Stage stage, Rule rule )
      {
      if( !counts.containsKey( stage ) )
        counts.put( stage, new LinkedHashSet<Rule>() );

      Set<Rule> rules = counts.get( stage );

      rules.add( rule );

      return rules.size() - 1;
      }

    }

  public RuleResult executePhase( PlanPhase phase, PlannerContext plannerContext, RuleResult ruleResult, FlowElementGraph flowElementGraph )
    {
    LOG.debug( "executing plan phase: {}", phase );

    LinkedList<Rule> rules = registry.getRulesFor( phase );

    writePhaseInitPlan( phase, flowElementGraph );

    PhaseContext context = new PhaseContext();

    context.ruleResult = ruleResult;
    context.currentElementGraph = flowElementGraph;

    try
      {
      for( Rule rule : rules )
        {
        long begin = System.currentTimeMillis();

        try
          {
          switch( phase.getStage() )
            {
            case Terminal:
              break;

            case Assembly:
              handleAssemblyPhase( plannerContext, context, phase, rule );
              break;

            case Steps:
              handleStepsPhase( plannerContext, context, phase, rule );
              break;

            case Nodes:
              handleNodesPhase( plannerContext, context, phase, rule );
              break;

            case Pipelines:
              handlePipelinesPhase( plannerContext, context, phase, rule );
              break;
            }
          }
        catch( Exception exception )
          {
          throw new PlannerException( rule, exception );
          }

        long end = System.currentTimeMillis();

        ruleResult.setRuleDuration( phase, rule, begin, end );
        }

      return context.ruleResult;
      }
    finally
      {
      LOG.debug( "completed plan phase: {}", phase );
      writePhaseResultPlan( phase, ruleResult, flowElementGraph );
      }
    }

  private void handleAssemblyPhase( PlannerContext plannerContext, PhaseContext context, PlanPhase phase, Rule rule )
    {
    if( rule instanceof GraphTransformer )
      performAssemblyTransform( plannerContext, context, phase, (GraphTransformer) rule );
    else if( rule instanceof GraphAssert )
      performAssemblyAssertion( plannerContext, context, (GraphAssert) rule );
    }

  private void handleStepsPhase( PlannerContext plannerContext, PhaseContext context, PlanPhase phase, Rule rule )
    {
    testRulePartitioner( rule );

    List<ElementGraph> stepSubGraphs = performStepPartitions( phase, context.addRule( rule ), plannerContext, context.currentElementGraph, (RulePartitioner) rule );

    context.ruleResult.addStepSubGraphs( stepSubGraphs );
    }

  private void handleNodesPhase( PlannerContext plannerContext, PhaseContext context, PlanPhase phase, Rule rule )
    {
    testRulePartitioner( rule );

    List<ElementGraph> stepSubGraphs = context.ruleResult.getStepSubGraphResults();
    Map<ElementGraph, List<ElementGraph>> priorResults = context.ruleResult.getNodeSubGraphResults();
    Map<ElementGraph, List<ElementGraph>> stepNodeSubGraphs = performNodePartitions( phase, context.addRule( rule ), plannerContext, context.currentElementGraph, stepSubGraphs, priorResults, (RulePartitioner) rule );

    context.ruleResult.addNodeSubGraphs( stepNodeSubGraphs );
    }

  private void handlePipelinesPhase( PlannerContext plannerContext, PhaseContext context, PlanPhase phase, Rule rule )
    {
    switch( phase )
      {
      case PartitionPipelines:
        testRulePartitioner( rule );

        Map<ElementGraph, List<ElementGraph>> nodeSubGraphs = context.ruleResult.getNodeSubGraphResults();
        Map<ElementGraph, List<ElementGraph>> priorNodePipelineGraphs = context.ruleResult.getNodePipelineGraphResults();
        Map<ElementGraph, List<ElementGraph>> nodePipelines = performNodePipelinePartitions( phase, context.addRule( rule ), plannerContext, context.currentElementGraph, nodeSubGraphs, priorNodePipelineGraphs, (RulePartitioner) rule );

        context.ruleResult.addNodePipelineGraphs( nodePipelines );
        break;

      case PostPipelines:

        if( rule instanceof GraphTransformer )
          performNodePipelineTransform( phase, context, plannerContext, (GraphTransformer) rule );
        else if( rule instanceof GraphAssert )
          performNodePipelineAssertion( phase, context, plannerContext, (GraphAssert) rule );

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

  private void performAssemblyAssertion( PlannerContext plannerContext, PhaseContext context, GraphAssert rule )
    {
    LOG.debug( "applying assertion: {}", ( (Rule) rule ).getRuleName() );

    Asserted asserted = rule.assertion( plannerContext, context.currentElementGraph );

    FlowElement primary = asserted.getFirstAnchor();

    if( primary == null )
      return;

    throw new PlannerException( asserted.getFirstAnchor(), asserted.getMessage() );
    }

  private void performAssemblyTransform( PlannerContext plannerContext, PhaseContext context, PlanPhase phase, GraphTransformer transformer )
    {
    LOG.debug( "applying transform: {}", ( (Rule) transformer ).getRuleName() );

    Transformed transformed = transformer.transform( plannerContext, context.currentElementGraph );

    traceWriter.writePlan( phase, context.addRule( (Rule) transformer ), transformed );

    FlowElementGraph endGraph = (FlowElementGraph) transformed.getEndGraph();

    if( endGraph != null )
      context.currentElementGraph = endGraph;

    context.ruleResult.setAssemblyResults( phase, context.currentElementGraph );
    }

  private List<ElementGraph> performStepPartitions( PlanPhase phase, int partitionRuleNum, PlannerContext plannerContext, FlowElementGraph elementGraph, RulePartitioner partitioner )
    {
    Set<FlowElement> exclusions = getExclusions( Collections.<ElementGraph>singletonList( elementGraph ), partitioner.getAnnotationExcludes() );

    Partitions partition = partitioner.partition( plannerContext, elementGraph, exclusions );

    traceWriter.writePlan( phase, partitionRuleNum, partition );

    return makeBoundedOn( elementGraph, partition.getAnnotatedSubGraphs() );
    }

  private Map<ElementGraph, List<ElementGraph>> performNodePartitions( PlanPhase phase, int partitionRuleNum, PlannerContext plannerContext, FlowElementGraph elementGraph, List<ElementGraph> stepSubGraphs, Map<ElementGraph, List<ElementGraph>> priorResults, RulePartitioner partitioner )
    {
    Map<ElementGraph, List<ElementGraph>> nodeSubGraphs = new LinkedHashMap<>();

    int stepCount = 0;

    for( ElementGraph stepSubGraph : stepSubGraphs )
      {
      Set<FlowElement> exclusions = getExclusions( priorResults.get( stepSubGraph ), partitioner.getAnnotationExcludes() );

      Partitions partition = partitioner.partition( plannerContext, stepSubGraph, exclusions );

      traceWriter.writePlan( phase, partitionRuleNum, stepCount++, partition );

      List<ElementGraph> results = makeBoundedOn( elementGraph, partition.getAnnotatedSubGraphs() );

      nodeSubGraphs.put( stepSubGraph, results );
      }

    return nodeSubGraphs;
    }

  private Map<ElementGraph, List<ElementGraph>> performNodePipelinePartitions( PlanPhase phase, int pipelineRuleNum, PlannerContext plannerContext, FlowElementGraph elementGraph, Map<ElementGraph, List<ElementGraph>> nodeSubGraphs, Map<ElementGraph, List<ElementGraph>> priorResults, RulePartitioner partitioner )
    {
    Map<ElementGraph, List<ElementGraph>> nodePipeLines = new LinkedHashMap<>();

    int stepCount = 0;

    for( Map.Entry<ElementGraph, List<ElementGraph>> entry : nodeSubGraphs.entrySet() )
      {
      int nodeCount = 0;

      for( ElementGraph nodeSubGraph : entry.getValue() )
        {
        Set<FlowElement> exclusions = getExclusions( priorResults.get( nodeSubGraph ), partitioner.getAnnotationExcludes() );

        Partitions partitions = partitioner.partition( plannerContext, nodeSubGraph, exclusions );

        traceWriter.writePlan( phase, pipelineRuleNum, stepCount, nodeCount++, partitions );

        List<ElementGraph> results = makeBoundedOn( elementGraph, partitions.getAnnotatedSubGraphs() );

        nodePipeLines.put( nodeSubGraph, results );
        }

      stepCount++;
      }

    return nodePipeLines;
    }

  private Set<FlowElement> getExclusions( List<ElementGraph> elementGraphs, Enum[] annotationExcludes )
    {
    if( elementGraphs == null )
      return null;

    Set<FlowElement> exclusions = new HashSet<>();

    for( ElementGraph elementGraph : elementGraphs )
      {
      if( !( elementGraph instanceof AnnotatedGraph ) || !( (AnnotatedGraph) elementGraph ).hasAnnotations() )
        continue;

      for( Enum annotationExclude : annotationExcludes )
        {
        Set<FlowElement> flowElements = ( (AnnotatedGraph) elementGraph ).getAnnotations().getValues( annotationExclude );

        if( flowElements != null )
          exclusions.addAll( flowElements );
        }
      }

    return exclusions;
    }

  private void performNodePipelineTransform( PlanPhase phase, PhaseContext context, PlannerContext plannerContext, GraphTransformer transformer )
    {
    LOG.debug( "applying pipeline transform: {}", ( (Rule) transformer ).getRuleName() );

    int ruleOrdinal = context.addRule( (Rule) transformer );

    Map<ElementGraph, List<ElementGraph>> results = new LinkedHashMap<>();
    Map<ElementGraph, List<ElementGraph>> nodeSubGraphs = context.ruleResult.getNodeSubGraphResults();
    Map<ElementGraph, List<ElementGraph>> nodePipelineGraphs = context.ruleResult.getNodePipelineGraphResults();

    int stepCount = 0;

    for( Map.Entry<ElementGraph, List<ElementGraph>> stepEntry : nodeSubGraphs.entrySet() )
      {
      int nodeCount = 0;

      List<ElementGraph> nodeGraphs = stepEntry.getValue();

      for( ElementGraph nodeGraph : nodeGraphs )
        {
        List<ElementGraph> pipelineGraphs = nodePipelineGraphs.get( nodeGraph );

        Map<ElementGraph, EnumMultiMap> resultPipelines = new LinkedHashMap<>( pipelineGraphs.size() );

        int pipelineCount = 0;

        for( ElementGraph pipelineGraph : pipelineGraphs )
          {
          Transformed transformed = transformer.transform( plannerContext, pipelineGraph );

          traceWriter.writePlan( phase, ruleOrdinal, stepCount, nodeCount, pipelineCount++, transformed );

          ElementGraph endGraph = transformed.getEndGraph();

          if( endGraph != null )
            resultPipelines.put( endGraph, ( (AnnotatedGraph) pipelineGraph ).getAnnotations() );
          else
            resultPipelines.put( pipelineGraph, null );
          }

        results.put( nodeGraph, makeBoundedOn( context.currentElementGraph, resultPipelines ) );
        nodeCount++;
        }
      }

    context.ruleResult.setNodePipelineGraphResults( results );
    }

  private void performNodePipelineAssertion( PlanPhase phase, PhaseContext context, PlannerContext plannerContext, GraphAssert rule )
    {
    LOG.debug( "applying assertion: {}", ( (Rule) rule ).getRuleName() );

    int ruleOrdinal = context.addRule( (Rule) rule );

    Map<ElementGraph, List<ElementGraph>> nodeSubGraphs = context.ruleResult.getNodeSubGraphResults();
    Map<ElementGraph, List<ElementGraph>> nodePipelineGraphs = context.ruleResult.getNodePipelineGraphResults();

    int stepCount = 0;

    for( Map.Entry<ElementGraph, List<ElementGraph>> stepEntry : nodeSubGraphs.entrySet() )
      {
      int nodeCount = 0;

      List<ElementGraph> nodeGraphs = stepEntry.getValue();

      for( ElementGraph nodeGraph : nodeGraphs )
        {

        int pipelineCount = 0;

        List<ElementGraph> pipelineGraphs = nodePipelineGraphs.get( nodeGraph );

        for( ElementGraph pipelineGraph : pipelineGraphs )
          {
          Asserted asserted = rule.assertion( plannerContext, pipelineGraph );

          traceWriter.writePlan( phase, ruleOrdinal, stepCount, nodeCount, pipelineCount++, asserted );

          FlowElement primary = asserted.getFirstAnchor();

          if( primary == null )
            continue;

          throw new PlannerException( asserted.getFirstAnchor(), asserted.getMessage() );
          }
        }
      }
    }


  // use the final assembly graph so we can get Scopes for heads and tails
  private List<ElementGraph> makeBoundedOn( ElementGraph currentElementGraph, Map<ElementGraph, EnumMultiMap> subGraphs )
    {
    List<ElementGraph> results = new ArrayList<>( subGraphs.size() );

    for( ElementGraph subGraph : subGraphs.keySet() )
      results.add( new BoundedElementMultiGraph( currentElementGraph, subGraph, subGraphs.get( subGraph ) ) );

    return results;
    }

  private void writePhaseInitPlan( PlanPhase phase, FlowElementGraph flowElementGraph )
    {
    if( phase.isAssembly() )
      traceWriter.writePlan( flowElementGraph, format( "%02d-%s-init.dot", phase.ordinal(), phase ) );
    }

  private void writePhaseResultPlan( PlanPhase phase, RuleResult ruleResult, FlowElementGraph flowElementGraph )
    {
    if( phase.isAssembly() )
      traceWriter.writePlan( flowElementGraph, format( "%02d-%s-result.dot", phase.ordinal(), phase ) );
    else if( phase.isSteps() )
      traceWriter.writePlan( ruleResult.getStepSubGraphResults(), phase, "result" );
    else if( phase == PlanPhase.PartitionNodes )
      traceWriter.writePlan( ruleResult.getNodeSubGraphResults(), phase, "result" );
    else if( phase == PlanPhase.PartitionPipelines )
      traceWriter.writePlan( ruleResult.getNodeSubGraphResults(), ruleResult.getNodePipelineGraphResults(), phase, "result" );
    }
  }
