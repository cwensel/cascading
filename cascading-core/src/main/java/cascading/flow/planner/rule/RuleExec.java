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

package cascading.flow.planner.rule;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cascading.flow.FlowElement;
import cascading.flow.planner.PlannerContext;
import cascading.flow.planner.PlannerException;
import cascading.flow.planner.graph.AnnotatedGraph;
import cascading.flow.planner.graph.BoundedElementMultiGraph;
import cascading.flow.planner.graph.ElementDirectedGraph;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.graph.FlowElementGraph;
import cascading.flow.planner.graph.IgnoreAnnotationsHashSet;
import cascading.flow.planner.iso.GraphResult;
import cascading.flow.planner.iso.assertion.Asserted;
import cascading.flow.planner.iso.assertion.GraphAssert;
import cascading.flow.planner.iso.subgraph.Partitions;
import cascading.flow.planner.iso.transformer.GraphTransformer;
import cascading.flow.planner.iso.transformer.Transformed;
import cascading.flow.planner.rule.util.TraceWriter;
import cascading.util.EnumMultiMap;
import cascading.util.ProcessLogger;

import static cascading.util.Util.formatDurationFromMillis;
import static java.lang.String.format;

/**
 *
 */
public class RuleExec
  {
  private static final int ELEMENT_THRESHOLD = 600;

  final TraceWriter traceWriter;
  final RuleRegistry registry;

  public RuleExec( TraceWriter traceWriter, RuleRegistry registry )
    {
    this.traceWriter = traceWriter;
    this.registry = registry;
    }

  public RuleResult exec( PlannerContext plannerContext, FlowElementGraph flowElementGraph )
    {
    RuleResult ruleResult = new RuleResult( registry, flowElementGraph );

    ProcessLogger logger = plannerContext.getLogger();
    int size = flowElementGraph.vertexSet().size();
    boolean logAsInfo = size >= ELEMENT_THRESHOLD;

    if( logAsInfo )
      logger.logInfo( "elements in graph: {}, info logging threshold: {}, logging planner execution status", size, ELEMENT_THRESHOLD );

    long beginExec = System.currentTimeMillis();

    try
      {
      planPhases( plannerContext, logAsInfo, ruleResult );
      }
    catch( Exception exception )
      {
      ruleResult.setPlannerException( exception );
      }
    finally
      {
      long endExec = System.currentTimeMillis();

      ruleResult.setDuration( beginExec, endExec );

      RuleResult.ResultStatus status = ruleResult.getResultStatus();
      String duration = formatDurationFromMillis( endExec - beginExec );
      logPhase( logger, logAsInfo, "rule registry completed: {}, with status: {}, and duration: {}", registry.getName(), status, duration );
      }

    return ruleResult;
    }

  protected void planPhases( PlannerContext plannerContext, boolean logAsInfo, RuleResult ruleResult )
    {
    ProcessLogger logger = plannerContext.getLogger();

    for( PlanPhase phase : PlanPhase.values() ) // iterate in order, all planner phases
      {
      long beginPhase = System.currentTimeMillis();

      logPhase( logger, logAsInfo, "starting rule phase: {}", phase );

      try
        {
        switch( phase.getAction() )
          {
          case Resolve:
            resolveElements( ruleResult );
            break;

          case Rule:
            executeRulePhase( phase, plannerContext, ruleResult );
            break;
          }
        }
      finally
        {
        long endPhase = System.currentTimeMillis();

        ruleResult.setPhaseDuration( phase, beginPhase, endPhase );

        logPhase( logger, logAsInfo, "ending rule phase: {}, duration: {}", phase, formatDurationFromMillis( endPhase - beginPhase ) );
        }
      }
    }

  private void resolveElements( RuleResult ruleResult )
    {
    if( !registry.enabledResolveElements() )
      return;

    FlowElementGraph elementGraph = ruleResult.getAssemblyGraph();

    elementGraph = (FlowElementGraph) elementGraph.copyElementGraph();

    elementGraph.resolveFields();

    ruleResult.setLevelResults( ProcessLevel.Assembly, ruleResult.initialAssembly, elementGraph );
    }

  public RuleResult executeRulePhase( PlanPhase phase, PlannerContext plannerContext, RuleResult ruleResult )
    {
    ProcessLogger logger = plannerContext.getLogger();

    logger.logDebug( "executing plan phase: {}", phase );

    LinkedList<Rule> rules = registry.getRulesFor( phase );

    writePhaseInitPlan( phase, ruleResult );

    try
      {
      // within this phase, execute all rules in declared order
      for( Rule rule : rules )
        {
        logger.logDebug( "executing rule: {}", rule );

        long begin = System.currentTimeMillis();

        try
          {
          switch( phase.getMode() )
            {
            case Mutate:
              performMutation( plannerContext, ruleResult, phase, rule );
              break;

            case Partition:
              performPartition( plannerContext, ruleResult, phase, rule );
              break;
            }
          }
        catch( UnsupportedPlanException exception )
          {
          logger.logDebug( "executing rule failed: {}, message: {}", rule, exception.getMessage() );

          throw new UnsupportedPlanException( rule, exception );
          }
        catch( PlannerException exception )
          {
          logger.logDebug( "executing rule failed: {}, message: {}", rule, exception.getMessage() );

          throw exception; // rethrow
          }
        catch( Exception exception )
          {
          logger.logDebug( "executing rule failed: {}, message: {}", rule, exception.getMessage() );

          throw new PlannerException( registry, phase, rule, exception );
          }
        finally
          {
          long end = System.currentTimeMillis();

          ruleResult.setRuleDuration( rule, begin, end );

          logger.logDebug( "completed rule: {}", rule );
          }
        }

      return ruleResult;
      }
    finally
      {
      logger.logDebug( "completed plan phase: {}", phase );
      writePhaseResultPlan( phase, ruleResult );
      }
    }

  protected void performMutation( PlannerContext plannerContext, RuleResult ruleResult, PlanPhase phase, Rule rule )
    {
    if( rule instanceof GraphTransformer )
      performTransform( plannerContext, ruleResult, phase, (GraphTransformer) rule );
    else if( rule instanceof GraphAssert )
      performAssertion( plannerContext, ruleResult, phase, (GraphAssert) rule );
    else
      throw new PlannerException( "unexpected rule: " + rule.getRuleName() );
    }

  private void performPartition( PlannerContext plannerContext, RuleResult ruleResult, PlanPhase phase, Rule rule )
    {
    if( !( rule instanceof RulePartitioner ) )
      throw new PlannerException( "unexpected rule: " + rule.getRuleName() );

    RulePartitioner partitioner = (RulePartitioner) rule;

    if( partitioner.getPartitionSource() == RulePartitioner.PartitionSource.PartitionParent )
      handleParentPartitioning( plannerContext, ruleResult, phase, partitioner );
    else if( partitioner.getPartitionSource() == RulePartitioner.PartitionSource.PartitionCurrent )
      handleCurrentPartitioning( plannerContext, ruleResult, phase, partitioner );
    else
      throw new IllegalStateException( "unknown partitioning type: " + partitioner.getPartitionSource() );
    }

  private void handleCurrentPartitioning( PlannerContext plannerContext, RuleResult ruleResult, PlanPhase phase, RulePartitioner partitioner )
    {
    Map<ElementGraph, List<? extends ElementGraph>> priorResults = ruleResult.getLevelResults( phase.getLevel() );

    Map<ElementGraph, List<? extends ElementGraph>> subGraphs = new LinkedHashMap<>();

    for( Map.Entry<ElementGraph, List<? extends ElementGraph>> entry : priorResults.entrySet() )
      {
      ElementGraph parent = entry.getKey();
      List<? extends ElementGraph> priors = entry.getValue();

      List<ElementGraph> resultChildren = new ArrayList<>( priors );

      Set<FlowElement> exclusions = getExclusions( priors, partitioner.getAnnotationExcludes() );

      for( ElementGraph child : priors )
        {
        ElementGraph priorAnnotated = annotateWithPriors( child, priors );

        Partitions partitions;

        try
          {
          partitions = partitioner.partition( plannerContext, priorAnnotated, exclusions );
          }
        catch( Throwable throwable )
          {
          throw new PlannerException( registry, phase, partitioner, priorAnnotated, throwable );
          }

        writeTransformTrace( ruleResult, phase, partitioner, parent, child, partitions );

        List<ElementGraph> results = makeBoundedOn( ruleResult.getAssemblyGraph(), partitions.getAnnotatedSubGraphs() );

        if( results.isEmpty() )
          continue;

        // ignore annotations on equality, but replace an newer graph with prior
        IgnoreAnnotationsHashSet uniques = new IgnoreAnnotationsHashSet( results );

        if( uniques.size() != results.size() )
          throw new PlannerException( "rule created duplicate element graphs" );

        // replace child with partitioned results
        resultChildren.remove( child );

        for( ElementGraph prior : resultChildren )
          {
          if( !uniques.add( prior ) ) // todo: setting to force failure on duplicates
            plannerContext.getLogger().logDebug( "re-partition rule created duplicate element graph to prior partitioner: {}, replacing duplicate result", partitioner.getRuleName() );
          }

        // order no longer preserved
        resultChildren = uniques.asList();
        }

      subGraphs.put( parent, resultChildren );
      }

    ruleResult.setLevelResults( phase.getLevel(), subGraphs );
    }

  private void handleParentPartitioning( PlannerContext plannerContext, RuleResult ruleResult, PlanPhase phase, RulePartitioner partitioner )
    {
    Map<ElementGraph, List<? extends ElementGraph>> priorResults = ruleResult.getLevelResults( phase.getLevel() );

    Map<ElementGraph, List<? extends ElementGraph>> subGraphs = new LinkedHashMap<>();

    for( Map.Entry<ElementGraph, List<? extends ElementGraph>> entry : priorResults.entrySet() )
      {
      ElementGraph parent = entry.getKey();
      List<? extends ElementGraph> priors = entry.getValue();

      Set<FlowElement> exclusions = getExclusions( priors, partitioner.getAnnotationExcludes() );
      ElementGraph priorAnnotated = annotateWithPriors( parent, priors );

      Partitions partitions;

      try
        {
        partitions = partitioner.partition( plannerContext, priorAnnotated, exclusions );
        }
      catch( Throwable throwable )
        {
        throw new PlannerException( registry, phase, partitioner, priorAnnotated, throwable );
        }

      writeTransformTrace( ruleResult, phase, partitioner, parent, null, partitions );

      List<ElementGraph> results = makeBoundedOn( ruleResult.getAssemblyGraph(), partitions.getAnnotatedSubGraphs() );

      // ignore annotations on equality, but replace an newer graph with prior
      IgnoreAnnotationsHashSet uniques = new IgnoreAnnotationsHashSet( results );

      if( uniques.size() != results.size() )
        throw new PlannerException( "rule created duplicate element graphs" );

      for( ElementGraph prior : priors )
        {
        if( !uniques.add( prior ) ) // todo: setting to force failure on duplicates
          plannerContext.getLogger().logDebug( "partition rule created duplicate element graph to prior partitioner: {}, replacing duplicate result", partitioner.getRuleName() );
        }

      // order no longer preserved
      subGraphs.put( parent, uniques.asList() );
      }

    ruleResult.setLevelResults( phase.getLevel(), subGraphs );
    }

  private void performAssertion( PlannerContext plannerContext, RuleResult ruleResult, PlanPhase phase, GraphAssert asserter )
    {
    plannerContext.getLogger().logDebug( "applying assertion: {}", ( (Rule) asserter ).getRuleName() );

    Map<ElementGraph, List<? extends ElementGraph>> levelResults = ruleResult.getLevelResults( phase.getLevel() );

    for( Map.Entry<ElementGraph, List<? extends ElementGraph>> entry : levelResults.entrySet() )
      {
      ElementGraph parent = entry.getKey(); // null for root case
      List<? extends ElementGraph> children = entry.getValue();

      for( ElementGraph child : children )
        {
        Asserted asserted;
        try
          {
          asserted = asserter.assertion( plannerContext, child );
          }
        catch( Throwable throwable )
          {
          throw new PlannerException( registry, phase, (Rule) asserter, child, throwable );
          }

        writeTransformTrace( ruleResult, phase, (Rule) asserter, parent, child, asserted );

        FlowElement primary = asserted.getFirstAnchor();

        if( primary == null )
          continue;

        if( asserted.getAssertionType() == GraphAssert.AssertionType.Unsupported )
          throw new UnsupportedPlanException( asserted.getFirstAnchor(), asserted.getMessage() );
        else // only two options
          throw new PlannerException( asserted.getFirstAnchor(), asserted.getMessage() );
        }
      }
    }

  private void performTransform( PlannerContext plannerContext, RuleResult ruleResult, PlanPhase phase, GraphTransformer transformer )
    {
    plannerContext.getLogger().logDebug( "applying transform: {}", ( (Rule) transformer ).getRuleName() );

    Map<ElementGraph, List<? extends ElementGraph>> levelResults = ruleResult.getLevelResults( phase.getLevel() );

    for( Map.Entry<ElementGraph, List<? extends ElementGraph>> entry : levelResults.entrySet() )
      {
      ElementGraph parent = entry.getKey(); // null for root case
      List<? extends ElementGraph> children = entry.getValue();

      List<ElementGraph> results = new ArrayList<>();

      for( ElementGraph child : children )
        {
        Transformed transformed;

        try
          {
          transformed = transformer.transform( plannerContext, child );
          }
        catch( Throwable throwable )
          {
          throw new PlannerException( registry, phase, (Rule) transformer, child, throwable );
          }

        writeTransformTrace( ruleResult, phase, (Rule) transformer, parent, child, transformed );

        ElementGraph endGraph = transformed.getEndGraph();

        if( endGraph != null )
          results.add( endGraph );
        else
          results.add( child );
        }

      ruleResult.setLevelResults( phase.getLevel(), parent, results );
      }
    }

  private ElementGraph annotateWithPriors( ElementGraph elementGraph, List<? extends ElementGraph> priorResults )
    {
    if( priorResults == null )
      return elementGraph;

    // the results are sub-graphs of the elementGraph, so guaranteed to exist in graph

    AnnotatedGraph resultGraph = new ElementDirectedGraph( elementGraph );

    for( ElementGraph result : priorResults )
      {
      if( !( result instanceof AnnotatedGraph ) || !( (AnnotatedGraph) result ).hasAnnotations() )
        continue;

      EnumMultiMap<FlowElement> annotations = ( (AnnotatedGraph) result ).getAnnotations();

      resultGraph.getAnnotations().addAll( annotations );
      }

    return (ElementGraph) resultGraph;
    }

  private Set<FlowElement> getExclusions( List<? extends ElementGraph> elementGraphs, Enum[] annotationExcludes )
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

  // use the final assembly graph so we can get Scopes for heads and tails
  private List<ElementGraph> makeBoundedOn( ElementGraph currentElementGraph, Map<ElementGraph, EnumMultiMap> subGraphs )
    {
    List<ElementGraph> results = new ArrayList<>( subGraphs.size() );

    for( ElementGraph subGraph : subGraphs.keySet() )
      results.add( new BoundedElementMultiGraph( currentElementGraph, subGraph, subGraphs.get( subGraph ) ) );

    return results;
    }

  private void writePhaseInitPlan( PlanPhase phase, RuleResult ruleResult )
    {
    switch( phase.getLevel() )
      {
      case Assembly:
        traceWriter.writeTransformPlan( registry.getName(), ruleResult.getAssemblyGraph(), format( "%02d-%s-init.dot", phase.ordinal(), phase ) );
        break;
      case Step:
        break;
      case Node:
        break;
      case Pipeline:
        break;
      }
    }

  private void writePhaseResultPlan( PlanPhase phase, RuleResult ruleResult )
    {
    switch( phase.getLevel() )
      {
      case Assembly:
        traceWriter.writeTransformPlan( registry.getName(), ruleResult.getAssemblyGraph(), format( "%02d-%s-result.dot", phase.ordinal(), phase ) );
        break;
      case Step:
        traceWriter.writeTransformPlan( registry.getName(), ruleResult.getAssemblyToStepGraphMap().get( ruleResult.getAssemblyGraph() ), phase, "result" );
        break;
      case Node:
        traceWriter.writeTransformPlan( registry.getName(), ruleResult.getStepToNodeGraphMap(), phase, "result" );
        break;
      case Pipeline:
        traceWriter.writeTransformPlan( registry.getName(), ruleResult.getStepToNodeGraphMap(), ruleResult.getNodeToPipelineGraphMap(), phase, "result" );
        break;
      }
    }

  private void logPhase( ProcessLogger logger, boolean logAsInfo, String message, Object... items )
    {
    if( logAsInfo )
      logger.logInfo( message, items );
    else
      logger.logDebug( message, items );
    }

  private void writeTransformTrace( RuleResult ruleResult, PlanPhase phase, Rule rule, ElementGraph parent, ElementGraph child, GraphResult result )
    {
    if( traceWriter.isTransformTraceDisabled() )
      return;

    int[] path = child != null ? ruleResult.getPathFor( parent, child ) : ruleResult.getPathFor( parent );

    traceWriter.writeTransformPlan( registry.getName(), phase, rule, path, result );
    }
  }
