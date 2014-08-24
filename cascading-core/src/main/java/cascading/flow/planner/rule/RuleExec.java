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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cascading.flow.BaseFlow;
import cascading.flow.FlowElement;
import cascading.flow.planner.PlannerContext;
import cascading.flow.planner.PlannerException;
import cascading.flow.planner.graph.AnnotatedGraph;
import cascading.flow.planner.graph.BoundedElementMultiGraph;
import cascading.flow.planner.graph.ElementDirectedGraph;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.graph.FlowElementGraph;
import cascading.flow.planner.iso.GraphResult;
import cascading.flow.planner.iso.assertion.Asserted;
import cascading.flow.planner.iso.assertion.GraphAssert;
import cascading.flow.planner.iso.subgraph.Partitions;
import cascading.flow.planner.iso.transformer.GraphTransformer;
import cascading.flow.planner.iso.transformer.Transformed;
import cascading.flow.planner.rule.util.TraceWriter;
import cascading.util.EnumMultiMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    ruleResult.initResult( flowElementGraph );

    for( PlanPhase phase : PlanPhase.values() ) // iterate in order, all planner phases
      {
      long beginPhase = System.currentTimeMillis();

      logPhase( logAsInfo, "starting rule phase: {}", phase );

      switch( phase.getAction() )
        {
        case Resolve:
          resolveElements( plannerContext, ruleResult );
          break;

        case Rule:
          executeRulePhase( phase, plannerContext, ruleResult );
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

  private void resolveElements( PlannerContext plannerContext, RuleResult ruleResult )
    {
    if( !registry.enabledResolveElements() )
      return;

    FlowElementGraph elementGraph = ruleResult.getAssemblyGraph();

    elementGraph = (FlowElementGraph) elementGraph.copyGraph();

    elementGraph.resolveFields();
    ( (BaseFlow) plannerContext.getFlow() ).updateSchemes( elementGraph );

    elementGraph = new FlowElementGraph( elementGraph ); // forces a re-hash in graph

    ruleResult.setLevelResults( ProcessLevel.Assembly, ruleResult.initialAssembly, elementGraph );
    }

  public RuleResult executeRulePhase( PlanPhase phase, PlannerContext plannerContext, RuleResult ruleResult )
    {
    LOG.debug( "executing plan phase: {}", phase );

    LinkedList<Rule> rules = registry.getRulesFor( phase );

    writePhaseInitPlan( phase, ruleResult );

    try
      {
      // within this phase, execute all rules in declared order
      for( Rule rule : rules )
        {
        LOG.debug( "executing rule: {}", rule );

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
        catch( Exception exception )
          {
          throw new PlannerException( rule, exception );
          }

        long end = System.currentTimeMillis();

        ruleResult.setRuleDuration( rule, begin, end );

        LOG.debug( "completed rule: {}", rule );
        }

      return ruleResult;
      }
    finally
      {
      LOG.debug( "completed plan phase: {}", phase );
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

    if( partitioner.getPartition() == RulePartitioner.Partition.SplitParent )
      handleParentPartitioning( plannerContext, ruleResult, phase, rule, partitioner );
    else if( partitioner.getPartition() == RulePartitioner.Partition.SplitCurrent )
      handleCurrentPartitioning( plannerContext, ruleResult, phase, rule, partitioner );
    else
      throw new IllegalStateException( "unknown partitioning type: " + partitioner.getPartition() );

    }

  private void handleCurrentPartitioning( PlannerContext plannerContext, RuleResult ruleResult, PlanPhase phase, Rule rule, RulePartitioner partitioner )
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

        Partitions partitions = partitioner.partition( plannerContext, priorAnnotated, exclusions );

        writeTrace( ruleResult, phase, rule, parent, child, partitions );

        List<ElementGraph> results = makeBoundedOn( ruleResult.getAssemblyGraph(), partitions.getAnnotatedSubGraphs() );

        if( results.isEmpty() )
          continue;

        // replace child with partitioned results
        resultChildren.remove( child );
        resultChildren.addAll( results );
        }

      subGraphs.put( parent, resultChildren );
      }

    ruleResult.setLevelResults( phase.getLevel(), subGraphs );
    }

  private void handleParentPartitioning( PlannerContext plannerContext, RuleResult ruleResult, PlanPhase phase, Rule rule, RulePartitioner partitioner )
    {
    Map<ElementGraph, List<? extends ElementGraph>> priorResults = ruleResult.getLevelResults( phase.getLevel() );

    Map<ElementGraph, List<? extends ElementGraph>> subGraphs = new LinkedHashMap<>();

    for( Map.Entry<ElementGraph, List<? extends ElementGraph>> entry : priorResults.entrySet() )
      {
      ElementGraph parent = entry.getKey();
      List<? extends ElementGraph> priors = entry.getValue();

      Set<FlowElement> exclusions = getExclusions( priors, partitioner.getAnnotationExcludes() );
      ElementGraph priorAnnotated = annotateWithPriors( parent, priors );

      Partitions partitions = partitioner.partition( plannerContext, priorAnnotated, exclusions );

      writeTrace( ruleResult, phase, rule, parent, null, partitions );

      List<ElementGraph> results = makeBoundedOn( ruleResult.getAssemblyGraph(), partitions.getAnnotatedSubGraphs() );

      Set<ElementGraph> uniques = new LinkedHashSet<>( results );

      if( uniques.size() != results.size() )
        throw new PlannerException( "rule created duplicate element graphs" );

      for( ElementGraph prior : priors )
        {
        if( !uniques.add( prior ) ) // todo: setting to force failure on duplicates
          LOG.info( "rule created duplicate element graph to prior partitioner, ignoring: {}", partitioner.getRuleName() );
        }

      subGraphs.put( parent, new ArrayList<>( uniques ) );
      }

    ruleResult.setLevelResults( phase.getLevel(), subGraphs );
    }

  private void performAssertion( PlannerContext plannerContext, RuleResult ruleResult, PlanPhase phase, GraphAssert rule )
    {
    LOG.debug( "applying assertion: {}", ( (Rule) rule ).getRuleName() );

    Map<ElementGraph, List<? extends ElementGraph>> levelResults = ruleResult.getLevelResults( phase.getLevel() );

    for( Map.Entry<ElementGraph, List<? extends ElementGraph>> entry : levelResults.entrySet() )
      {
      ElementGraph parent = entry.getKey(); // null for root case
      List<? extends ElementGraph> children = entry.getValue();

      for( ElementGraph child : children )
        {
        Asserted asserted = rule.assertion( plannerContext, child );

        writeTrace( ruleResult, phase, (Rule) rule, parent, child, asserted );

        FlowElement primary = asserted.getFirstAnchor();

        if( primary == null )
          continue;

        throw new PlannerException( asserted.getFirstAnchor(), asserted.getMessage() );
        }
      }
    }

  private void performTransform( PlannerContext plannerContext, RuleResult ruleResult, PlanPhase phase, GraphTransformer transformer )
    {
    LOG.debug( "applying transform: {}", ( (Rule) transformer ).getRuleName() );

    Map<ElementGraph, List<? extends ElementGraph>> levelResults = ruleResult.getLevelResults( phase.getLevel() );

    for( Map.Entry<ElementGraph, List<? extends ElementGraph>> entry : levelResults.entrySet() )
      {
      ElementGraph parent = entry.getKey(); // null for root case
      List<? extends ElementGraph> children = entry.getValue();

      List<ElementGraph> results = new ArrayList<>();

      for( ElementGraph child : children )
        {
        Transformed transformed = transformer.transform( plannerContext, child );

        writeTrace( ruleResult, phase, (Rule) transformer, parent, child, transformed );

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
        traceWriter.writePlan( ruleResult.getAssemblyGraph(), format( "%02d-%s-init.dot", phase.ordinal(), phase ) );
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
        traceWriter.writePlan( ruleResult.getAssemblyGraph(), format( "%02d-%s-result.dot", phase.ordinal(), phase ) );
        break;
      case Step:
        traceWriter.writePlan( ruleResult.getStepGraphs().get( ruleResult.getAssemblyGraph() ), phase, "result" );
        break;
      case Node:
        traceWriter.writePlan( ruleResult.getNodeGraphs(), phase, "result" );
        break;
      case Pipeline:
        traceWriter.writePlan( ruleResult.getNodeGraphs(), ruleResult.getPipelineGraphs(), phase, "result" );
        break;
      }
    }

  private void logPhase( boolean logAsInfo, String message, Object... items )
    {
    if( logAsInfo )
      LOG.info( message, items );
    else
      LOG.debug( message, items );
    }

  private void writeTrace( RuleResult ruleResult, PlanPhase phase, Rule rule, ElementGraph parent, ElementGraph child, GraphResult result )
    {
    if( traceWriter.isDisabled() )
      return;

    int[] path = child != null ? ruleResult.getPathFor( parent, child ) : ruleResult.getPathFor( parent );

    traceWriter.writePlan( phase, rule, path, result );
    }
  }
