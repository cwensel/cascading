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

import java.io.File;
import java.util.LinkedList;
import java.util.List;

import cascading.flow.BaseFlow;
import cascading.flow.FlowElement;
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

/**
 *
 */
public class RuleExec
  {
  private static final Logger LOG = LoggerFactory.getLogger( RuleExec.class );

  private static final int ELEMENT_THRESHOLD = 600;

  String transformTracePath;
  RuleRegistry registry;

  public RuleExec( RuleRegistry registry )
    {
    this.registry = registry;
    }

  public void enableTransformTracing( String dotPath )
    {
    this.transformTracePath = dotPath;
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

      FlowElementGraph elementGraph = ruleResult.getResult( PlanPhase.prior( phase ) );

      switch( phase )
        {
        case Start:
        case Complete:
          ruleResult.addResult( phase, flowElementGraph );
          break;

        case PrePartitionElements:
        case PartitionElements:
        case PreResolveElements:
        case PostResolveElements:
        case PartitionSteps:
          executePhase( phase, plannerContext, ruleResult );
          break;

        case ResolveElements:
          elementGraph = new FlowElementGraph( elementGraph );
          elementGraph.resolveFields();
          ( (BaseFlow) plannerContext.getFlow() ).updateSchemes( elementGraph );
          ruleResult.addResult( phase, elementGraph );
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

  private void logPhase( boolean logAsInfo, String message, Object... items )
    {
    if( logAsInfo )
      LOG.info( message, items );
    else
      LOG.debug( message, items );
    }

  protected FlowElementGraph executePhase( PlanPhase phase, PlannerContext plannerContext, RuleResult ruleResult )
    {
    FlowElementGraph flowElementGraph = ruleResult.getResult( PlanPhase.prior( phase ) );

    if( flowElementGraph == null )
      throw new IllegalStateException( "no prior graph found for state: " + PlanPhase.prior( phase ) );

    return executePhase( phase, plannerContext, ruleResult, flowElementGraph );
    }

  public FlowElementGraph executePhase( PlanPhase phase, PlannerContext plannerContext, RuleResult ruleResult, FlowElementGraph flowElementGraph )
    {
    LOG.debug( "executing plan phase: {}", phase );

    LinkedList<Rule> rules = registry.getRulesFor( phase );

    writePlan( flowElementGraph, phase.ordinal() + "-" + phase + "-init.dot" );

    try
      {
      int transformNum = 0;
      int partitionNum = 0;

      for( Rule rule : rules )
        {
        long begin = System.currentTimeMillis();

        if( rule instanceof GraphTransformer )
          {
          FlowElementGraph resultGraph = performTransform( phase, transformNum++, plannerContext, flowElementGraph, (GraphTransformer) rule );

          if( resultGraph != null )
            flowElementGraph = resultGraph;
          }
        else if( rule instanceof RulePartitioner )
          {
          List<ElementGraph> subGraphs = performPartition( phase, partitionNum++, plannerContext, flowElementGraph, (RulePartitioner) rule );

          ruleResult.addSubGraphs( subGraphs );
          }
        else if( rule instanceof GraphAssert )
          {
          performAssertion( plannerContext, flowElementGraph, (GraphAssert) rule );
          }
        else
          {
          throw new IllegalStateException( "unknown rule type: " + rule.getClass().getName() );
          }

        long end = System.currentTimeMillis();

        ruleResult.setRuleDuration( phase, rule, begin, end );
        }

      return flowElementGraph;
      }
    finally
      {
      if( phase.isElements() )
        {
        ruleResult.addResult( phase, flowElementGraph );

        writePlan( flowElementGraph, phase.ordinal() + "-" + phase + "-result.dot" );
        }
      else if( phase.isSteps() )
        {
        writePlan( ruleResult.getElementSubGraphs(), phase, "result" );
        }

      LOG.debug( "completed plan phase: {}", phase );
      }
    }

  private void writePlan( List<ElementGraph> flowElementGraphs, PlanPhase phase, String subName )
    {
    if( transformTracePath == null )
      return;

    for( int i = 0; i < flowElementGraphs.size(); i++ )
      {
      ElementGraph flowElementGraph = flowElementGraphs.get( i );
      String name = phase.ordinal() + "-" + phase + "-" + subName + "-" + i + ".dot";

      File file = new File( transformTracePath, name );

      LOG.info( "writing phase sub-graph trace: {}, to: {}", name, file );

      flowElementGraph.writeDOT( file.toString() );
      }
    }

  private void writePlan( FlowElementGraph flowElementGraph, String name )
    {
    if( transformTracePath == null )
      return;

    File file = new File( transformTracePath, name );

    LOG.info( "writing phase graph trace: {}, to: {}", name, file );

    flowElementGraph.writeDOT( file.toString() );
    }

  private void performAssertion( PlannerContext plannerContext, FlowElementGraph flowElementGraph, GraphAssert rule )
    {
    LOG.debug( "applying assertion: {}", ( (Rule) rule ).getRuleName() );

    Assertion assertion = rule.assertion( plannerContext, flowElementGraph );

    FlowElement primary = assertion.getFirstPrimary();

    if( primary == null )
      return;

    throw new PlannerException( (Pipe) assertion.getFirstPrimary(), assertion.getMessage() );
    }

  private FlowElementGraph performTransform( PlanPhase phase, int transformNum, PlannerContext plannerContext, FlowElementGraph flowElementGraph, GraphTransformer transformer )
    {
    LOG.debug( "applying transform: {}", ( (Rule) transformer ).getRuleName() );

    Transform transform = transformer.transform( plannerContext, flowElementGraph );

    writePlan( phase, transformNum, transform );

    return (FlowElementGraph) transform.getEndGraph();
    }

  private List<ElementGraph> performPartition( PlanPhase phase, int partitionNum, PlannerContext plannerContext, FlowElementGraph elementGraph, RulePartitioner partitioner )
    {
    Partitions partition = partitioner.partition( plannerContext, elementGraph );

    writePlan( phase, partitionNum, partition );

    return partition.getSubGraphs();
    }

  private void writePlan( PlanPhase phase, int ruleOrdinal, Transform transform )
    {
    if( transformTracePath == null )
      return;

    String ruleName = transform.getRuleName();

    ruleName = String.format( "%d-%s-%04d-%s", phase.ordinal(), phase, ruleOrdinal, ruleName );

    transform.writeDOTs( new File( transformTracePath, ruleName ).toString() );
    }

  private void writePlan( PlanPhase phase, int partitionNum, Partitions partition )
    {
    if( transformTracePath == null )
      return;

    String ruleName = partition.getRuleName();

    ruleName = String.format( "%d-%s-%04d-%s", phase.ordinal(), phase, partitionNum, ruleName );

    partition.writeDOTs( new File( transformTracePath, ruleName ).toString() );
    }
  }
