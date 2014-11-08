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

package cascading.flow.planner.rule.util;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cascading.flow.Flow;
import cascading.flow.FlowNode;
import cascading.flow.FlowStep;
import cascading.flow.Flows;
import cascading.flow.planner.FlowPlanner;
import cascading.flow.planner.PlannerContext;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.graph.FlowElementGraph;
import cascading.flow.planner.iso.GraphResult;
import cascading.flow.planner.iso.assertion.Asserted;
import cascading.flow.planner.iso.subgraph.Partitions;
import cascading.flow.planner.iso.transformer.Transformed;
import cascading.flow.planner.process.FlowNodeGraph;
import cascading.flow.planner.process.FlowStepGraph;
import cascading.flow.planner.rule.PlanPhase;
import cascading.flow.planner.rule.ProcessLevel;
import cascading.flow.planner.rule.Rule;
import cascading.flow.planner.rule.RuleResult;
import cascading.property.AppProps;
import cascading.util.Util;
import cascading.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cascading.property.PropertyUtil.getStringProperty;

public class TraceWriter
  {
  private static final Logger LOG = LoggerFactory.getLogger( TraceWriter.class );

  public static final String GREEN = "0000000000000000000400000000000000000000000000000000000000000000";
  public static final String ORANGE = "0000000000000000000E00000000000000000000000000000000000000000000";
  public static final String RED = "0000000000000000000C00000000000000000000000000000000000000000000";

  private String flowName;
  private Map properties = Collections.emptyMap();

  Map<ProcessLevel, Set<Rule>> counts = new EnumMap<>( ProcessLevel.class );

  public TraceWriter()
    {
    }

  public TraceWriter( Flow flow )
    {
    if( flow != null )
      {
      this.properties = flow.getConfigAsProperties();
      this.flowName = Flows.getNameOrID( flow );
      }
    }

  protected Path getFullTransformTracePath( String registryName )
    {
    Path planTransformTracePath = getPlanTransformTracePath();

    if( planTransformTracePath == null )
      return null;

    return planTransformTracePath.resolve( registryName );
    }

  public boolean isTransformTraceEnabled()
    {
    return !isTransformTraceDisabled();
    }

  public boolean isTransformTraceDisabled()
    {
    return getPlanTransformTracePath() == null;
    }

  public void writeTransformPlan( String registryName, PlanPhase phase, Rule rule, int[] ordinals, GraphResult graphResult )
    {
    if( isTransformTraceDisabled() )
      return;

    String ruleName = String.format( "%02d-%s-%04d", phase.ordinal(), phase, addRule( rule ) );

    for( int i = 1; i < ordinals.length; i++ )
      ruleName = String.format( "%s-%04d", ruleName, ordinals[ i ] );

    ruleName = String.format( "%s-%s", ruleName, graphResult.getRuleName() );

    Path path = getFullTransformTracePath( registryName ).resolve( ruleName );

    graphResult.writeDOTs( path.toString() );

    markResult( graphResult, path );
    }

  public void writeTransformPlan( String registryName, FlowElementGraph flowElementGraph, String name )
    {
    if( isTransformTraceDisabled() )
      return;

    Path file = getFullTransformTracePath( registryName ).resolve( name ).normalize();

    LOG.info( "writing phase assembly trace: {}, to: {}", name, file );

    flowElementGraph.writeDOT( file.toString() );
    }

  public void writeTransformPlan( String registryName, List<? extends ElementGraph> flowElementGraphs, PlanPhase phase, String subName )
    {
    if( isTransformTraceDisabled() )
      return;

    for( int i = 0; i < flowElementGraphs.size(); i++ )
      {
      ElementGraph flowElementGraph = flowElementGraphs.get( i );
      String name = String.format( "%02d-%s-%s-%04d.dot", phase.ordinal(), phase, subName, i );

      Path file = getFullTransformTracePath( registryName ).resolve( name ).normalize();

      LOG.info( "writing phase step trace: {}, to: {}", name, file );

      flowElementGraph.writeDOT( file.toString() );
      }
    }

  public void writeTransformPlan( String registryName, Map<ElementGraph, List<? extends ElementGraph>> parentGraphsMap, Map<ElementGraph, List<? extends ElementGraph>> subGraphsMap, PlanPhase phase, String subName )
    {
    if( isTransformTraceDisabled() )
      return;

    int stepCount = 0;
    for( Map.Entry<ElementGraph, List<? extends ElementGraph>> entry : parentGraphsMap.entrySet() )
      {
      int nodeCount = 0;
      for( ElementGraph elementGraph : entry.getValue() )
        {
        List<? extends ElementGraph> pipelineGraphs = subGraphsMap.get( elementGraph );

        if( pipelineGraphs == null )
          continue;

        for( int i = 0; i < pipelineGraphs.size(); i++ )
          {
          ElementGraph flowElementGraph = pipelineGraphs.get( i );
          String name = String.format( "%02d-%s-%s-%04d-%04d-%04d.dot", phase.ordinal(), phase, subName, stepCount, nodeCount, i );

          Path file = getFullTransformTracePath( registryName ).resolve( name );

          LOG.info( "writing phase node pipeline trace: {}, to: {}", name, file );

          flowElementGraph.writeDOT( file.toString() );
          }

        nodeCount++;
        }

      stepCount++;
      }
    }

  public void writeTransformPlan( String registryName, Map<ElementGraph, List<? extends ElementGraph>> subGraphsMap, PlanPhase phase, String subName )
    {
    if( isTransformTraceDisabled() )
      return;

    int stepCount = 0;
    for( Map.Entry<ElementGraph, List<? extends ElementGraph>> entry : subGraphsMap.entrySet() )
      {
      List<? extends ElementGraph> flowElementGraphs = entry.getValue();

      for( int i = 0; i < flowElementGraphs.size(); i++ )
        {
        ElementGraph flowElementGraph = flowElementGraphs.get( i );
        String name = String.format( "%02d-%s-%s-%04d-%04d.dot", phase.ordinal(), phase, subName, stepCount, i );

        Path file = getFullTransformTracePath( registryName ).resolve( name );

        LOG.info( "writing phase node trace: {}, to: {}", name, file );

        flowElementGraph.writeDOT( file.toString() );
        }

      stepCount++;
      }
    }

  protected Path getPlanTracePath()
    {
    return applyScope( getStringProperty( System.getProperties(), properties, FlowPlanner.TRACE_PLAN_PATH ) );
    }

  protected Path getPlanTransformTracePath()
    {
    return applyScope( getStringProperty( System.getProperties(), properties, FlowPlanner.TRACE_PLAN_TRANSFORM_PATH ) );
    }

  protected Path getPlanStatsPath()
    {
    return applyScope( getStringProperty( System.getProperties(), properties, FlowPlanner.TRACE_STATS_PATH ) );
    }

  private Path applyScope( String path )
    {
    if( path == null )
      return null;

    return FileSystems.getDefault().getPath( path, flowName );
    }

  public void writeTracePlan( String registryName, String fileName, ElementGraph flowElementGraph )
    {
    Path path = getPlanTracePath();

    if( path == null )
      return;

    if( registryName != null )
      path = path.resolve( registryName );

    Path filePath = path.resolve( String.format( "%s.dot", fileName ) );
    File file = filePath.toFile();

    LOG.info( "writing trace element plan: {}", file );

    String filename = file.toString();

    flowElementGraph.writeDOT( filename );
    }

  public void writeTracePlan( String registryName, String fileName, FlowStepGraph stepGraph )
    {
    Path path = getPlanTracePath();

    if( path == null )
      return;

    if( registryName != null )
      path = path.resolve( registryName );

    Path filePath = path.resolve( String.format( "%s.dot", fileName ) );
    File file = filePath.toFile();

    LOG.info( "writing trace step plan: {}", file );

    stepGraph.writeDOT( file.toString() );
    }

  public void writeTracePlanSteps( String directoryName, FlowStepGraph stepGraph )
    {
    Iterator<FlowStep> iterator = stepGraph.getTopologicalIterator();

    while( iterator.hasNext() )
      writePlan( iterator.next(), directoryName );
    }

  private void writePlan( FlowStep flowStep, String directoryName )
    {
    Path path = getPlanTracePath();

    if( path == null )
      return;

    int ordinal = flowStep.getOrdinal();

    Path rootPath = path.resolve( directoryName );
    String stepGraphName = String.format( "%s/%04d-step-sub-graph.dot", rootPath, ordinal );

    ElementGraph stepSubGraph = flowStep.getElementGraph();

    stepSubGraph.writeDOT( stepGraphName );

    FlowNodeGraph flowNodeGraph = flowStep.getFlowNodeGraph();

    Iterator<FlowNode> iterator = flowNodeGraph.getOrderedTopologicalIterator();

    int i = 0;
    while( iterator.hasNext() )
      {
      FlowNode flowNode = iterator.next();
      ElementGraph nodeGraph = flowNode.getElementGraph();
      String nodeGraphName = String.format( "%s/%04d-%04d-step-node-sub-graph.dot", rootPath, ordinal, i );

      nodeGraph.writeDOT( nodeGraphName );

      List<? extends ElementGraph> pipelineGraphs = flowNode.getPipelineGraphs();

      for( int j = 0; j < pipelineGraphs.size(); j++ )
        {
        ElementGraph pipelineGraph = pipelineGraphs.get( j );

        String pipelineGraphName = String.format( "%s/%04d-%04d-%04d-step-node-pipeline-sub-graph.dot", rootPath, ordinal, i, j );

        pipelineGraph.writeDOT( pipelineGraphName );
        }

      i++;
      }
    }

  public void writeFinal( String fileName, RuleResult ruleResult )
    {
    Path path = getPlanTracePath();

    if( path == null )
      return;

    Path filePath = path.resolve( String.format( "%s-%s.txt", fileName, ruleResult.getRegistry().getName() ) );
    File file = filePath.toFile();

    LOG.info( "writing final registry: {}", file );

    try
      {
      file.createNewFile();
      }
    catch( IOException exception )
      {
      // do nothing
      }
    }

  public void writeStats( PlannerContext plannerContext, RuleResult ruleResult )
    {
    Path path = getPlanStatsPath();

    if( path == null )
      return;

    File file = path.resolve( String.format( "planner-stats-%s-%s.txt", ruleResult.getRegistry().getName(), ruleResult.getResultStatus() ) ).toFile();

    LOG.info( "writing planner stats to: {}", file );

    file.getParentFile().mkdirs();

    try( PrintWriter writer = new PrintWriter( file ) )
      {
      Flow flow = plannerContext.getFlow();

      Map<Object, Object> configAsProperties = flow.getConfigAsProperties();

      writer.format( "cascading version: %s, build: %s\n", emptyOrValue( Version.getReleaseFull() ), emptyOrValue( Version.getReleaseBuild() ) );
      writer.format( "application id: %s\n", emptyOrValue( AppProps.getApplicationID( configAsProperties ) ) );
      writer.format( "application name: %s\n", emptyOrValue( AppProps.getApplicationName( configAsProperties ) ) );
      writer.format( "application version: %s\n", emptyOrValue( AppProps.getApplicationVersion( configAsProperties ) ) );
      writer.format( "platform: %s\n", emptyOrValue( flow.getPlatformInfo() ) );
      writer.format( "frameworks: %s\n", emptyOrValue( AppProps.getApplicationFrameworks( configAsProperties ) ) );

      writer.println();

      ruleResult.writeStats( writer );
      }
    catch( IOException exception )
      {
      LOG.error( "could not write stats", exception );
      }
    }

  private static String emptyOrValue( Object value )
    {
    if( value == null )
      return "";

    if( Util.isEmpty( value.toString() ) )
      return "";

    return value.toString();
    }

  private void markResult( GraphResult graphResult, Path path )
    {
    if( graphResult instanceof Transformed )
      markTransformed( (Transformed) graphResult, path );
    else if( graphResult instanceof Asserted )
      markAsserted( (Asserted) graphResult, path );
    else if( graphResult instanceof Partitions )
      markPartitioned( (Partitions) graphResult, path );
    }

  private void markPartitioned( Partitions partition, Path path )
    {
    String color = null;

    if( partition.hasContractedMatches() )
      color = ORANGE;

    if( partition.hasSubGraphs() )
      color = GREEN;

    markFolder( path, color );
    }

  private void markAsserted( Asserted asserted, Path path )
    {
    if( asserted.getFirstAnchor() != null )
      markFolder( path, RED );
    }

  private void markTransformed( Transformed transformed, Path path )
    {
    if( transformed.getEndGraph() != null && !transformed.getBeginGraph().equals( transformed.getEndGraph() ) )
      markFolder( path, GREEN );
    }

  private void markFolder( Path path, String color )
    {
    if( !Util.IS_OSX )
      return;

    if( color == null )
      return;

    // xattr -wx com.apple.FinderInfo 0000000000000000000400000000000000000000000000000000000000000000 child-0-ContractedGraphTransformer

    File file = path.toFile();

    File parentFile = file.getParentFile();
    String name = file.getName();

    String[] command = {
      "xattr",
      "-wx",
      "com.apple.FinderInfo",
      color,
      name
    };

    Util.execProcess( parentFile, command );
    }

  int addRule( Rule rule )
    {
    return addRule( rule.getRulePhase().getLevel(), rule );
    }

  int addRule( ProcessLevel level, Rule rule )
    {
    if( !counts.containsKey( level ) )
      counts.put( level, new LinkedHashSet<Rule>() );

    Set<Rule> rules = counts.get( level );

    rules.add( rule );

    return rules.size() - 1;
    }
  }