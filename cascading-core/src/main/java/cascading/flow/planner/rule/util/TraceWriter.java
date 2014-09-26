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
import java.util.EnumMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.graph.FlowElementGraph;
import cascading.flow.planner.iso.GraphResult;
import cascading.flow.planner.iso.assertion.Asserted;
import cascading.flow.planner.iso.subgraph.Partitions;
import cascading.flow.planner.iso.transformer.Transformed;
import cascading.flow.planner.rule.PlanPhase;
import cascading.flow.planner.rule.ProcessLevel;
import cascading.flow.planner.rule.Rule;
import cascading.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TraceWriter
  {
  private static final Logger LOG = LoggerFactory.getLogger( TraceWriter.class );

  public static final String GREEN = "0000000000000000000400000000000000000000000000000000000000000000";
  public static final String ORANGE = "0000000000000000000E00000000000000000000000000000000000000000000";
  public static final String RED = "0000000000000000000C00000000000000000000000000000000000000000000";

  public String transformTracePath;

  Map<ProcessLevel, Set<Rule>> counts = new EnumMap<>( ProcessLevel.class );

  public TraceWriter()
    {
    }

  public void setTracePath( String transformTracePath )
    {
    if( transformTracePath != null && transformTracePath.endsWith( File.separator ) )
      transformTracePath = transformTracePath.substring( 0, transformTracePath.length() - 2 );

    this.transformTracePath = transformTracePath;
    }

  protected String getTracePath()
    {
    return transformTracePath + File.separator;
    }

  public boolean isEnabled()
    {
    return !isDisabled();
    }

  public boolean isDisabled()
    {
    return transformTracePath == null;
    }

  public void writePlan( PlanPhase phase, Rule rule, int[] ordinals, GraphResult graphResult )
    {
    if( isDisabled() )
      return;

    String ruleName = String.format( "%02d-%s-%04d", phase.ordinal(), phase, addRule( rule ) );

    for( int i = 1; i < ordinals.length; i++ )
      ruleName = String.format( "%s-%04d", ruleName, ordinals[ i ] );

    ruleName = String.format( "%s-%s", ruleName, graphResult.getRuleName() );

    String path = new File( getTracePath(), ruleName ).toString();
    graphResult.writeDOTs( path );

    markResult( graphResult, path );
    }

  public void writePlan( FlowElementGraph flowElementGraph, String name )
    {
    if( isDisabled() )
      return;

    File file = new File( getTracePath(), name );

    LOG.info( "writing phase assembly trace: {}, to: {}", name, file );

    flowElementGraph.writeDOT( file.toString() );
    }

  public void writePlan( List<? extends ElementGraph> flowElementGraphs, PlanPhase phase, String subName )
    {
    if( isDisabled() )
      return;

    for( int i = 0; i < flowElementGraphs.size(); i++ )
      {
      ElementGraph flowElementGraph = flowElementGraphs.get( i );
      String name = String.format( "%02d-%s-%s-%04d.dot", phase.ordinal(), phase, subName, i );

      File file = new File( getTracePath(), name );

      LOG.info( "writing phase step trace: {}, to: {}", name, file );

      flowElementGraph.writeDOT( file.toString() );
      }
    }

  public void writePlan( Map<ElementGraph, List<? extends ElementGraph>> parentGraphsMap, Map<ElementGraph, List<? extends ElementGraph>> subGraphsMap, PlanPhase phase, String subName )
    {
    if( isDisabled() )
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

          File file = new File( getTracePath(), name );

          LOG.info( "writing phase node pipeline trace: {}, to: {}", name, file );

          flowElementGraph.writeDOT( file.toString() );
          }

        nodeCount++;
        }

      stepCount++;
      }
    }

  public void writePlan( Map<ElementGraph, List<? extends ElementGraph>> subGraphsMap, PlanPhase phase, String subName )
    {
    if( isDisabled() )
      return;

    int stepCount = 0;
    for( Map.Entry<ElementGraph, List<? extends ElementGraph>> entry : subGraphsMap.entrySet() )
      {
      List<? extends ElementGraph> flowElementGraphs = entry.getValue();

      for( int i = 0; i < flowElementGraphs.size(); i++ )
        {
        ElementGraph flowElementGraph = flowElementGraphs.get( i );
        String name = String.format( "%02d-%s-%s-%04d-%04d.dot", phase.ordinal(), phase, subName, stepCount, i );

        File file = new File( getTracePath(), name );

        LOG.info( "writing phase node trace: {}, to: {}", name, file );

        flowElementGraph.writeDOT( file.toString() );
        }

      stepCount++;
      }
    }

  private void markResult( GraphResult graphResult, String path )
    {
    if( graphResult instanceof Transformed )
      markTransformed( (Transformed) graphResult, path );
    else if( graphResult instanceof Asserted )
      markAsserted( (Asserted) graphResult, path );
    else if( graphResult instanceof Partitions )
      markPartitioned( (Partitions) graphResult, path );
    }

  private void markPartitioned( Partitions partition, String path )
    {
    String color = null;

    if( partition.hasContractedMatches() )
      color = ORANGE;

    if( partition.hasSubGraphs() )
      color = GREEN;

    markFolder( path, color );
    }

  private void markAsserted( Asserted asserted, String path )
    {
    if( asserted.getFirstAnchor() != null )
      markFolder( path, RED );
    }

  private void markTransformed( Transformed transformed, String path )
    {
    if( transformed.getEndGraph() != null && !transformed.getBeginGraph().equals( transformed.getEndGraph() ) )
      markFolder( path, GREEN );
    }

  private void markFolder( String path, String color )
    {
    if( !Util.IS_OSX )
      return;

    if( color == null )
      return;

    // xattr -wx com.apple.FinderInfo 0000000000000000000400000000000000000000000000000000000000000000 child-0-ContractedGraphTransformer

    File file = new File( path );

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