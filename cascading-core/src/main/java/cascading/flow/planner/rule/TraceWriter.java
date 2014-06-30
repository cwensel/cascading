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
import java.util.List;
import java.util.Map;

import cascading.flow.planner.FlowElementGraph;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.iso.assertion.Asserted;
import cascading.flow.planner.iso.subgraph.Partitions;
import cascading.flow.planner.iso.transformer.Transformed;
import cascading.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TraceWriter
  {
  private static final Logger LOG = LoggerFactory.getLogger( TraceWriter.class );

  public static final String GREEN = "0000000000000000000400000000000000000000000000000000000000000000";
  public static final String ORANGE = "0000000000000000000E00000000000000000000000000000000000000000000";
  public static final String RED = "0000000000000000000C00000000000000000000000000000000000000000000";

  String transformTracePath;

  public TraceWriter()
    {
    }

  void writePlan( PlanPhase phase, int ruleOrdinal, Transformed transformed )
    {
    if( transformTracePath == null )
      return;

    String ruleName = transformed.getRuleName();

    ruleName = String.format( "%02d-%s-%04d-%s", phase.ordinal(), phase, ruleOrdinal, ruleName );

    String path = new File( transformTracePath, ruleName ).toString();
    transformed.writeDOTs( path );

    markTransformed( transformed, path );
    }

  void writePlan( PlanPhase phase, int ruleOrdinal, Partitions partition )
    {
    if( transformTracePath == null )
      return;

    String ruleName = partition.getRuleName();

    ruleName = String.format( "%02d-%s-%04d-%s", phase.ordinal(), phase, ruleOrdinal, ruleName );

    String path = new File( transformTracePath, ruleName ).toString();
    partition.writeDOTs( path );

    markPartitioned( partition, path );
    }

  void writePlan( PlanPhase phase, int ruleOrdinal, int stepOrdinal, Partitions partition )
    {
    if( transformTracePath == null )
      return;

    String ruleName = partition.getRuleName();

    ruleName = String.format( "%02d-%s-%04d-%04d-%s", phase.ordinal(), phase, ruleOrdinal, stepOrdinal, ruleName );

    String path = new File( transformTracePath, ruleName ).toString();
    partition.writeDOTs( path );

    markPartitioned( partition, path );
    }

  void writePlan( PlanPhase phase, int ruleOrdinal, int stepOrdinal, int nodeOrdinal, Partitions partition )
    {
    if( transformTracePath == null )
      return;

    String ruleName = partition.getRuleName();

    ruleName = String.format( "%02d-%s-%04d-%04d-%04d-%s", phase.ordinal(), phase, ruleOrdinal, stepOrdinal, nodeOrdinal, ruleName );

    String path = new File( transformTracePath, ruleName ).toString();
    partition.writeDOTs( path );

    markPartitioned( partition, path );
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

  void writePlan( PlanPhase phase, int ruleOrdinal, int stepOrdinal, int nodeOrdinal, Asserted asserted )
    {
    if( transformTracePath == null )
      return;

    String ruleName = asserted.getRuleName();

    ruleName = String.format( "%02d-%s-%04d-%04d-%04d-%s", phase.ordinal(), phase, ruleOrdinal, stepOrdinal, nodeOrdinal, ruleName );

    String path = new File( transformTracePath, ruleName ).toString();
    asserted.writeDOTs( path );

    markAsserted( asserted, path );
    }

  void writePlan( PlanPhase phase, int ruleOrdinal, int stepOrdinal, int nodeOrdinal, int pipelineOrdinal, Asserted asserted )
    {
    if( transformTracePath == null )
      return;

    String ruleName = asserted.getRuleName();

    ruleName = String.format( "%02d-%s-%04d-%04d-%04d-%04d-%s", phase.ordinal(), phase, ruleOrdinal, stepOrdinal, nodeOrdinal, pipelineOrdinal, ruleName );

    String path = new File( transformTracePath, ruleName ).toString();
    asserted.writeDOTs( path );

    markAsserted( asserted, path );
    }

  private void markAsserted( Asserted asserted, String path )
    {
    if( asserted.getFirstAnchor() != null )
      markFolder( path, RED );
    }

  void writePlan( PlanPhase phase, int ruleOrdinal, int stepOrdinal, int nodeOrdinal, Transformed transformed )
    {
    if( transformTracePath == null )
      return;

    String ruleName = transformed.getRuleName();

    ruleName = String.format( "%02d-%s-%04d-%04d-%04d-%s", phase.ordinal(), phase, ruleOrdinal, stepOrdinal, nodeOrdinal, ruleName );

    String path = new File( transformTracePath, ruleName ).toString();
    transformed.writeDOTs( path );

    markTransformed( transformed, path );
    }

  void writePlan( PlanPhase phase, int ruleOrdinal, int stepOrdinal, int nodeOrdinal, int pipelineOrdinal, Transformed transformed )
    {
    if( transformTracePath == null )
      return;

    String ruleName = transformed.getRuleName();

    ruleName = String.format( "%02d-%s-%04d-%04d-%04d-%04d-%s", phase.ordinal(), phase, ruleOrdinal, stepOrdinal, nodeOrdinal, pipelineOrdinal, ruleName );

    String path = new File( transformTracePath, ruleName ).toString();
    transformed.writeDOTs( path );

    markTransformed( transformed, path );
    }

  private void markTransformed( Transformed transformed, String path )
    {
    if( transformed.getEndGraph() != null && !transformed.getBeginGraph().equals( transformed.getEndGraph() ) )
      markFolder( path, GREEN );
    }

  void writePlan( FlowElementGraph flowElementGraph, String name )
    {
    if( transformTracePath == null )
      return;

    File file = new File( transformTracePath, name );

    LOG.info( "writing phase graph trace: {}, to: {}", name, file );

    flowElementGraph.writeDOT( file.toString() );
    }

  void writePlan( List<ElementGraph> flowElementGraphs, PlanPhase phase, String subName )
    {
    if( transformTracePath == null )
      return;

    for( int i = 0; i < flowElementGraphs.size(); i++ )
      {
      ElementGraph flowElementGraph = flowElementGraphs.get( i );
      String name = String.format( "%02d-%s-%s-%04d.dot", phase.ordinal(), phase, subName, i );

      File file = new File( transformTracePath, name );

      LOG.info( "writing phase step sub-graph trace: {}, to: {}", name, file );

      flowElementGraph.writeDOT( file.toString() );
      }
    }

  void writePlan( Map<ElementGraph, List<ElementGraph>> parentGraphsMap, Map<ElementGraph, List<ElementGraph>> subGraphsMap, PlanPhase phase, String subName )
    {
    if( transformTracePath == null )
      return;

    int stepCount = 0;
    for( Map.Entry<ElementGraph, List<ElementGraph>> entry : parentGraphsMap.entrySet() )
      {
      int nodeCount = 0;
      for( ElementGraph elementGraph : entry.getValue() )
        {
        List<ElementGraph> pipelineGraphs = subGraphsMap.get( elementGraph );

        if( pipelineGraphs == null )
          continue;

        for( int i = 0; i < pipelineGraphs.size(); i++ )
          {
          ElementGraph flowElementGraph = pipelineGraphs.get( i );
          String name = String.format( "%02d-%s-%s-%04d-%04d-%04d.dot", phase.ordinal(), phase, subName, stepCount, nodeCount, i );

          File file = new File( transformTracePath, name );

          LOG.info( "writing phase node pipeline sub-graph trace: {}, to: {}", name, file );

          flowElementGraph.writeDOT( file.toString() );
          }

        nodeCount++;
        }

      stepCount++;
      }
    }

  void writePlan( Map<ElementGraph, List<ElementGraph>> subGraphsMap, PlanPhase phase, String subName )
    {
    if( transformTracePath == null )
      return;

    int stepCount = 0;
    for( Map.Entry<ElementGraph, List<ElementGraph>> entry : subGraphsMap.entrySet() )
      {
      List<ElementGraph> flowElementGraphs = entry.getValue();

      for( int i = 0; i < flowElementGraphs.size(); i++ )
        {
        ElementGraph flowElementGraph = flowElementGraphs.get( i );
        String name = String.format( "%02d-%s-%s-%04d-%04d.dot", phase.ordinal(), phase, subName, stepCount, i );

        File file = new File( transformTracePath, name );

        LOG.info( "writing phase node sub-graph trace: {}, to: {}", name, file );

        flowElementGraph.writeDOT( file.toString() );
        }

      stepCount++;
      }
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
  }