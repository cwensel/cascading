/*
 * Copyright (c) 2016 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 * Copyright (c) 2007-2016 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow.hadoop.stream.graph;

import cascading.flow.FlowElement;
import cascading.flow.FlowNode;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.flow.hadoop.stream.element.HadoopCoGroupGate;
import cascading.flow.hadoop.stream.element.HadoopGroupByGate;
import cascading.flow.hadoop.stream.element.HadoopSinkStage;
import cascading.flow.stream.duct.Duct;
import cascading.flow.stream.duct.Gate;
import cascading.flow.stream.element.SinkStage;
import cascading.flow.stream.graph.IORole;
import cascading.flow.stream.graph.NodeStreamGraph;
import cascading.pipe.CoGroup;
import cascading.pipe.Group;
import cascading.pipe.GroupBy;
import cascading.pipe.HashJoin;
import cascading.tap.Tap;
import cascading.util.Util;

/**
 *
 */
public class HadoopReduceStreamGraph extends NodeStreamGraph
  {
  public HadoopReduceStreamGraph( HadoopFlowProcess flowProcess, FlowNode node, FlowElement sourceElement )
    {
    super( flowProcess, node, sourceElement );

    buildGraph();

    setTraps();
    setScopes();

    printGraph( node.getID(), "reduce", flowProcess.getCurrentSliceNum() );

    bind();

    printBoundGraph( node.getID(), "reduce", flowProcess.getCurrentSliceNum() );
    }

  protected void buildGraph()
    {
    Group group = (Group) Util.getFirst( node.getSourceElements() );

    Duct rhsDuct;

    if( group.isGroupBy() )
      rhsDuct = new HadoopGroupByGate( flowProcess, (GroupBy) group, IORole.source );
    else
      rhsDuct = new HadoopCoGroupGate( flowProcess, (CoGroup) group, IORole.source );

    addHead( rhsDuct );

    handleDuct( group, rhsDuct );
    }

  @Override
  protected SinkStage createSinkStage( Tap element )
    {
    return new HadoopSinkStage( flowProcess, element );
    }

  protected Gate createCoGroupGate( CoGroup element, IORole role )
    {
    throw new IllegalStateException( "should not happen" );
    }

  @Override
  protected Gate createGroupByGate( GroupBy element, IORole role )
    {
    throw new IllegalStateException( "should not happen" );
    }

  @Override
  protected Gate createHashJoinGate( HashJoin join )
    {
    throw new IllegalStateException( "should not happen" );
    }

  }
