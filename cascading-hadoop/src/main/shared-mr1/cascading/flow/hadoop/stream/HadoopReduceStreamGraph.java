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

package cascading.flow.hadoop.stream;

import cascading.flow.FlowElement;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.flow.planner.FlowNode;
import cascading.flow.stream.Duct;
import cascading.flow.stream.Gate;
import cascading.flow.stream.NodeStreamGraph;
import cascading.flow.stream.SinkStage;
import cascading.flow.stream.SpliceGate;
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
    }

  protected void buildGraph()
    {
    Group group = (Group) Util.getFirst( node.getSourceElements() );

    Duct rhsDuct;

    if( group.isGroupBy() )
      rhsDuct = new HadoopGroupByGate( flowProcess, (GroupBy) group, SpliceGate.Role.source );
    else
      rhsDuct = new HadoopCoGroupGate( flowProcess, (CoGroup) group, SpliceGate.Role.source );

    addHead( rhsDuct );

    handleDuct( group, rhsDuct );
    }

  @Override
  protected SinkStage createSinkStage( Tap element )
    {
    return new HadoopSinkStage( flowProcess, element );
    }

  protected Gate createCoGroupGate( CoGroup element, SpliceGate.Role role )
    {
    throw new IllegalStateException( "should not happen" );
    }

  @Override
  protected Gate createGroupByGate( GroupBy element, SpliceGate.Role role )
    {
    throw new IllegalStateException( "should not happen" );
    }

  @Override
  protected Gate createHashJoinGate( HashJoin join )
    {
    throw new IllegalStateException( "should not happen" );
    }

  }
