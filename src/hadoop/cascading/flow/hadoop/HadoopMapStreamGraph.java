/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow.hadoop;

import java.util.List;
import java.util.Set;

import cascading.flow.FlowElement;
import cascading.flow.stream.Gate;
import cascading.flow.stream.MemoryJoinGate;
import cascading.flow.stream.SinkStage;
import cascading.flow.stream.SourceStage;
import cascading.flow.stream.SpliceGate;
import cascading.flow.stream.StepStreamGraph;
import cascading.pipe.CoGroup;
import cascading.pipe.Group;
import cascading.pipe.GroupBy;
import cascading.pipe.Join;
import cascading.tap.Tap;

/**
 *
 */
public class HadoopMapStreamGraph extends StepStreamGraph
  {
  private final Tap source;
  private SourceStage streamedHead;

  public HadoopMapStreamGraph( HadoopFlowProcess flowProcess, HadoopFlowStep step, Tap source )
    {
    super( flowProcess, step );
    this.source = source;

    buildGraph();

    setTraps();
    setScopes();

    printGraph( step.getID(), "map", flowProcess.getCurrentSliceNum() );
    bind();
    }

  public SourceStage getStreamedHead()
    {
    return streamedHead;
    }

  protected void buildGraph()
    {
    streamedHead = handleHead( this.source );

    FlowElement tail = step.getGroup() != null ? step.getGroup() : step.getSink();
    Set<Tap> tributaries = step.getJoinTributariesBetween( this.source, tail );

    tributaries.remove( this.source );

    for( Object source : tributaries )
      handleHead( (Tap) source );
    }

  private SourceStage handleHead( Tap source )
    {
    SourceStage sourceDuct = new SourceStage( flowProcess, source );

    addHead( sourceDuct );

    handleDuct( source, sourceDuct );

    return sourceDuct;
    }

  @Override
  protected SinkStage createSinkStage( Tap element )
    {
    return new HadoopSinkStage( flowProcess, element );
    }

  protected Gate createCoGroupGate( CoGroup element )
    {
    return new HadoopCoGroupGate( flowProcess, (CoGroup) element, SpliceGate.Role.sink );
    }

  protected Gate createGroupByGate( GroupBy element )
    {
    return new HadoopGroupByGate( flowProcess, (GroupBy) element, SpliceGate.Role.sink );
    }

  @Override
  protected MemoryJoinGate createNonBlockingJoinGate( Join join )
    {
    return new HadoopMemoryJoinGate( flowProcess, join ); // does not use a latch
    }

  protected boolean stopOnElement( FlowElement lhsElement, List<FlowElement> successors )
    {
    if( lhsElement instanceof Group )
      return true;

    if( successors.isEmpty() )
      {
      if( !( lhsElement instanceof Tap ) )
        throw new IllegalStateException( "expected a Tap instance" );

      return true;
      }

    return false;
    }
  }
