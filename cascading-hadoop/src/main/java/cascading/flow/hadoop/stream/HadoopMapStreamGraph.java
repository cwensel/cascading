/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cascading.flow.FlowElement;
import cascading.flow.FlowException;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.flow.hadoop.HadoopFlowStep;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.flow.stream.Gate;
import cascading.flow.stream.MemoryHashJoinGate;
import cascading.flow.stream.SinkStage;
import cascading.flow.stream.SourceStage;
import cascading.flow.stream.SpliceGate;
import cascading.flow.stream.StepStreamGraph;
import cascading.pipe.CoGroup;
import cascading.pipe.Group;
import cascading.pipe.GroupBy;
import cascading.pipe.HashJoin;
import cascading.tap.Tap;
import org.apache.hadoop.mapred.JobConf;

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
    streamedHead = handleHead( this.source, flowProcess );

    FlowElement tail = step.getGroup() != null ? step.getGroup() : step.getSink();
    Set<Tap> tributaries = step.getJoinTributariesBetween( this.source, tail );

    tributaries.remove( this.source ); // we cannot stream and accumulate the same source

    // accumulated paths
    for( Object source : tributaries )
      {
      HadoopFlowProcess hadoopProcess = (HadoopFlowProcess) flowProcess;
      JobConf conf = hadoopProcess.getJobConf();

      // allows client side config to be used cluster side
      String property = conf.getRaw( "cascading.step.accumulated.source.conf." + Tap.id( (Tap) source ) );

      if( property == null )
        throw new IllegalStateException( "accumulated source conf property missing for: " + ( (Tap) source ).getIdentifier() );

      conf = getSourceConf( hadoopProcess, conf, property );
      flowProcess = new HadoopFlowProcess( hadoopProcess, conf );

      handleHead( (Tap) source, flowProcess );
      }
    }

  private JobConf getSourceConf( HadoopFlowProcess flowProcess, JobConf conf, String property )
    {
    Map<String, String> priorConf;
    try
      {
      priorConf = (Map<String, String>) HadoopUtil.deserializeBase64( property, conf, HashMap.class, true );
      }
    catch( IOException exception )
      {
      throw new FlowException( "unable to deserialize properties", exception );
      }

    return flowProcess.mergeMapIntoConfig( conf, priorConf );
    }

  private SourceStage handleHead( Tap source, FlowProcess flowProcess )
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
    return new HadoopCoGroupGate( flowProcess, element, SpliceGate.Role.sink );
    }

  protected Gate createGroupByGate( GroupBy element )
    {
    return new HadoopGroupByGate( flowProcess, element, SpliceGate.Role.sink );
    }

  @Override
  protected MemoryHashJoinGate createNonBlockingJoinGate( HashJoin join )
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
