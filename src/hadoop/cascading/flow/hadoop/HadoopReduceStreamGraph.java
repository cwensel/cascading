/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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

import cascading.flow.FlowElement;
import cascading.flow.stream.Duct;
import cascading.flow.stream.Gate;
import cascading.flow.stream.GroupGate;
import cascading.flow.stream.SinkStage;
import cascading.flow.stream.StepStreamGraph;
import cascading.pipe.Group;
import cascading.tap.Tap;

/**
 *
 */
public class HadoopReduceStreamGraph extends StepStreamGraph
  {
  public HadoopReduceStreamGraph( HadoopFlowProcess flowProcess, HadoopFlowStep step )
    {
    super( flowProcess, step );

    buildGraph();

    setTraps();
    setScopes();

    bind();
    }

  protected void buildGraph()
    {
    Group group = step.getGroup();

    Duct rhsDuct = null;

    if( group.isGroupBy() )
      rhsDuct = new HadoopGroupByGate( flowProcess, group, GroupGate.Role.source );
    else
      rhsDuct = new HadoopCoGroupGate( flowProcess, group, GroupGate.Role.source );

    addHead( rhsDuct );

    handleDuct( (FlowElement) group, rhsDuct );
    }

  @Override
  protected SinkStage createSinkStage( Tap element )
    {
    return new HadoopSinkStage( flowProcess, element );
    }

  protected Gate createCoGroupGate( Group element )
    {
    throw new IllegalStateException( "should not happen" );
    }

  protected Gate createGroupByGate( Group element )
    {
    throw new IllegalStateException( "should not happen" );
    }

  protected boolean stopOnElement( FlowElement lhsElement, List<FlowElement> successors )
    {
    if( successors.isEmpty() )
      {
      if( !( lhsElement instanceof Tap ) )
        throw new IllegalStateException( "expected a Tap instance" );

      return true;
      }

    return false;
    }
  }
