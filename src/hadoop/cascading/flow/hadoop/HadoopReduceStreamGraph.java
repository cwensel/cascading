/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Cascading is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Cascading is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Cascading.  If not, see <http://www.gnu.org/licenses/>.
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
