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

package cascading.flow.local;

import java.util.List;

import cascading.flow.FlowElement;
import cascading.flow.stream.Duct;
import cascading.flow.stream.Gate;
import cascading.flow.stream.SourceStage;
import cascading.flow.stream.StepStreamGraph;
import cascading.pipe.Group;
import cascading.tap.Tap;

/**
 *
 */
public class LocalStepStreamGraph extends StepStreamGraph
  {

  public LocalStepStreamGraph( LocalFlowProcess flowProcess, LocalFlowStep step )
    {
    super( flowProcess, step );

    buildGraph();
    setTraps();
    setScopes();

//    printGraph( "streamgraph.dot" );
    bind();
    }

  protected void buildGraph()
    {
    for( Object rhsElement : step.getSources() )
      {
      Duct rhsDuct = new SourceStage( flowProcess, (Tap) rhsElement );

      addHead( rhsDuct );

      handleDuct( (FlowElement) rhsElement, rhsDuct );
      }
    }

  protected Gate createCoGroupGate( Group element )
    {
    return new LocalCoGroupGate( flowProcess, (Group) element );
    }

  protected Gate createGroupByGate( Group element )
    {
    return new LocalGroupByGate( flowProcess, (Group) element );
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
