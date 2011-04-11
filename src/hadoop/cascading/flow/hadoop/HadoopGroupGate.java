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

import java.util.Iterator;

import cascading.flow.FlowProcess;
import cascading.flow.stream.Duct;
import cascading.flow.stream.GroupGate;
import cascading.flow.stream.StreamGraph;
import cascading.pipe.Group;
import cascading.pipe.cogroup.GroupByClosure;
import cascading.tuple.Tuple;
import org.apache.hadoop.mapred.OutputCollector;

/**
 *
 */
public abstract class HadoopGroupGate extends GroupGate
  {
  protected GroupByClosure closure;
  protected OutputCollector collector;

  public HadoopGroupGate( FlowProcess flowProcess, Group group, Role role )
    {
    super( flowProcess, group, role );
    }

  @Override
  public void bind( StreamGraph streamGraph )
    {
    allPrevious = getAllPreviousFor( streamGraph );

    if( role != Role.sink )
      next = getNextFor( streamGraph );
    }

  @Override
  public void prepare()
    {
    collector = ( (HadoopFlowProcess) flowProcess ).getOutputCollector();
    }

  @Override
  public void start( Duct previous )
    {

    }

  @Override
  public void complete( Duct previous )
    {

    }

  public void run( Tuple key, Iterator values )
    {
    key = unwrapGrouping( key );

    closure.reset( key, values );

    values = group.getJoiner().getIterator( closure );

    groupingEntry.setTuple( key );
    tupleEntryIterator.reset( values );

    next.receive( this, grouping );
    }

  protected abstract Tuple unwrapGrouping( Tuple key );
  }
