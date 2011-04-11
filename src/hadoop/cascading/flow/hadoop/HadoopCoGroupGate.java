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

import java.util.IdentityHashMap;
import java.util.Map;

import cascading.CascadingException;
import cascading.flow.FlowProcess;
import cascading.flow.stream.Duct;
import cascading.flow.stream.DuctException;
import cascading.flow.stream.GroupGate;
import cascading.pipe.Group;
import cascading.pipe.cogroup.CoGroupClosure;
import cascading.tuple.IndexTuple;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TuplePair;
import cascading.tuple.Tuples;

/**
 *
 */
public class HadoopCoGroupGate extends HadoopGroupGate
  {
  private final Map<Duct, Integer> posMap = new IdentityHashMap<Duct, Integer>();


  public HadoopCoGroupGate( FlowProcess flowProcess, Group group, GroupGate.Role role )
    {
    super( flowProcess, group, role );
    }

  @Override
  public void initialize()
    {
    super.initialize();

    orderDucts();
    }

  @Override
  public void prepare()
    {
    super.prepare();

    makePosMap( posMap );

    if( role != Role.sink )
      closure = new CoGroupClosure( flowProcess, group.getNumSelfJoins(), groupFields, valuesFields );
    }

  @Override
  public void receive( Duct previous, TupleEntry incomingEntry )
    {
    Integer pos = posMap.get( previous );

    Tuple groupTuple = incomingEntry.selectTuple( groupFields[ pos ] );
    Tuple sortTuple = sortFields == null ? null : incomingEntry.selectTuple( sortFields[ pos ] );
    Tuple valuesTuple = Tuples.nulledCopy( incomingEntry, groupFields[ pos ] );

    Tuple groupKey = sortTuple == null ? groupTuple : new TuplePair( groupTuple, sortTuple );

    try
      {
      collector.collect( new IndexTuple( pos, groupKey ), new IndexTuple( pos, valuesTuple ) );
      }
    catch( OutOfMemoryError error )
      {
      handleReThrowableException( "out of memory, try increasing task memory allocation", error );
      }
    catch( CascadingException exception )
      {
      handleException( exception, incomingEntry );
      }
    catch( Throwable throwable )
      {
      handleException( new DuctException( "internal error: " + incomingEntry.getTuple().print(), throwable ), incomingEntry );
      }
    }

  @Override
  protected Tuple unwrapGrouping( Tuple key )
    {
    return ( (IndexTuple) key ).getTuple();
    }
  }
