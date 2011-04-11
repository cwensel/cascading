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

import cascading.CascadingException;
import cascading.flow.FlowProcess;
import cascading.flow.stream.Duct;
import cascading.flow.stream.DuctException;
import cascading.pipe.Group;
import cascading.pipe.cogroup.GroupByClosure;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TuplePair;
import cascading.tuple.Tuples;

/**
 *
 */
public class HadoopGroupByGate extends HadoopGroupGate
  {
  public HadoopGroupByGate( FlowProcess flowProcess, Group group, Role role )
    {
    super( flowProcess, group, role );
    }

  @Override
  public void prepare()
    {
    super.prepare();

    if( role != Role.sink )
      closure = new GroupByClosure( flowProcess, groupFields, valuesFields );
    }

  @Override
  public void receive( Duct previous, TupleEntry incomingEntry )
    {
    Tuple groupTuple = incomingEntry.selectTuple( groupFields[ 0 ] );
    Tuple sortTuple = sortFields == null ? null : incomingEntry.selectTuple( sortFields[ 0 ] );
    Tuple valuesTuple = Tuples.nulledCopy( incomingEntry, groupFields[ 0 ] );

    Tuple groupKey = sortTuple == null ? groupTuple : new TuplePair( groupTuple, sortTuple );

    try
      {
      collector.collect( groupKey, valuesTuple );
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
    return sortFields == null ? (Tuple) key : ( (TuplePair) key ).getLhs();
    }
  }
