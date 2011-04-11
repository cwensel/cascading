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

import java.util.Collections;
import java.util.List;

import cascading.flow.FlowProcess;
import cascading.flow.stream.Duct;
import cascading.pipe.Group;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;

/**
 *
 */
public class LocalGroupByGate extends LocalGroupGate
  {
  private ListMultimap<Tuple, Tuple> valueMap;

  public LocalGroupByGate( FlowProcess flowProcess, Group group )
    {
    super( flowProcess, group );
    }

  private ListMultimap<Tuple, Tuple> initNewValueMap()
    {
    return (ListMultimap<Tuple, Tuple>) Multimaps.synchronizedListMultimap( ArrayListMultimap.<Tuple, Tuple>create() );
    }

  @Override
  public void prepare()
    {
    super.prepare();

    valueMap = initNewValueMap();
    }

  @Override
  public void start( Duct previous )
    {
    }

  @Override
  public void receive( Duct previous, TupleEntry incomingEntry )
    {
    Tuple groupTuple = incomingEntry.selectTuple( groupFields[ 0 ] );
    Tuple valuesTuple = incomingEntry.getTupleCopy();

    keysSet.add( groupTuple );
    valueMap.put( groupTuple, valuesTuple );
    }

  @Override
  public void complete( Duct previous )
    {
    if( count.decrementAndGet() != 0 )
      return;

    next.start( this );

    try
      {
      // no need to synchronize here as we are guaranteed all writer threads are completed
      for( Tuple groupTuple : keysSet )
        {
        groupingEntry.setTuple( groupTuple );

        List<Tuple> tuples = valueMap.get( groupTuple );

        if( valueComparators != null )
          Collections.sort( tuples, valueComparators[ 0 ] );

        tupleEntryIterator.reset( tuples.iterator() );

        next.receive( this, grouping );
        }
      }
    finally
      {
      keysSet = initNewKeySet();
      valueMap = initNewValueMap();
      count.set( allPrevious.length );

      next.complete( this );
      }
    }
  }
