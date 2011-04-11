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

import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import cascading.flow.FlowProcess;
import cascading.flow.stream.CoGroupClosure;
import cascading.flow.stream.Duct;
import cascading.pipe.Group;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.google.common.collect.ArrayListMultimap;

/**
 *
 */
public class LocalCoGroupGate extends LocalGroupGate
  {
  private Map<Duct, ArrayListMultimap<Tuple, Tuple>> valueMap;
  private final Map<Duct, Integer> posMap = new IdentityHashMap<Duct, Integer>();

  private CoGroupClosure closure;

  public LocalCoGroupGate( FlowProcess flowProcess, Group group )
    {
    super( flowProcess, group );
    }

  @Override
  public void prepare()
    {
    super.prepare();

    valueMap = initNewValuesMap();

    makePosMap( posMap );

    closure = new CoGroupClosure( flowProcess, group.getNumSelfJoins(), groupFields, valuesFields );
    }

  /**
   * This lets us just replace an old map and let the gc cleanup, vs clearing each map
   *
   * @return
   */
  private Map<Duct, ArrayListMultimap<Tuple, Tuple>> initNewValuesMap()
    {
    Map<Duct, ArrayListMultimap<Tuple, Tuple>> valueMap = new LinkedHashMap<Duct, ArrayListMultimap<Tuple, Tuple>>();
    for( int i = 0; i < orderedPrevious.length; i++ )
      {
      if( orderedPrevious[ i ] == null ) // only true for local mode
        throw new IllegalStateException( "previous duct is null" );

      valueMap.put( orderedPrevious[ i ], ArrayListMultimap.<Tuple, Tuple>create() );
      }

    return valueMap;
    }

  @Override
  public void start( Duct previous )
    {
    }

  @Override
  public void receive( Duct previous, TupleEntry incomingEntry )
    {
    int pos = posMap.get( previous );

    Tuple groupTuple = incomingEntry.selectTuple( groupFields[ pos ] );
    Tuple valuesTuple = incomingEntry.getTupleCopy();

    keysSet.add( groupTuple );
    valueMap.get( previous ).put( groupTuple, valuesTuple );
    }

  @Override
  public void complete( Duct previous )
    {
    if( count.decrementAndGet() != 0 )
      return;

    next.start( this );

    try
      {
      List[] lists = new List[ orderedPrevious.length ];

      for( Tuple groupTuple : keysSet )
        {
        int pos = 0;

        for( Map.Entry<Duct, ArrayListMultimap<Tuple, Tuple>> entry : valueMap.entrySet() )
          lists[ pos++ ] = entry.getValue().get( groupTuple );

        closure.reset( lists );

        groupingEntry.setTuple( groupTuple );

        // create Closure type here
        tupleEntryIterator.reset( group.getJoiner().getIterator( closure ) );

        next.receive( this, grouping );
        }
      }
    finally
      {
      keysSet = initNewKeySet();
      valueMap = initNewValuesMap();

      count.set( allPrevious.length );

      next.complete( this );
      }
    }
  }
