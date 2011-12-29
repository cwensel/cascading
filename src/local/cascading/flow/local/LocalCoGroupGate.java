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
