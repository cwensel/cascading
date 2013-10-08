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

package cascading.flow.local.stream;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import cascading.flow.FlowProcess;
import cascading.flow.stream.Duct;
import cascading.flow.stream.MemorySpliceGate;
import cascading.pipe.Splice;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;

/**
 *
 */
public class LocalGroupByGate extends MemorySpliceGate
  {
  private ListMultimap<Tuple, Tuple> valueMap;

  public LocalGroupByGate( FlowProcess flowProcess, Splice splice )
    {
    super( flowProcess, splice );
    }

  @Override
  protected boolean isBlockingStreamed()
    {
    return true;
    }

  private ListMultimap<Tuple, Tuple> initNewValueMap()
    {
    return Multimaps.synchronizedListMultimap( ArrayListMultimap.<Tuple, Tuple>create() );
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
    // chained below in #complete()
    }

  @Override
  public void receive( Duct previous, TupleEntry incomingEntry )
    {
    Tuple valuesTuple = incomingEntry.getTupleCopy();
    Tuple groupTuple = keyBuilder[ 0 ].makeResult( valuesTuple, null ); // view on valuesTuple

    groupTuple = getDelegatedTuple( groupTuple ); // wrap so hasher/comparator is honored

    keys.add( groupTuple );
    valueMap.put( groupTuple, valuesTuple );
    }

  @Override
  public void complete( Duct previous )
    {
    if( count.decrementAndGet() != 0 )
      return;

    next.start( this );

    // drain the keys and keyValues collections to preserve memory
    Iterator<Tuple> iterator = keys.iterator();

    // no need to synchronize here as we are guaranteed all writer threads are completed
    while( iterator.hasNext() )
      {
      Tuple groupTuple = iterator.next();

      iterator.remove();

      keyEntry.setTuple( groupTuple );

      List<Tuple> tuples = valueMap.get( groupTuple ); // can't removeAll, returns unmodifiable collection

      if( valueComparators != null )
        Collections.sort( tuples, valueComparators[ 0 ] );

      tupleEntryIterator.reset( tuples.iterator() );

      next.receive( this, grouping );

      tuples.clear();
      }

    keys = createKeySet();
    valueMap = initNewValueMap();
    count.set( numIncomingPaths );

    next.complete( this );
    }
  }
