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

package cascading.flow.stream;

import java.util.Collection;

import cascading.flow.FlowProcess;
import cascading.pipe.Splice;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 *
 */
public class MemoryCoGroupGate extends MemorySpliceGate
  {
  public MemoryCoGroupGate( FlowProcess flowProcess, Splice splice )
    {
    super( flowProcess, splice );
    }

  @Override
  protected boolean isBlockingStreamed()
    {
    return true;
    }

  @Override
  public void start( Duct previous )
    {
    }

  @Override
  public void receive( Duct previous, TupleEntry incomingEntry )
    {
    int pos = (int) posMap.get( previous );

    Tuple groupTuple = incomingEntry.selectTuple( keyFields[ pos ] );
    Tuple valuesTuple = incomingEntry.getTupleCopy();

    groupTuple = getDelegatedTuple( groupTuple ); // wrap so hasher/comparator is honored

    keys.add( groupTuple );
    keyValues[ pos ].get( groupTuple ).add( valuesTuple );
    }

  @Override
  public void complete( Duct previous )
    {
    if( count.decrementAndGet() != 0 )
      return;

    next.start( this );

    try
      {
      Collection<Tuple>[] collections = new Collection[ orderedPrevious.length ];

      for( Tuple keysTuple : keys )
        {
        for( int i = 0; i < keyValues.length; i++ )
          collections[ i ] = keyValues[ i ].get( keysTuple );

        closure.reset( collections );

        keyEntry.setTuple( closure.getGroupTuple( keysTuple ) );

        // create Closure type here
        tupleEntryIterator.reset( splice.getJoiner().getIterator( closure ) );

        next.receive( this, grouping );
        }
      }
    finally
      {
      keys = createKeySet();
      keyValues = createKeyValuesArray();

      count.set( numIncomingPaths );

      next.complete( this );
      }
    }
  }
