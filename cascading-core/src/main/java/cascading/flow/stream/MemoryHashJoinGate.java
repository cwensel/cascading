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

package cascading.flow.stream;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;

import cascading.flow.FlowProcess;
import cascading.pipe.HashJoin;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 *
 */
public class MemoryHashJoinGate extends MemorySpliceGate
  {
  protected CountDownLatch latch;

  private Collection<Tuple>[] collections;
  private ArrayList<Tuple> streamedCollection;

  public MemoryHashJoinGate( FlowProcess flowProcess, HashJoin join )
    {
    super( flowProcess, join );
    }

  @Override
  public void bind( StreamGraph streamGraph )
    {
    super.bind( streamGraph );

    count.set( numIncomingPaths ); // the number of paths incoming
    latch = new CountDownLatch( numIncomingPaths - 1 );
    }

  @Override
  public void prepare()
    {
    super.prepare();

    streamedCollection = new ArrayList<Tuple>( Arrays.asList( new Tuple() ) ); // placeholder in collection
    collections = new Collection[ orderedPrevious.length ];
    collections[ 0 ] = streamedCollection;
    }

  @Override
  public void receive( Duct previous, TupleEntry incomingEntry )
    {
    int pos = posMap.get( previous );

    Tuple incomingTuple = pos != 0 ? incomingEntry.getTupleCopy() : incomingEntry.getTuple();
    Tuple keyTuple = keyBuilder[ pos ].makeResult( incomingTuple, null ); // view in incomingTuple

    keyTuple = getDelegatedTuple( keyTuple );

    if( pos != 0 )
      {
      keys.add( keyTuple );
      keyValues[ pos ].get( keyTuple ).add( incomingTuple ); // always a copy
      return;
      }

    waitOnLatch();

    keys.remove( keyTuple );

    streamedCollection.set( 0, incomingTuple ); // no need to copy, temp setting

    performJoinWith( keyTuple );
    }

  private void performJoinWith( Tuple keyTuple )
    {
    // never replace the first array, pos == 0
    for( int i = 1; i < keyValues.length; i++ )
      {
      // if key does not exist, #get will create an empty array list,
      // and store the key, which is not a copy
      if( keyValues[ i ].containsKey( keyTuple ) )
        collections[ i ] = keyValues[ i ].get( keyTuple );
      else
        collections[ i ] = Collections.EMPTY_LIST;
      }

    closure.reset( collections );

    keyEntry.setTuple( keyTuple );
    tupleEntryIterator.reset( splice.getJoiner().getIterator( closure ) );

    next.receive( this, grouping );
    }

  @Override
  public void complete( Duct previous )
    {
    countDownLatch();

    if( count.decrementAndGet() != 0 )
      return;

    collections[ 0 ] = Collections.EMPTY_LIST;

    for( Tuple keyTuple : keys )
      performJoinWith( keyTuple );

    keys = createKeySet();
    keyValues = createKeyValuesArray();

    super.complete( previous );
    }

  protected void waitOnLatch()
    {
    try
      {
      latch.await();
      }
    catch( InterruptedException exception )
      {
      throw new RuntimeException( "interrupted", exception );
      }
    }

  protected void countDownLatch()
    {
    latch.countDown();
    }

  @Override
  protected boolean isBlockingStreamed()
    {
    return false;
    }
  }
