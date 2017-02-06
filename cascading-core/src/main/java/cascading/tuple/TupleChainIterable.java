/*
 * Copyright (c) 2007-2017 Xplenty, Inc. All Rights Reserved.
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

package cascading.tuple;

import java.util.Iterator;

import cascading.tuple.util.Resettable;

/**
 * TupleChainIterator chains the given Iterators into a single Iterator.
 * <p/>
 * As one iterator is completed, it will be closed and a new one will start.
 */
public class TupleChainIterable implements Iterable<Tuple>, Resettable<Iterable<Tuple>>
  {
  /** Field iterator */
  Iterable<Tuple>[] iterables;

  public TupleChainIterable( Iterable<Tuple> first, Iterable<Tuple>... iterables )
    {
    Iterable<Tuple>[] initial;

    if( first instanceof TupleChainIterable )
      initial = ( (TupleChainIterable) first ).iterables;
    else
      initial = new Iterable[]{first};

    this.iterables = new Iterable[ initial.length + iterables.length ];

    System.arraycopy( initial, 0, this.iterables, 0, initial.length );
    System.arraycopy( iterables, 0, this.iterables, initial.length, iterables.length );
    }


  @Override
  public Iterator<Tuple> iterator()
    {
    Iterator<Tuple>[] iterators = new Iterator[ iterables.length ];

    for( int i = 0; i < iterables.length; i++ )
      iterators[ i ] = iterables[ i ].iterator();

    return new TupleChainIterator( iterators );
    }

  @Override
  public void reset( Iterable<Tuple>... iterables )
    {
    this.iterables = iterables;
    }
  }
