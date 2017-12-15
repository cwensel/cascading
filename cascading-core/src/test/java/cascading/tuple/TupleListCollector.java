/*
 * Copyright (c) 2016-2017 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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
import java.util.LinkedList;

/**
 * TupleEntryCollector is a convenience class for managing a list of tuples. More specifically it can simultaneously
 * append and modify in place elements of the list through the use of a ListIterator.
 */
public class TupleListCollector extends TupleEntryCollector implements Iterable<Tuple>
  {
  /** Field tuples */
  private final LinkedList<Tuple> tuples = new LinkedList<Tuple>();
  /** Field copyTupleOnCollect */
  private boolean copyTupleOnCollect = false;

  /**
   * Constructor TupleEntryCollector creates a new TupleEntryCollector instance.
   *
   * @param fields of type Fields
   * @param tuple  of type Tuple...
   */
  public TupleListCollector( Fields fields, Tuple... tuple )
    {
    super( fields );

    collect( tuple );
    }

  /**
   * Constructor TupleListCollector creates a new TupleListCollector instance.
   * <p>
   * Set copyTupleOnCollect to {@code true} if a new Tuple instance should be stored in the
   * underlying list.
   *
   * @param fields             of type Fields
   * @param copyTupleOnCollect of type boolean
   */
  public TupleListCollector( Fields fields, boolean copyTupleOnCollect )
    {
    super( fields );
    this.copyTupleOnCollect = copyTupleOnCollect;
    }

  /**
   * Method collect adds every given Tuple instance. It tests for and ignores empty Tuples.
   *
   * @param tuples of type Tuple
   */
  private void collect( Tuple... tuples )
    {
    for( Tuple tuple : tuples )
      add( tuple );
    }

  protected void collect( TupleEntry tupleEntry )
    {
    if( copyTupleOnCollect )
      tuples.add( tupleEntry.getTupleCopy() );
    else
      tuples.add( tupleEntry.getTuple() );
    }

  /**
   * Method isEmpty returns true if this collection is empty.
   *
   * @return the empty (type boolean) of this TupleCollector object.
   */
  public boolean isEmpty()
    {
    return tuples.isEmpty();
    }

  /** Method clear clears all Tuple instances from this instance. */
  public void clear()
    {
    tuples.clear();
    }

  /**
   * Returns the size of this collection.
   *
   * @return int
   */
  public int size()
    {
    return tuples.size();
    }

  /**
   * Method iterator returns an iterator for this collection.
   *
   * @return Iterator
   */
  public Iterator<Tuple> iterator()
    {
    return tuples.iterator();
    }

  /**
   * Method entryIterator return a TupleEntry iterator for this collection.
   * <p>
   * Note the same TupleEntry will be returned on each next() call.
   *
   * @return Iterator
   */
  public Iterator<TupleEntry> entryIterator()
    {
    return new TupleEntryChainIterator( tupleEntry.getFields(), tuples.iterator() );
    }
  }
