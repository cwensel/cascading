/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
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

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

/**
 * TupleEntryChainIterator chains the given Iterators into a single Iterator.
 * <p/>
 * As one iterator is completed, it will be closed and a new one will start.
 */
public class TupleEntryIterableChainIterator extends TupleEntryIterator
  {
  Iterator<Iterator<Tuple>> iterators;
  Iterator<Tuple> currentIterator = null;

  public TupleEntryIterableChainIterator( Fields fields )
    {
    super( fields );
    }

  public TupleEntryIterableChainIterator( Fields fields, Iterable<Iterator<Tuple>> iterable )
    {
    super( fields );
    this.iterators = iterable.iterator();
    }

  /**
   * Method hasNext returns true if there is a next TupleEntry
   *
   * @return boolean
   */
  public boolean hasNext()
    {
    if( currentIterator == null && !iterators.hasNext() )
      return false;

    if( currentIterator != null && currentIterator.hasNext() )
      return true;

    closeCurrent();

    currentIterator = null;

    if( iterators.hasNext() )
      currentIterator = iterators.next();

    return hasNext();
    }

  public void reset( Iterable<Iterator<Tuple>> iterable )
    {
    this.currentIterator = null;
    this.iterators = iterable.iterator();
    }

  /**
   * Method next returns the next TupleEntry.
   *
   * @return TupleEntry
   */
  public TupleEntry next()
    {
    hasNext(); // force roll to next iterator

    entry.setTuple( currentIterator.next() );

    return entry;
    }

  /** Method remove removes the current TupleEntry from the underlying collection. */
  public void remove()
    {
    currentIterator.remove();
    }

  /** Method close closes all underlying resources. */
  public void close()
    {
    if( currentIterator != null )
      closeCurrent();
    }

  protected void closeCurrent()
    {
    close( currentIterator );
    }

  private void close( Iterator iterator )
    {
    if( iterator instanceof Closeable )
      {
      try
        {
        ( (Closeable) iterator ).close();
        }
      catch( IOException exception )
        {
        // ignore
        }
      }
    }
  }
