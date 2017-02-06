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

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

import cascading.tuple.util.Resettable;

/**
 * TupleChainIterator chains the given Iterators into a single Iterator.
 * <p/>
 * As one iterator is completed, it will be closed and a new one will start.
 */
public class TupleChainIterator implements Iterator<Tuple>, Closeable, Resettable<Iterator<Tuple>>
  {
  /** Field iterator */
  Iterator<Tuple>[] iterators;
  int currentIterator = 0;

  public TupleChainIterator( Iterator<Tuple>... iterators )
    {
    this.iterators = iterators;
    }

  /**
   * Method hasNext returns true if there is a next TupleEntry
   *
   * @return boolean
   */
  public boolean hasNext()
    {
    if( iterators.length < currentIterator + 1 ) // past the end
      return false;

    if( iterators[ currentIterator ].hasNext() )
      return true;

    closeCurrent();

    currentIterator++;

    return iterators.length != currentIterator && hasNext();
    }

  @Override
  public void reset( Iterator<Tuple>... iterators )
    {
    this.currentIterator = 0;
    this.iterators = iterators;
    }

  /**
   * Method next returns the next TupleEntry.
   *
   * @return TupleEntry
   */
  public Tuple next()
    {
    hasNext(); // force roll to next iterator

    return iterators[ currentIterator ].next();
    }

  /** Method remove removes the current Tuple from the underlying collection. */
  public void remove()
    {
    iterators[ currentIterator ].remove();
    }

  /** Method close closes all underlying resources. */
  @Override
  public void close()
    {
    if( iterators.length != currentIterator )
      closeCurrent();
    }

  protected void closeCurrent()
    {
    close( iterators[ currentIterator ] );
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
