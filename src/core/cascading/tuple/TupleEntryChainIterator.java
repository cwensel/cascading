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

package cascading.tuple;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

/**
 *
 */
public class TupleEntryChainIterator extends TupleEntryIterator
  {
  /** Field iterator */
  Iterator<Tuple>[] iterators;
  int currentIterator = 0;

  public TupleEntryChainIterator( Fields fields, Iterator<Tuple>... iterators )
    {
    super( fields );
    this.iterators = iterators;
    }

  /**
   * Method hasNext returns true if there is a next TupleEntry
   *
   * @return boolean
   */
  public boolean hasNext()
    {
    if( iterators.length == 0 )
      return false;

    if( iterators[ currentIterator ].hasNext() )
      return true;

    currentIterator++;

    return iterators.length != currentIterator && hasNext();
    }

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
  public TupleEntry next()
    {
    hasNext(); // force roll to next iterator

    entry.setTuple( (Tuple) iterators[ currentIterator ].next() );

    return entry;
    }

  /** Method remove removes the current TupleEntry from the underlying collection. */
  public void remove()
    {
    iterators[ currentIterator ].remove();
    }

  /** Method close closes all underlying resources. */
  public void close()
    {
    for( Iterator iterator : iterators )
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
  }
