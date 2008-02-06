/*
 * Copyright (c) 2007-2008 Vinculum Technologies, Inc. All Rights Reserved.
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

import java.util.LinkedList;
import java.util.ListIterator;

/**
 * TupleEntryCollector is a convenience class for managing a list of tuples. More specifically it can simultaneously
 * append and modify in place elements of the list through the use of a ListIterator.
 */
public class TupleEntryCollector implements Iterable<TupleEntry>
  {
  /** Field entry */
  private final TupleEntry entry = new TupleEntry();
  /** Field tuples */
  private final LinkedList<Tuple> tuples = new LinkedList<Tuple>();

  /** Constructor TupleEntryCollector creates a new TupleEntryCollector instance. */
  public TupleEntryCollector()
    {
    }

  /**
   * Constructor TupleEntryCollector creates a new TupleEntryCollector instance.
   *
   * @param fields of type Fields
   * @param tuple  of type Tuple...
   */
  public TupleEntryCollector( Fields fields, Tuple... tuple )
    {
    if( fields == null )
      throw new IllegalArgumentException( "fields must not be null" );

    entry.fields = fields;
    collect( tuple );
    }

  /**
   * Method collect adds every given Tuple instance. It tests for and ignores empty Tuples.
   *
   * @param tuples of type Tuple
   */
  public void collect( Tuple... tuples )
    {
    for( Tuple tuple : tuples )
      add( tuple );
    }

  /**
   * Method remove removes the last Tuple instance from this instance.
   *
   * @return TupleEntry
   */
  public TupleEntry remove()
    {
    entry.tuple = tuples.remove();

    return entry;
    }

  /**
   * Method add adds the given Tuple instance if it is not empty.
   *
   * @param tuple of type Tuple
   */
  public void add( Tuple tuple )
    {
    if( tuple.isEmpty() )
      return;

    if( !entry.fields.isUnknown() && entry.fields.size() != tuple.size() )
      throw new TupleException( "added the wrong number of fields, expected: " + entry.fields + ", got: " + tuple.size() );

    tuples.add( tuple );
    }

  /**
   * Method iterator returns a ListIterator instance.
   *
   * @return ListIterator<Tuple>
   */
  public TupleEntryListIterator iterator()
    {
    return new TupleEntryListIterator( entry, (ListIterator<Tuple>) tuples.iterator() );
    }

  /**
   * Method isEmpty returns the empty of this TupleCollector object.
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
  }
