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

import java.util.Iterator;
import java.util.LinkedList;

/**
 * TupleEntryCollector is a convenience class for managing a list of tuples. More specifically it can simultaneously
 * append and modify in place elements of the list through the use of a ListIterator.
 */
public class TupleListCollector extends TupleCollector implements Iterable<Tuple>
  {
  /** Field tuples */
  private final LinkedList<Tuple> tuples = new LinkedList<Tuple>();

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
   * Method collect adds every given Tuple instance. It tests for and ignores empty Tuples.
   *
   * @param tuples of type Tuple
   */
  private void collect( Tuple... tuples )
    {
    for( Tuple tuple : tuples )
      add( tuple );
    }

  protected void collect( Tuple tuple )
    {
    tuples.add( tuple );
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

  public Iterator<Tuple> iterator()
    {
    return tuples.iterator();
    }
  }
