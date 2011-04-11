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
import java.util.Iterator;

/**
 * Class TupleEntryIterator provides an efficient Iterator for returning {@link TupleEntry} elements in an
 * underlying {@link Tuple} collection.
 */
public abstract class TupleEntryIterator implements Iterator<TupleEntry>, Closeable
  {
  /** Field entry */
  final TupleEntry entry = new TupleEntry( true );

  /**
   * Constructor TupleEntryIterator creates a new TupleEntryIterator instance.
   *
   * @param fields of type Fields
   */
  public TupleEntryIterator( Fields fields )
    {
    this.entry.fields = fields;
    this.entry.tuple = Tuple.size( fields.size() );
    }

  /**
   * Method getFields returns the fields of this TupleEntryIterator object.
   *
   * @return the fields (type Fields) of this TupleEntryIterator object.
   */
  public Fields getFields()
    {
    return entry.fields;
    }

  /**
   * Method getTupleEntry returns the entry of this TupleEntryIterator object.
   * <p/>
   * Since TupleEntry instances are re-used, this entry will inherit a new Tuple
   * on every {@link #next()} call.
   *
   * @return the entry (type TupleEntry) of this TupleEntryIterator object.
   */
  public TupleEntry getTupleEntry()
    {
    return entry;
    }
  }
