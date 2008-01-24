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
import java.util.ListIterator;

import cascading.flow.FlowCollector;

/**
 *
 */
public class TupleEntryListIterator implements ListIterator<TupleEntry>, FlowCollector
  {
  final TupleEntry entry;
  final ListIterator<Tuple> iterator;

  public TupleEntryListIterator( TupleEntry entry, ListIterator<Tuple> iterator )
    {
    this.entry = entry;
    this.iterator = iterator;
    }

  public boolean hasNext()
    {
    return iterator.hasNext();
    }

  public TupleEntry next()
    {
    entry.tuple = iterator.next();

    return entry;
    }

  public boolean hasPrevious()
    {
    return iterator.hasPrevious();
    }

  public TupleEntry previous()
    {
    entry.tuple = iterator.previous();

    return entry;
    }

  public int nextIndex()
    {
    return iterator.nextIndex();
    }

  public int previousIndex()
    {
    return iterator.previousIndex();
    }

  public void remove()
    {
    iterator.remove();
    }

  public void set( TupleEntry entry )
    {
    iterator.set( entry.tuple );
    }

  public void collect( Tuple tuple )
    {
    add( tuple );
    }

  public void collect( Tuple key, Iterator tupleIterator )
    {

    }

  public void add( TupleEntry entry )
    {
    if( entry.tuple.isEmpty() )
      return;

    if( this.entry.fields != null && !this.entry.fields.isUnknown() && this.entry.fields.size() != entry.tuple.size() )
      throw new TupleException( "added the wrong number of fields, expected: " + this.entry.fields + ", got result size: " + entry.tuple.size() );

    iterator.add( entry.tuple );
    }

  public void set( Tuple tuple )
    {
    if( tuple.isEmpty() )
      return;

    if( this.entry.fields != null && !entry.fields.isUnknown() && entry.fields.size() != tuple.size() )
      throw new TupleException( "added the wrong number of fields, expected: " + entry.fields + ", got result size: " + tuple.size() );

    iterator.set( tuple );
    }

  public void add( Tuple tuple )
    {
    if( tuple.isEmpty() )
      return;

    if( !entry.fields.isUnknown() && entry.fields.size() != tuple.size() )
      throw new TupleException( "added the wrong number of fields, expected: " + entry.fields + ", got result size: " + tuple.size() );

    iterator.add( tuple );
    }
  }
