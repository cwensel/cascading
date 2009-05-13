/*
 * Copyright (c) 2007-2009 Concurrent, Inc. All Rights Reserved.
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

package cascading.pipe.cogroup;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;

import java.util.Arrays;
import java.util.Iterator;

/** Class GroupClosure is used internally to represent groups of tuples during grouping. */
public class GroupClosure
  {
  final Fields[] groupingFields;
  final Fields[] valueFields;
  final Tuple grouping;
  final Iterator values;

  public GroupClosure( Fields[] groupingFields, Fields[] valueFields, Tuple key, Iterator values )
    {
    this.groupingFields = Arrays.copyOf( groupingFields, groupingFields.length );
    this.valueFields = Arrays.copyOf( valueFields, valueFields.length );
    this.grouping = key;
    this.values = values;
    }

  public int size()
    {
    return 1;
    }

  public Tuple getGrouping()
    {
    return grouping;
    }

  public Iterator getIterator( int pos )
    {
    if( pos != 0 )
      throw new IllegalArgumentException( "invalid group position: " + pos );

    return makeIterator( 0, values );
    }

  protected Iterator<Tuple> makeIterator( final int pos, final Iterator values )
    {
    return new Iterator<Tuple>()
    {
    final int cleanPos = valueFields.length == 1 ? 0 : pos; // support repeated pipes

    public boolean hasNext()
      {
      return values.hasNext();
      }

    public Tuple next()
      {
      Tuple tuple = (Tuple) values.next();

      tuple.set( valueFields[ cleanPos ], groupingFields[ cleanPos ], grouping );

      return tuple;
      }

    public void remove()
      {
      throw new UnsupportedOperationException( "remove not supported" );
      }
    };
    }
  }
