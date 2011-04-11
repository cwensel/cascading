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

package cascading.pipe.cogroup;

import java.util.Iterator;

import cascading.flow.FlowProcess;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/** Class GroupClosure is used internally to represent groups of tuples during grouping. */
public class GroupByClosure extends GroupClosure
  {
  Iterator values;

  public GroupByClosure( FlowProcess flowProcess, Fields[] groupingFields, Fields[] valueFields )
    {
    super( flowProcess, groupingFields, valueFields );
    }

  public int size()
    {
    return 1;
    }

  @Override
  public Iterator getIterator( int pos )
    {
    if( pos != 0 )
      throw new IllegalArgumentException( "invalid group position: " + pos );

    return makeIterator( 0, values );
    }

  @Override
  public boolean isEmpty( int pos )
    {
    return values != null;
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

      // todo: cache pos
      tuple.set( valueFields[ cleanPos ], groupingFields[ cleanPos ], grouping );

      return tuple;
      }

    public void remove()
      {
      throw new UnsupportedOperationException( "remove not supported" );
      }
    };
    }

  public void reset( Tuple grouping, Iterator values )
    {
    this.grouping = grouping;
    this.values = values;
    }
  }
