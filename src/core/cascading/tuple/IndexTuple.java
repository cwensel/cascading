/*
 * Copyright (c) 2007-2010 Concurrent, Inc. All Rights Reserved.
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

import java.util.List;

/** Class IndexTuple allows for managing an int index value with a Tuple instance. Used internally for co-grouping values. */
public class IndexTuple extends Tuple implements Comparable
  {
  int index;
  Tuple tuple;

  /** Constructor IndexTuple creates a new IndexTuple instance. */
  public IndexTuple()
    {
    super( (List<Object>) null );
    }

  /**
   * Constructor IndexTuple creates a new IndexTuple instance.
   *
   * @param index of type int
   * @param tuple of type Tuple
   */
  public IndexTuple( int index, Tuple tuple )
    {
    super( (List<Comparable>) null );
    this.index = index;
    this.tuple = tuple;
    }

  public void setIndex( int index )
    {
    this.index = index;
    }

  public int getIndex()
    {
    return index;
    }

  public void setTuple( Tuple tuple )
    {
    this.tuple = tuple;
    }

  public Tuple getTuple()
    {
    return tuple;
    }

  @Override
  public String print()
    {
    return print( new StringBuffer() ).toString();
    }

  protected StringBuffer print( StringBuffer buffer )
    {
    buffer.append( "{" );

    buffer.append( index ).append( ":" );

    tuple.print( buffer );

    buffer.append( "}" );

    return buffer;
    }

  public int compareTo( Object object )
    {
    if( object instanceof IndexTuple )
      return compareTo( (IndexTuple) object );

    return -1;
    }

  public int compareTo( IndexTuple indexTuple )
    {
    int c = this.index - indexTuple.index;

    if( c != 0 )
      return c;

    return this.tuple.compareTo( indexTuple.tuple );
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( object == null || getClass() != object.getClass() )
      return false;

    IndexTuple that = (IndexTuple) object;

    if( index != that.index )
      return false;
    if( tuple != null ? !tuple.equals( that.tuple ) : that.tuple != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = index;
    result = 31 * result + ( tuple != null ? tuple.hashCode() : 0 );
    return result;
    }

  @Override
  public String toString()
    {
    return "[" + index + "]" + tuple;
    }
  }
