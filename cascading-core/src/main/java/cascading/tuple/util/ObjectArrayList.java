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

package cascading.tuple.util;

import java.util.AbstractList;
import java.util.Arrays;

/**
 *
 */
class ObjectArrayList extends AbstractList<Object> implements Resettable<Object>
  {
  Object[] array;

  ObjectArrayList()
    {
    this.array = new Object[ 0 ];
    }

  ObjectArrayList( Object[] array )
    {
    this.array = array;
    }

  @Override
  public Object get( int index )
    {
    if( index >= array.length )
      throw new IllegalArgumentException( "invalid index: " + index + ", length: " + array.length );

    return array[ index ];
    }

  @Override
  public Object set( int index, Object element )
    {
    if( index >= array.length )
      throw new IllegalArgumentException( "invalid index: " + index + ", length: " + array.length );

    return array[ index ] = element;
    }

  @Override
  public Object[] toArray()
    {
    Object[] result = new Object[ size() ];

    for( int i = 0; i < size(); i++ )
      result[ i ] = get( i );

    return result;
    }

  @Override
  public <T> T[] toArray( T[] a )
    {
    for( int i = 0; i < a.length; i++ )
      a[ i ] = (T) get( i );

    return a;
    }

  @Override
  public int size()
    {
    return array.length;
    }

  @Override
  public void reset( Object... values )
    {
    array = values;
    }

  @Override
  public String toString()
    {
    final StringBuilder sb = new StringBuilder();
    sb.append( "ObjectArrayList" );
    sb.append( "{array=" ).append( array == null ? "null" : Arrays.asList( array ).toString() );
    sb.append( '}' );
    return sb.toString();
    }
  }
