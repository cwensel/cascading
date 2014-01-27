/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
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
