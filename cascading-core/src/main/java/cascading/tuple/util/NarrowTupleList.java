/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
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

import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 *
 */
class NarrowTupleList extends AbstractList<Object> implements Resettable<Tuple>
  {
  private int[] basePos;
  private Tuple base;

  public NarrowTupleList( Fields fields )
    {
    this( fields.getPos(), null );
    }

  public NarrowTupleList( int[] basePos )
    {
    this( basePos, null );
    }

  public NarrowTupleList( Fields fields, Tuple base )
    {
    this( fields.getPos(), base );
    }

  public NarrowTupleList( int[] basePos, Tuple base )
    {
    this.basePos = basePos;
    this.base = base;
    }

  public void reset( Tuple... bases )
    {
    if( bases.length != 1 )
      throw new IllegalArgumentException( "wrong number of arguments, only expected one Tuple" );

    this.base = bases[ 0 ];
    }

  @Override
  public Object get( int index )
    {
    if( index >= basePos.length )
      throw new IllegalArgumentException( "invalid index: " + index + ", length: " + basePos.length );

    return base.getObject( basePos[ index ] );
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
    return basePos.length;
    }

  @Override
  public String toString()
    {
    final StringBuilder sb = new StringBuilder();
    sb.append( "NarrowTupleList" );
    sb.append( "{basePos=" ).append( basePos == null ? "null" : "" );
    for( int i = 0; basePos != null && i < basePos.length; ++i )
      sb.append( i == 0 ? "" : ", " ).append( basePos[ i ] );
    sb.append( ", base=" ).append( base );
    sb.append( '}' );
    return sb.toString();
    }
  }
