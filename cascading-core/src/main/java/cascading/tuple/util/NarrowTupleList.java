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
