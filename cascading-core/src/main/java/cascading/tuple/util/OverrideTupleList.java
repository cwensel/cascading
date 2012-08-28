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

import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 *
 */
class OverrideTupleList extends AbstractList<Object> implements Resettable<Tuple>
  {
  private int[] basePos;
  private Tuple base;
  private int[] overridePos;
  private Tuple override;

  public OverrideTupleList( Fields baseFields, Fields overrideFields )
    {
    this( baseFields.getPos(), baseFields.getPos( overrideFields ) );
    }

  public OverrideTupleList( int[] basePos, int[] overridePos )
    {
    this( basePos, null, overridePos, null );
    }

  public OverrideTupleList( Fields baseFields, Tuple baseTuple, Fields overrideFields, Tuple overrideTuple )
    {
    this( baseFields.getPos(), baseTuple, overrideFields.getPos(), overrideTuple );
    }

  public OverrideTupleList( int[] basePos, Tuple baseTuple, int[] overridePos, Tuple overrideTuple )
    {
    this.basePos = basePos;
    this.base = baseTuple;

    this.overridePos = new int[ basePos.length ];

    Arrays.fill( this.overridePos, -1 );

    for( int i = 0; i < overridePos.length; i++ )
      this.overridePos[ overridePos[ i ] ] = i;

    this.override = overrideTuple;
    }

  public void reset( Tuple... tuples )
    {
    this.base = tuples[ 0 ];
    this.override = tuples[ 1 ];
    }

  @Override
  public Object get( int index )
    {
    if( index >= basePos.length )
      throw new IllegalArgumentException( "invalid index: " + index + ", length: " + basePos.length );

    if( overridePos[ index ] != -1 )
      return override.getObject( overridePos[ index ] );

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
    return base.size();
    }

  @Override
  public String toString()
    {
    final StringBuilder sb = new StringBuilder();
    sb.append( "OverrideTupleList" );
    sb.append( "{basePos=" ).append( basePos == null ? "null" : "" );
    for( int i = 0; basePos != null && i < basePos.length; ++i )
      sb.append( i == 0 ? "" : ", " ).append( basePos[ i ] );
    sb.append( ", base=" ).append( base );
    sb.append( ", overridePos=" ).append( overridePos == null ? "null" : "" );
    for( int i = 0; overridePos != null && i < overridePos.length; ++i )
      sb.append( i == 0 ? "" : ", " ).append( overridePos[ i ] );
    sb.append( ", override=" ).append( override );
    sb.append( '}' );
    return sb.toString();
    }
  }
