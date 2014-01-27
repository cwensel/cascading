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
