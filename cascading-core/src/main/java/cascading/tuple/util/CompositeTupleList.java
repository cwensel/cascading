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
class CompositeTupleList extends AbstractList<Object> implements Resettable<Tuple>
  {
  private int[][] basePos;
  private Tuple[] bases;
  private int[] skip;
  private int length;

  private static int[][] makePos( Tuple[] bases )
    {
    int[][] pos = new int[ bases.length ][];

    for( int i = 0; i < bases.length; i++ )
      {
      Tuple base = bases[ i ];
      pos[ i ] = Fields.size( base.size() ).getPos();
      }

    return pos;
    }

  private static int[][] makePos( Fields[] fields )
    {
    int[][] pos = new int[ fields.length ][];

    for( int i = 0; i < fields.length; i++ )
      pos[ i ] = fields[ i ].getPos();

    return pos;
    }

  public CompositeTupleList( Fields... fields )
    {
    this( makePos( fields ), new Tuple[ fields.length ] );
    }

  public CompositeTupleList( int[]... basePos )
    {
    this( basePos, new Tuple[ basePos.length ] );
    }

  public CompositeTupleList( Tuple... bases )
    {
    this( makePos( bases ), bases );
    }

  public CompositeTupleList( Fields[] fields, Tuple[] bases )
    {
    this( makePos( fields ), bases );
    }

  protected CompositeTupleList( Fields fields, Tuple base )
    {
    this( new int[][]{fields.getPos()}, new Tuple[]{base} );
    }

  protected CompositeTupleList( int[] basePos, Tuple base )
    {
    this( new int[][]{basePos}, new Tuple[]{base} );
    }

  public CompositeTupleList( int[][] basePos, Tuple[] bases )
    {
    this.basePos = basePos;
    this.bases = bases;

    this.skip = new int[ basePos.length + 1 ];

    for( int i = 0; i < basePos.length; i++ )
      {
      this.skip[ i + 1 ] = this.skip[ i ] + basePos[ i ].length;
      this.length += basePos[ i ].length;
      }
    }

  public void reset( Tuple... bases )
    {
    if( this.basePos.length != bases.length )
      throw new IllegalArgumentException( "bases is wrong length, expects: " + this.basePos.length + ", got: " + bases.length );

    this.bases = bases;
    }

  @Override
  public Object get( int index )
    {
    for( int i = 0; i < basePos.length; i++ )
      {
      if( index < skip[ i + 1 ] )
        return bases[ i ].getObject( basePos[ i ][ index - skip[ i ] ] );
      }

    throw new IllegalArgumentException( "invalid index: " + index + ", length: " + basePos.length );
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
    return length;
    }

  @Override
  public String toString()
    {
    final StringBuilder sb = new StringBuilder();
    sb.append( "CompositeTupleList" );
    sb.append( "{basePos=" ).append( basePos == null ? "null" : Arrays.asList( basePos ).toString() );
    sb.append( ", bases=" ).append( bases == null ? "null" : Arrays.asList( bases ).toString() );
    sb.append( ", skip=" ).append( skip == null ? "null" : "" );
    for( int i = 0; skip != null && i < skip.length; ++i )
      sb.append( i == 0 ? "" : ", " ).append( skip[ i ] );
    sb.append( ", length=" ).append( length );
    sb.append( '}' );
    return sb.toString();
    }
  }
