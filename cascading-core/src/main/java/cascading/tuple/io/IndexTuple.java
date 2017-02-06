/*
 * Copyright (c) 2007-2017 Xplenty, Inc. All Rights Reserved.
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

package cascading.tuple.io;

import java.util.List;

import cascading.tuple.Tuple;

/** Class IndexTuple allows for managing an int index value with a Tuple instance. Used internally for co-grouping values. */
public class IndexTuple extends Tuple implements Comparable<Object>
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
    return printTo( new StringBuffer() ).toString();
    }

  public StringBuffer printTo( StringBuffer buffer )
    {
    buffer.append( "{" );

    buffer.append( index ).append( ":" );

    tuple.printTo( buffer );

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
