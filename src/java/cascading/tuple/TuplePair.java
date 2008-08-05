/*
 * Copyright (c) 2007-2008 Concurrent, Inc. All Rights Reserved.
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Class TuplePair is a utility class that is optimized to hold two Tuple instances for sorting and hashing of each
 * part independently.
 */
public class TuplePair extends Tuple
  {
  /** Field tuples */
  private Tuple[] tuples = new Tuple[2];

  /** Constructor Tuple creates a new Tuple instance. */
  public TuplePair()
    {
    super( (List<Comparable>) null ); // bypass unnecessary List creation
    tuples[ 0 ] = new Tuple();
    tuples[ 1 ] = new Tuple();
    }

  /**
   * Constructor TuplePair creates a new TuplePair instance.
   *
   * @param lhs of type Tuple
   * @param rhs of type Tuple
   */
  public TuplePair( Tuple lhs, Tuple rhs )
    {
    super( (List<Comparable>) null ); // bypass unnecessary List creation
    tuples[ 0 ] = lhs;
    tuples[ 1 ] = rhs;

    if( lhs == null )
      throw new IllegalArgumentException( "lhs may not be null" );

    if( rhs == null )
      throw new IllegalArgumentException( "rhs may not be null" );
    }

  /**
   * Method getLhs returns the lhs of this TuplePair object.
   *
   * @return the lhs (type Tuple) of this TuplePair object.
   */
  public Tuple getLhs()
    {
    return tuples[ 0 ];
    }

  /**
   * Method getRhs returns the rhs of this TuplePair object.
   *
   * @return the rhs (type Tuple) of this TuplePair object.
   */
  public Tuple getRhs()
    {
    return tuples[ 1 ];
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( object == null || getClass() != object.getClass() )
      return false;

    TuplePair tuplePair = (TuplePair) object;

    if( !Arrays.equals( tuples, tuplePair.tuples ) )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    return Arrays.hashCode( tuples );
    }

  @Override
  public void write( DataOutput dataOutput ) throws IOException
    {
    tuples[ 0 ].write( dataOutput );
    tuples[ 1 ].write( dataOutput );
    }

  @Override
  public void readFields( DataInput dataInput ) throws IOException
    {
    tuples[ 0 ].readFields( dataInput );
    tuples[ 1 ].readFields( dataInput );
    }

  @Override
  public int compareTo( Object other )
    {
    if( other instanceof TuplePair )
      return compareTo( (TuplePair) other );
    else
      return -1;
    }

  /**
   * Method compareTo compares this instance to the given TuplePair.
   *
   * @param tuplePair of type TuplePair
   * @return int
   */
  public int compareTo( TuplePair tuplePair )
    {
    int c = tuples[ 0 ].compareTo( tuplePair.tuples[ 0 ] );

    if( c != 0 )
      return c;

    c = tuples[ 1 ].compareTo( tuplePair.tuples[ 1 ] );

    return c;
    }

  @Override
  public String toString()
    {
    return tuples[ 0 ].print() + tuples[ 1 ].print();
    }
  }
