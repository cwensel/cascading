/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
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

import java.util.Arrays;
import java.util.List;

import cascading.tuple.Tuple;
import cascading.tuple.util.Resettable2;

/**
 * Class TuplePair is a utility class that is optimized to hold two Tuple instances for sorting and hashing of each
 * part independently.
 */
public class TuplePair extends Tuple implements Resettable2<Tuple, Tuple>
  {
  /** Field tuples */
  private final Tuple[] tuples = new Tuple[ 2 ];

  /**
   * Returns a reference to the private tuples of the given TuplePark
   *
   * @param tuplePair of type Tuple[]
   * @return Tuple[]
   */
  public static Tuple[] tuples( TuplePair tuplePair )
    {
    return tuplePair.tuples;
    }

  /** Constructor Tuple creates a new Tuple instance. */
  public TuplePair()
    {
    super( (List<Object>) null ); // bypass unnecessary List creation
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
    super( (List<Object>) null ); // bypass unnecessary List creation
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
  public void reset( Tuple lhs, Tuple rhs )
    {
    tuples[ 0 ] = lhs;
    tuples[ 1 ] = rhs;
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
  public int compareTo( Object other )
    {
    if( other instanceof TuplePair )
      return compareTo( (TuplePair) other );
    else
      return -1;
    }

  @Override
  public int compareTo( Tuple other )
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

  @Override
  public String print()
    {
    return tuples[ 0 ].print() + tuples[ 1 ].print();
    }
  }
