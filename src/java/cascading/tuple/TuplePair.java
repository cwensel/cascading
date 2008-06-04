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

/**
 * Class TuplePair is a utility class that is optimized to hold two Tuple instances for sorting and hashing
 * within Hadoop
 */
public class TuplePair extends Tuple
  {
  private Tuple[] tuples = new Tuple[2];

  public TuplePair()
    {
    tuples[ 0 ] = new Tuple();
    tuples[ 1 ] = new Tuple();
    }

  public TuplePair( Tuple lhs, Tuple rhs )
    {
    tuples[ 0 ] = lhs;
    tuples[ 1 ] = rhs;
    }

  public Tuple getLhs()
    {
    return tuples[ 0 ];
    }

  public Tuple getRhs()
    {
    return tuples[ 1 ];
    }

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

  public int hashCode()
    {
    return tuples != null ? Arrays.hashCode( tuples ) : 0;
    }

  public void write( DataOutput dataOutput ) throws IOException
    {
    tuples[ 0 ].write( dataOutput );
    tuples[ 1 ].write( dataOutput );
    }

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

  public int compareTo( TuplePair tuplePair )
    {
    int c = tuples[ 0 ].compareTo( tuplePair.tuples[ 0 ] );

    if( c != 0 )
      return c;

    c = tuples[ 1 ].compareTo( tuplePair.tuples[ 1 ] );

    return c;
    }

  public String toString()
    {
    return tuples[ 0 ].print() + tuples[ 1 ].print();
    }
  }
