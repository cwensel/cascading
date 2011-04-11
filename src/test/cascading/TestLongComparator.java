/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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

package cascading;

import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;

import cascading.tuple.StreamComparator;
import cascading.tuple.hadoop.BufferedInputStream;
import cascading.tuple.hadoop.HadoopTupleInputStream;
import cascading.tuple.hadoop.TupleSerialization;

/**
 *
 */
public class TestLongComparator implements StreamComparator<BufferedInputStream>, Comparator<Long>, Serializable
  {
  boolean reverse = true;

  public TestLongComparator()
    {
    }

  public TestLongComparator( boolean reverse )
    {
    this.reverse = reverse;
    }

  @Override
  public int compare( Long o1, Long o2 )
    {
    return reverse ? o2.compareTo( o1 ) : o1.compareTo( o2 );
    }

  @Override
  public int compare( BufferedInputStream lhsStream, BufferedInputStream rhsStream )
    {
    HadoopTupleInputStream lhsInput = new HadoopTupleInputStream( lhsStream, new TupleSerialization().getElementReader() );
    HadoopTupleInputStream rhsInput = new HadoopTupleInputStream( rhsStream, new TupleSerialization().getElementReader() );

    try
      {
      // explicit for debugging purposes
      Long l1 = (Long) lhsInput.readVLong();
      Long l2 = (Long) rhsInput.readVLong();
      return reverse ? l2.compareTo( l1 ) : l1.compareTo( l2 );
      }
    catch( IOException exception )
      {
      throw new CascadingException( exception );
      }
    }

  }
