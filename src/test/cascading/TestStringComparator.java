/*
 * Copyright (c) 2007-2010 Concurrent, Inc. All Rights Reserved.
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
import cascading.tuple.TupleInputStream;
import cascading.tuple.hadoop.BufferedInputStream;

/**
 *
 */
public class TestStringComparator implements StreamComparator<BufferedInputStream>, Comparator<String>, Serializable
  {
  boolean reverse = true;

  public TestStringComparator()
    {
    }

  public TestStringComparator( boolean reverse )
    {
    this.reverse = reverse;
    }

  @Override
  public int compare( String o1, String o2 )
    {
    return reverse ? o2.compareTo( o1 ) : o1.compareTo( o2 );
    }

  @Override
  public int compare( BufferedInputStream lhsStream, BufferedInputStream rhsStream )
    {
    TupleInputStream lhsInput = new TupleInputStream( lhsStream );
    TupleInputStream rhsInput = new TupleInputStream( rhsStream );

    try
      {
      // explicit for debugging purposes
      String s1 = (String) lhsInput.readString();
      String s2 = (String) rhsInput.readString();
      return reverse ? s2.compareTo( s1 ) : s1.compareTo( s2 );
      }
    catch( IOException exception )
      {
      throw new CascadingException( exception );
      }
    }
  }
