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

import java.util.Iterator;

import cascading.CascadingTestCase;
import org.apache.hadoop.io.Text;

/**
 *
 */
public class SpillableTupleTest extends CascadingTestCase
  {
  public SpillableTupleTest()
    {
    super( "spillable tuple list test" );
    }

  public void testSpill()
    {
    long time = System.currentTimeMillis();

    performSpillTest( 5, 50 );
    performSpillTest( 49, 50 );
    performSpillTest( 50, 50 );
    performSpillTest( 51, 50 );
    performSpillTest( 499, 50 );
    performSpillTest( 500, 50 );
    performSpillTest( 501, 50 );

    System.out.println( "time = " + ( System.currentTimeMillis() - time ) );
    }

  private void performSpillTest( int size, int threshold )
    {
    SpillableTupleList list = new SpillableTupleList( threshold );

    for( int i = 0; i < size; i++ )
      {
      String aString = "string number " + i;
      double random = Math.random();

      list.add( new Tuple( i, aString, random, new Text( aString ) ) );
      }

    assertEquals( "not equal: list.size();", size, list.size() );
    assertEquals( "not equal: list.getNumFiles()", (int) Math.floor( size / threshold ), list.getNumFiles() );

    int i = -1;
    int count = 0;
    for( Tuple tuple : list )
      {
      int value = tuple.getInteger( 0 );
      assertTrue( "wrong diff", value - i == 1 );
      i = value;
      count++;
      }

    assertEquals( "not equal: list.size();", size, count );

    Iterator<Tuple> iterator = list.iterator();

    assertEquals( "not equal: iterator.next().get(0)", "string number 0", iterator.next().get( 1 ) );
    }
  }
