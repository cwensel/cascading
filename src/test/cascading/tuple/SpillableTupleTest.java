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

package cascading.tuple;

import cascading.CascadingTestCase;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;

import java.util.Iterator;

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

    performSpillTest( 5, 50, null );
    performSpillTest( 49, 50, null );
    performSpillTest( 50, 50, null );
    performSpillTest( 51, 50, null );
    performSpillTest( 499, 50, null );
    performSpillTest( 500, 50, null );
    performSpillTest( 501, 50, null );

    System.out.println( "time = " + ( System.currentTimeMillis() - time ) );
    }

  public void testSpillCompressed()
    {
    GzipCodec codec = ReflectionUtils.newInstance( GzipCodec.class, new JobConf() );

    long time = System.currentTimeMillis();

    performSpillTest( 5, 50, codec );
    performSpillTest( 49, 50, codec );
    performSpillTest( 50, 50, codec );
    performSpillTest( 51, 50, codec );
    performSpillTest( 499, 50, codec );
    performSpillTest( 500, 50, codec );
    performSpillTest( 501, 50, codec );

    System.out.println( "time = " + ( System.currentTimeMillis() - time ) );
    }

  private void performSpillTest( int size, int threshold, CompressionCodec codec )
    {
    SpillableTupleList list = new SpillableTupleList( threshold, codec );

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
