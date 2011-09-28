/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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

package cascading.tuple;

import java.util.Iterator;

import cascading.CascadingTestCase;
import cascading.flow.hadoop.HadoopSpillableTupleList;
import cascading.test.PlatformTest;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;

/**
 *
 */
@PlatformTest(platforms = {"hadoop"})
public class SpillableTupleTest extends CascadingTestCase
  {
  public SpillableTupleTest()
    {
    super();
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
    HadoopSpillableTupleList list = new HadoopSpillableTupleList( threshold, null, codec );

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
      assertEquals( "wrong value", "string number " + count, tuple.get( 3 ).toString() );
      i = value;
      count++;
      }

    assertEquals( "not equal: list.size();", size, count );

    Iterator<Tuple> iterator = list.iterator();

    assertEquals( "not equal: iterator.next().get(0)", "string number 0", iterator.next().get( 1 ) );
    }
  }
