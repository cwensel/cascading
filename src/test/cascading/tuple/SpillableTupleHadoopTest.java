/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;

import cascading.CascadingTestCase;
import cascading.tuple.hadoop.HadoopSpillableTupleList;
import cascading.tuple.hadoop.HadoopSpillableTupleMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

/**
 *
 */
public class SpillableTupleHadoopTest extends CascadingTestCase
  {
  public SpillableTupleHadoopTest()
    {
    super();
    }

  @Test
  public void testSpillList()
    {
    long time = System.currentTimeMillis();

    performListTest( 5, 50, null );
    performListTest( 49, 50, null );
    performListTest( 50, 50, null );
    performListTest( 51, 50, null );
    performListTest( 499, 50, null );
    performListTest( 500, 50, null );
    performListTest( 501, 50, null );

    System.out.println( "time = " + ( System.currentTimeMillis() - time ) );
    }

  @Test
  public void testSpillListCompressed()
    {
    GzipCodec codec = ReflectionUtils.newInstance( GzipCodec.class, new JobConf() );

    long time = System.currentTimeMillis();

    performListTest( 5, 50, codec );
    performListTest( 49, 50, codec );
    performListTest( 50, 50, codec );
    performListTest( 51, 50, codec );
    performListTest( 499, 50, codec );
    performListTest( 500, 50, codec );
    performListTest( 501, 50, codec );

    System.out.println( "time = " + ( System.currentTimeMillis() - time ) );
    }

  private void performListTest( int size, int threshold, CompressionCodec codec )
    {
    HadoopSpillableTupleList list = new HadoopSpillableTupleList( threshold, codec, new JobConf() );

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

  @Test
  public void testSpillMap()
    {
    long time = System.currentTimeMillis();

    performMapTest( 5, 5, 100, 20, null );
    performMapTest( 5, 50, 100, 20, null );
    performMapTest( 50, 5, 200, 20, null );
    performMapTest( 500, 50, 7000, 20, null );

    System.out.println( "time = " + ( System.currentTimeMillis() - time ) );
    }

  @Test
  public void testSpillMapCompressed()
    {
    long time = System.currentTimeMillis();

    GzipCodec codec = ReflectionUtils.newInstance( GzipCodec.class, new JobConf() );

    performMapTest( 5, 5, 100, 20, codec );
    performMapTest( 5, 50, 100, 20, codec );
    performMapTest( 50, 5, 200, 20, codec );
    performMapTest( 500, 50, 7000, 20, codec );

    System.out.println( "time = " + ( System.currentTimeMillis() - time ) );
    }

  private void performMapTest( int numKeys, int listSize, int mapThreshold, int listThreshold, CompressionCodec codec )
    {
    HadoopSpillableTupleMap map = new HadoopSpillableTupleMap( mapThreshold, listThreshold, codec, new JobConf() );

    Set<Integer> keySet = new HashSet<Integer>();
    Random gen = new Random( 1 );

    for( int i = 0; i < listSize * numKeys; i++ )
      {
      String aString = "string number " + i;
      double random = Math.random();

      double keys = numKeys / 3.0;
      int key = (int) ( gen.nextDouble() * keys + gen.nextDouble() * keys + gen.nextDouble() * keys );
      map.get( new Tuple( key ) ).add( new Tuple( i, aString, random, new Text( aString ) ) );

      keySet.add( key );
      }

    assertEquals( "not equal: map.size();", keySet.size(), map.size() );
    }
  }
