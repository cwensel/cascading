/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
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

package cascading.tuple.hadoop;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;

import cascading.CascadingTestCase;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.tuple.Tuple;
import cascading.tuple.collect.SpillableProps;
import cascading.tuple.hadoop.collect.HadoopSpillableTupleList;
import cascading.tuple.hadoop.collect.HadoopSpillableTupleMap;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.serializer.WritableSerialization;
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

    performListTest( 5, 50, null, 0 );
    performListTest( 49, 50, null, 0 );
    performListTest( 50, 50, null, 0 );
    performListTest( 51, 50, null, 1 );
    performListTest( 499, 50, null, 9 );
    performListTest( 500, 50, null, 9 );
    performListTest( 501, 50, null, 10 );

    System.out.println( "time = " + ( System.currentTimeMillis() - time ) );
    }

  @Test
  public void testSpillListCompressed()
    {
    GzipCodec codec = ReflectionUtils.newInstance( GzipCodec.class, new JobConf() );

    long time = System.currentTimeMillis();

    performListTest( 5, 50, codec, 0 );
    performListTest( 49, 50, codec, 0 );
    performListTest( 50, 50, codec, 0 );
    performListTest( 51, 50, codec, 1 );
    performListTest( 499, 50, codec, 9 );
    performListTest( 500, 50, codec, 9 );
    performListTest( 501, 50, codec, 10 );

    System.out.println( "time = " + ( System.currentTimeMillis() - time ) );
    }

  private void performListTest( int size, int threshold, CompressionCodec codec, int spills )
    {
    JobConf jobConf = new JobConf();

    jobConf.set( "io.serializations", TestSerialization.class.getName() + "," + WritableSerialization.class.getName() ); // disable/replace WritableSerialization class
    jobConf.set( "cascading.serialization.tokens", "1000=" + BooleanWritable.class.getName() + ",10001=" + Text.class.getName() ); // not using Text, just testing parsing

    HadoopSpillableTupleList list = new HadoopSpillableTupleList( threshold, codec, jobConf );

    for( int i = 0; i < size; i++ )
      {
      String aString = "string number " + i;
      double random = Math.random();

      list.add( new Tuple( i, aString, random, new Text( aString ), new TestText( aString ), new Tuple( "inner tuple", new BytesWritable( aString.getBytes() ) ) ) );
      }

    assertEquals( "not equal: list.size();", size, list.size() );

    assertEquals( "not equal: list.getNumFiles()", spills, list.spillCount() );

    int i = -1;
    int count = 0;
    for( Tuple tuple : list )
      {
      int value = tuple.getInteger( 0 );
      assertTrue( "wrong diff", value - i == 1 );
      assertEquals( "wrong value", "string number " + count, tuple.getObject( 3 ).toString() );
      assertEquals( "wrong value", "string number " + count, tuple.getObject( 4 ).toString() );
      assertTrue( "wrong type", tuple.getObject( 5 ) instanceof Tuple );

      BytesWritable bytesWritable = (BytesWritable) ( (Tuple) tuple.getObject( 5 ) ).getObject( 1 );
      byte[] bytes = bytesWritable.getBytes();
      String actual = new String( bytes, 0, bytesWritable.getLength() );

      assertEquals( "wrong value", "string number " + count, actual );

      i = value;
      count++;
      }

    assertEquals( "not equal: list.size();", size, count );

    Iterator<Tuple> iterator = list.iterator();

    assertEquals( "not equal: iterator.next().get(1)", "string number 0", iterator.next().getObject( 1 ) );
    assertEquals( "not equal: iterator.next().get(1)", "string number 1", iterator.next().getObject( 1 ) );
    }

  @Test
  public void testSpillMap()
    {
    long time = System.currentTimeMillis();

    JobConf jobConf = new JobConf();

    performMapTest( 5, 5, 100, 20, jobConf );
    performMapTest( 5, 50, 100, 20, jobConf );
    performMapTest( 50, 5, 200, 20, jobConf );
    performMapTest( 500, 50, 7000, 20, jobConf );

    System.out.println( "time = " + ( System.currentTimeMillis() - time ) );
    }

  @Test
  public void testSpillMapCompressed()
    {
    long time = System.currentTimeMillis();

    JobConf jobConf = new JobConf();

    jobConf.set( SpillableProps.SPILL_CODECS, "org.apache.hadoop.io.compress.GzipCodec" );

    performMapTest( 5, 5, 100, 20, jobConf );
    performMapTest( 5, 50, 100, 20, jobConf );
    performMapTest( 50, 5, 200, 20, jobConf );
    performMapTest( 500, 50, 7000, 20, jobConf );

    System.out.println( "time = " + ( System.currentTimeMillis() - time ) );
    }

  private void performMapTest( int numKeys, int listSize, int mapThreshold, int listThreshold, JobConf jobConf )
    {
    jobConf.set( "io.serializations", TestSerialization.class.getName() + "," + WritableSerialization.class.getName() ); // disable/replace WritableSerialization class
    jobConf.set( "cascading.serialization.tokens", "1000=" + BooleanWritable.class.getName() + ",10001=" + Text.class.getName() ); // not using Text, just testing parsing

    HadoopFlowProcess flowProcess = new HadoopFlowProcess( jobConf );
    HadoopSpillableTupleMap map = new HadoopSpillableTupleMap( SpillableProps.defaultMapInitialCapacity, SpillableProps.defaultMapLoadFactor, mapThreshold, listThreshold, flowProcess );

    Set<Integer> keySet = new HashSet<Integer>();
    Random gen = new Random( 1 );

    for( int i = 0; i < listSize * numKeys; i++ )
      {
      String aString = "string number " + i;
      double random = Math.random();

      double keys = numKeys / 3.0;
      int key = (int) ( gen.nextDouble() * keys + gen.nextDouble() * keys + gen.nextDouble() * keys );

      Tuple tuple = new Tuple( i, aString, random, new Text( aString ), new TestText( aString ), new Tuple( "inner tuple", new BytesWritable( aString.getBytes() ) ) );

      map.get( new Tuple( key ) ).add( tuple );

      keySet.add( key );
      }

    // the list test above verifies the contents are being serialized, the Map is just a container of lists.
    assertEquals( "not equal: map.size();", keySet.size(), map.size() );
    }
  }
