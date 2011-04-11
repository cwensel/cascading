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

package cascading.tuple.hadoop;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import cascading.PlatformTestCase;
import cascading.test.PlatformTest;
import cascading.tuple.Tuple;
import cascading.tuple.TupleInputStream;
import cascading.tuple.TupleOutputStream;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.apache.hadoop.mapred.JobConf;

/**
 *
 */
@PlatformTest(platforms = {"hadoop"})
public class HadoopSerializationTest extends PlatformTestCase
  {
  public HadoopSerializationTest()
    {
    }

  public void testInputOutputSerialization() throws IOException
    {
    long time = System.currentTimeMillis();

    JobConf jobConf = new JobConf();

    jobConf.set( "io.serializations", TestSerialization.class.getName() + "," + WritableSerialization.class.getName() ); // disable/replace WritableSerialization class
    jobConf.set( "cascading.serialization.tokens", "1000=" + BooleanWritable.class.getName() + ",10001=" + Text.class.getName() ); // not using Text, just testing parsing

    TupleSerialization tupleSerialization = new TupleSerialization( jobConf );

    File file = new File( getOutputPath( "serialization" ) );

    file.mkdirs();
    file = new File( file, "/test.bytes" );

    TupleOutputStream output = new HadoopTupleOutputStream( new FileOutputStream( file, false ), tupleSerialization.getElementWriter() );

    for( int i = 0; i < 501; i++ ) // 501 is arbitrary
      {
      String aString = "string number " + i;
      double random = Math.random();

      output.writeTuple( new Tuple( i, aString, random, new TestText( aString ), new Tuple( "inner tuple", new BytesWritable( "some string".getBytes() ) ), new BytesWritable( Integer.toString( i ).getBytes( "UTF-8" ) ), new BooleanWritable( false ) ) );
      }

    output.close();

    assertEquals( "wrong size", 89967L, file.length() ); // just makes sure the file size doesnt change from expected

    TupleInputStream input = new HadoopTupleInputStream( new FileInputStream( file ), tupleSerialization.getElementReader() );

    int k = -1;
    for( int i = 0; i < 501; i++ )
      {
      Tuple tuple = input.readTuple();
      int value = tuple.getInteger( 0 );
      assertTrue( "wrong diff", value - k == 1 );
      assertTrue( "wrong type", tuple.get( 3 ) instanceof TestText );
      assertTrue( "wrong type", tuple.get( 4 ) instanceof Tuple );
      assertTrue( "wrong type", tuple.get( 5 ) instanceof BytesWritable );

      byte[] bytes = ( (BytesWritable) tuple.get( 5 ) ).getBytes();
      String string = new String( bytes, 0, bytes.length > 1 ? bytes.length - 1 : bytes.length, "UTF-8" );
      assertEquals( "wrong value", Integer.parseInt( string ), i );
      assertTrue( "wrong type", tuple.get( 6 ) instanceof BooleanWritable );
      k = value;
      }

    input.close();

    System.out.println( "time = " + ( System.currentTimeMillis() - time ) );
    }
  }