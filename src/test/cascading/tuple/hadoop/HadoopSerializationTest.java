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

package cascading.tuple.hadoop;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import cascading.CascadingTestCase;
import cascading.tuple.Tuple;
import cascading.tuple.TupleInputStream;
import cascading.tuple.TupleOutputStream;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;

/**
 *
 */
public class HadoopSerializationTest extends CascadingTestCase
  {
  String outputPath = "build/test/output/tuples/serialization";

  public HadoopSerializationTest()
    {
    super( "serialization tuple test" );
    }

  public void testInputOutputSerialization() throws IOException
    {
    long time = System.currentTimeMillis();

    JobConf jobConf = new JobConf();

    jobConf.set( "io.serializations", TestSerialization.class.getName() ); // disable/replace WritableSerialization class
    jobConf.set( "cascading.serialization.tokens", "1000=" + BooleanWritable.class.getName() + ",10001=" + Text.class.getName() ); // not using Text, just testing parsing

    TupleSerialization tupleSerialization = new TupleSerialization( jobConf );

    File file = new File( outputPath );

    file.mkdirs();
    file = new File( file, "/test.bytes" );

    TupleOutputStream output = new TupleOutputStream( new FileOutputStream( file, false ), tupleSerialization.getElementWriter() );

    for( int i = 0; i < 501; i++ ) // 501 is arbitrary
      {
      String aString = "string number " + i;
      double random = Math.random();

      output.writeTuple( new Tuple( i, aString, random, new TestText( aString ), new Tuple( "inner tuple", new BytesWritable( "some string".getBytes() ) ), new BytesWritable( "some string".getBytes() ), new BooleanWritable( false ) ) );
      }

    output.close();

    assertEquals( "wrong size", 92582L, file.length() ); // just makes sure the file size doesnt change from expected

    TupleInputStream input = new TupleInputStream( new FileInputStream( file ), tupleSerialization.getElementReader() );

    int k = -1;
    for( int i = 0; i < 501; i++ )
      {
      Tuple tuple = input.readTuple();
      int value = tuple.getInteger( 0 );
      assertTrue( "wrong diff", value - k == 1 );
      assertTrue( "wrong type", tuple.get( 3 ) instanceof TestText );
      assertTrue( "wrong type", tuple.get( 4 ) instanceof Tuple );
      assertTrue( "wrong type", tuple.get( 5 ) instanceof BytesWritable );
      assertTrue( "wrong type", tuple.get( 6 ) instanceof BooleanWritable );
      k = value;
      }

    input.close();

    System.out.println( "time = " + ( System.currentTimeMillis() - time ) );
    }


  }