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

package cascading.scheme;

import java.io.IOException;

import cascading.PlatformTestCase;
import cascading.flow.Flow;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.test.PlatformTest;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryIterator;

import static data.InputData.testDelimited;
import static data.InputData.testDelimitedSpecialCharData;

/**
 *
 */
@PlatformTest(platforms = {"local", "hadoop"})
public class TextDelimitedTest extends PlatformTestCase
  {
  public TextDelimitedTest()
    {
    }

  public void testQuotedText() throws IOException
    {
    runQuotedText( "normchar", testDelimited, ",", false );
    }

  public void testQuotedTextAll() throws IOException
    {
    runQuotedText( "normchar", testDelimited, ",", true );
    }

  public void testQuotedTextSpecChar() throws IOException
    {
    runQuotedText( "specchar", testDelimitedSpecialCharData, "|", false );
    }

  public void testQuotedTextSpecCharAll() throws IOException
    {
    runQuotedText( "specchar", testDelimitedSpecialCharData, "|", true );
    }

  public void runQuotedText( String path, String inputData, String delimiter, boolean useAll ) throws IOException
    {
    Object[][] results = new Object[][]{
      {"foo", "bar", "baz", "bin", 1L},
      {"foo", "bar", "baz", "bin", 2L},
      {"foo", "bar" + delimiter + "bar", "baz", "bin", 3L},
      {"foo", "bar\"" + delimiter + "bar", "baz", "bin", 4L},
      {"foo", "bar\"\"" + delimiter + "bar", "baz", "bin", 5L},
      {null, null, "baz", null, 6L},
      {null, null, null, null, 7L},
      {"foo", null, null, null, 8L},
      {null, null, null, null, 9L},
      {"f", null, null, null, 10L}, // this one is quoted, single char
      {"f", null, null, ",bin", 11L}
    };

    if( useAll )
      {
      for( int i = 0; i < results.length; i++ )
        {
        Object[] result = results[ i ];

        for( int j = 0; j < result.length; j++ )
          result[ j ] = result[ j ] != null ? result[ j ].toString() : null;
        }
      }

    Tuple[] tuples = new Tuple[ results.length ];

    for( int i = 0; i < results.length; i++ )
      tuples[ i ] = new Tuple( results[ i ] );

    Class[] types = new Class[]{String.class, String.class, String.class, String.class, long.class};
    Fields fields = new Fields( "first", "second", "third", "fourth", "fifth" );

    if( useAll )
      {
      types = null;
      fields = Fields.ALL;
      }

    Tap input = getPlatform().getDelimitedFile( fields, false, delimiter, "\"", types, inputData, SinkMode.KEEP );
    Tap output = getPlatform().getDelimitedFile( fields, false, delimiter, "\"", types, getOutputPath( "quoted/" + path + "" + useAll ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "pipe" );

    Flow flow = getPlatform().getFlowConnector().connect( input, output, pipe );

    flow.complete();

    validateLength( flow, results.length, 5 );

    // validate input parsing compares to expected, and results compare to expected
    TupleEntryIterator iterator = flow.openSource();

    int count = 0;
    while( iterator.hasNext() )
      {
      Tuple tuple = iterator.next().getTuple();
      assertEquals( tuples[ count++ ], tuple );
      }

    iterator = flow.openSink();

    count = 0;
    while( iterator.hasNext() )
      {
      Tuple tuple = iterator.next().getTuple();
      assertEquals( tuples[ count++ ], tuple );
      }
    }

  public void testHeader() throws IOException
    {
    Class[] types = new Class[]{String.class, String.class, String.class, String.class, long.class};
    Fields fields = new Fields( "first", "second", "third", "fourth", "fifth" );

    Tap input = getPlatform().getDelimitedFile( fields, true, ",", "\"", types, testDelimited, SinkMode.KEEP );
    Tap output = getPlatform().getDelimitedFile( fields, false, ",", "\"", types, getOutputPath( "header" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "pipe" );

    Flow flow = getPlatform().getFlowConnector().connect( input, output, pipe );

    flow.complete();

    validateLength( flow, 10, 5 );
    }
  }
