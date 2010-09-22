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

package cascading.scheme;

import java.io.IOException;
import java.util.Properties;

import cascading.CascadingTestCase;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.pipe.Pipe;
import cascading.tap.Hfs;
import cascading.tap.SinkMode;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryIterator;

/**
 *
 */
public class TextDelimitedTest extends CascadingTestCase
  {
  String testData = "build/test/data/delimited.txt";
  String testSpecialCharData = "build/test/data/delimited-spec-char.txt";

  String outputPath = "build/test/output/delim";


  public TextDelimitedTest()
    {
    super( "delimited text tests" );
    }

  public void testQuotedText() throws IOException
    {
    runQuotedText( "normchar", testData, "," );
    }

  public void testQuotedTextSpecChar() throws IOException
    {
    runQuotedText( "specchar", testSpecialCharData, "|" );
    }

  public void runQuotedText( String path, String inputData, String delimiter ) throws IOException
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
      {"f", null, null, null, 10L} // this one is quoted, single char
    };

    Tuple[] tuples = new Tuple[results.length];

    for( int i = 0; i < results.length; i++ )
      tuples[ i ] = new Tuple( results[ i ] );

    Properties properties = new Properties();

    Class[] types = new Class[]{String.class, String.class, String.class, String.class, long.class};
    Fields fields = new Fields( "first", "second", "third", "fourth", "fifth" );
    TextDelimited scheme = new TextDelimited( fields, delimiter, "\"", types );

    Hfs input = new Hfs( scheme, inputData );
    Hfs output = new Hfs( scheme, outputPath + "/quoted/" + path, SinkMode.REPLACE );

    Pipe pipe = new Pipe( "pipe" );

    Flow flow = new FlowConnector( properties ).connect( input, output, pipe );

    flow.complete();

    validateLength( flow, 10, 5 );

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
    Properties properties = new Properties();

    Class[] types = new Class[]{String.class, String.class, String.class, String.class, long.class};
    Fields fields = new Fields( "first", "second", "third", "fourth", "fifth" );

    Hfs input = new Hfs( new TextDelimited( fields, true, ",", "\"", types ), testData );
    Hfs output = new Hfs( new TextDelimited( fields, ",", "\"", types ), outputPath + "/header", SinkMode.REPLACE );

    Pipe pipe = new Pipe( "pipe" );

    Flow flow = new FlowConnector( properties ).connect( input, output, pipe );

    flow.complete();

    validateLength( flow, 9, 5 );
    }
  }
