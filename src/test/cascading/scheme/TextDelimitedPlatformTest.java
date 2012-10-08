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

package cascading.scheme;

import java.io.IOException;

import cascading.PlatformTestCase;
import cascading.flow.Flow;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.test.HadoopPlatform;
import cascading.test.LocalPlatform;
import cascading.test.PlatformRunner;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryIterator;
import org.junit.Test;

import static data.InputData.*;

/**
 *
 */
@PlatformRunner.Platform({LocalPlatform.class, HadoopPlatform.class})
public class TextDelimitedPlatformTest extends PlatformTestCase
  {
  public TextDelimitedPlatformTest()
    {
    }

  @Test
  public void testQuotedText() throws IOException
    {
    runQuotedText( "normchar", testDelimited, ",", false );
    }

  @Test
  public void testQuotedTextAll() throws IOException
    {
    runQuotedText( "normchar", testDelimited, ",", true );
    }

  @Test
  public void testQuotedTextSpecChar() throws IOException
    {
    runQuotedText( "specchar", testDelimitedSpecialCharData, "|", false );
    }

  @Test
  public void testQuotedTextSpecCharAll() throws IOException
    {
    runQuotedText( "specchar", testDelimitedSpecialCharData, "|", true );
    }

  private void runQuotedText( String path, String inputData, String delimiter, boolean useAll ) throws IOException
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
      {"f", null, null, ",bin", 11L},
      {"f", null, null, "bin,", 11L}
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

  @Test
  public void testHeader() throws IOException
    {
    Class[] types = new Class[]{String.class, String.class, String.class, String.class, long.class};
    Fields fields = new Fields( "first", "second", "third", "fourth", "fifth" );

    Tap input = getPlatform().getDelimitedFile( fields, true, true, ",", "\"", types, testDelimited, SinkMode.KEEP );
    Tap output = getPlatform().getDelimitedFile( fields, true, true, ",", "\"", types, getOutputPath( "header" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "pipe" );

    Flow flow = getPlatform().getFlowConnector().connect( input, output, pipe );

    flow.complete();

    validateLength( flow, 11, 5 );
    }

  @Test
  public void testHeaderAll() throws IOException
    {
    Fields fields = new Fields( "first", "second", "third", "fourth", "fifth" );

    Tap input = getPlatform().getDelimitedFile( fields, true, true, ",", "\"", null, testDelimited, SinkMode.KEEP );
    Tap output = getPlatform().getDelimitedFile( Fields.ALL, true, true, ",", "\"", null, getOutputPath( "headerall" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "pipe" );

    Flow flow = getPlatform().getFlowConnector().connect( input, output, pipe );

    flow.complete();

    validateLength( flow, 11, 5 );
    }

  @Test
  public void testHeaderFieldsAll() throws IOException
    {
    Tap input = getPlatform().getDelimitedFile( Fields.UNKNOWN, true, true, ",", "\"", null, testDelimitedHeader, SinkMode.KEEP );
    Tap output = getPlatform().getDelimitedFile( Fields.ALL, true, true, ",", "\"", null, getOutputPath( "headerfieldsall" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "pipe" );

    Flow flow = getPlatform().getFlowConnector().connect( input, output, pipe );

    flow.complete();

    validateLength( flow, 11, 5 );

    TupleEntryIterator iterator = flow.openTapForRead( getPlatform().getTextFile( new Fields( "line" ), output.getIdentifier() ) );

    assertEquals( iterator.next().getObject( 0 ), "first,second,third,fourth,fifth" );

    iterator.close();
    }
  }
