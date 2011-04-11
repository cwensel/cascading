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

package cascading;

import java.io.File;
import java.io.IOException;

import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.Debug;
import cascading.operation.Identity;
import cascading.operation.regex.RegexFilter;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.test.PlatformTest;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryIterator;

import static data.InputData.inputFileNums10;

@PlatformTest(platforms = {"local", "hadoop"})
public class RegressionMiscTest extends PlatformTestCase
  {
  public RegressionMiscTest()
    {
    }

  /**
   * sanity check to make sure writeDOT still works
   *
   * @throws Exception
   */
  public void testWriteDot() throws Exception
    {
    Tap source = getPlatform().getTextFile( "input" );
    Tap sink = getPlatform().getTextFile( "unknown" );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexSplitter( Fields.UNKNOWN ) );

    pipe = new Each( pipe, new Debug() );

    pipe = new Each( pipe, new Fields( 2 ), new Identity( new Fields( "label" ) ) );

    pipe = new Each( pipe, new Debug() );

    pipe = new Each( pipe, new Fields( "label" ), new RegexFilter( "[A-Z]*" ) );

    pipe = new Each( pipe, new Debug() );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    String outputPath = getOutputPath( "writedot.dot" );

    flow.writeDOT( outputPath );

    assertTrue( new File( outputPath ).exists() );
    }

  /**
   * verifies sink fields are consulted during planning
   *
   * @throws IOException
   */
  public void testSinkDeclaredFieldsFails() throws IOException
    {
    Tap source = getPlatform().getTextFile( new Fields( "line" ), "input" );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new RegexSplitter( new Fields( "first", "second", "third" ), "\\s" ), Fields.ALL );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), new Fields( "first", "second", "fifth" ), getOutputPath( "output" ), SinkMode.REPLACE );

    try
      {
      new HadoopFlowConnector().connect( source, sink, pipe );
      fail( "did not fail on bad sink field names" );
      }
    catch( Exception exception )
      {
      // test passed
      }
    }

  public void testTupleEntryNextTwice() throws IOException
    {
    Tap tap = getPlatform().getTextFile( inputFileNums10 );

    TupleEntryIterator iterator = tap.openForRead( getPlatform().getFlowProcess() );

    int count = 0;
    while( iterator.hasNext() )
      {
      iterator.next();
      count++;
      }

    assertFalse( iterator.hasNext() );
    assertEquals( 10, count );
    }
  }