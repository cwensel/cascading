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
import cascading.test.HadoopPlatform;
import cascading.test.LocalPlatform;
import cascading.test.PlatformRunner;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryIterator;
import org.junit.Test;

import static data.InputData.inputFileNums10;

@PlatformRunner.Platform({LocalPlatform.class, HadoopPlatform.class})
public class RegressionMiscPlatformTest extends PlatformTestCase
  {
  public RegressionMiscPlatformTest()
    {
    }

  /**
   * sanity check to make sure writeDOT still works
   *
   * @throws Exception
   */
  @Test
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
  @Test
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

  @Test
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