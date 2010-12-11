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

package cascading;

import java.io.File;
import java.io.IOException;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.Debug;
import cascading.operation.Identity;
import cascading.operation.regex.RegexFilter;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryIterator;
import org.apache.hadoop.mapred.JobConf;

public class RegressionMiscTest extends CascadingTestCase
  {
  String inputFileNums10 = "build/test/data/nums.10.txt";

  String outputPath = "build/test/output/regressionmisc/";

  public RegressionMiscTest()
    {
    super( "regression misc" );
    }

  /**
   * sanity check to make sure writeDOT still works
   *
   * @throws Exception
   */
  public void testWriteDot() throws Exception
    {
    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), "/input" );
    Tap sink = new Hfs( new TextLine(), outputPath + "/unknown", true );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexSplitter( Fields.UNKNOWN ) );

    pipe = new Each( pipe, new Debug() );

    pipe = new Each( pipe, new Fields( 2 ), new Identity( new Fields( "label" ) ) );

    pipe = new Each( pipe, new Debug() );

    pipe = new Each( pipe, new Fields( "label" ), new RegexFilter( "[A-Z]*" ) );

    pipe = new Each( pipe, new Debug() );

    Flow flow = new FlowConnector().connect( source, sink, pipe );

    new File( outputPath ).mkdirs();

    flow.writeDOT( outputPath + "/writedot.dot" );
    }

  /**
   * verifies sink fields are consulted during planning
   *
   * @throws IOException
   */
  public void testSinkDeclaredFieldsFails() throws IOException
    {
    Tap source = new Hfs( new TextLine( new Fields( "line" ) ), "/input" );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new RegexSplitter( new Fields( "first", "second", "third" ), "\\s" ), Fields.ALL );

    Tap sink = new Hfs( new TextLine( new Fields( "line" ), new Fields( "first", "second", "fifth" ) ), "output", true );

    try
      {
      Flow flow = new FlowConnector().connect( source, sink, pipe );
      fail( "did not fail on bad sink field names" );
      }
    catch( Exception exception )
      {
      // ignore
      }
    }

  public void testTupleEntryNextTwice() throws IOException
    {
    Tap tap = new Hfs( new TextLine(), inputFileNums10 );

    TupleEntryIterator iterator = tap.openForRead( new JobConf() );

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