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

package cascading;

import java.io.File;
import java.io.IOException;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexParser;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.Group;
import cascading.pipe.Pipe;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/**
 *
 */
public class TrapTest extends ClusterTestCase
  {
  String inputFileApache = "build/test/data/apache.10.txt";
  String outputPath = "build/test/output/traps/";

  public TrapTest()
    {
    super( "trap tests", false );
    }

  public void testTrapNone() throws Exception
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileApache );

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileApache );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );
    pipe = new Group( pipe, new Fields( "ip" ) );
    pipe = new Every( pipe, new Count(), new Fields( "ip", "count" ) );

    Tap sink = new Hfs( new TextLine(), outputPath + "none/tap", true );
    Tap trap = new Hfs( new TextLine(), outputPath + "none/trap", true );

    Flow flow = new FlowConnector( jobConf ).connect( "trap test", source, sink, trap, pipe );

    flow.complete();

    validateLength( flow, 8, null );
    validateLength( flow.openTrap(), 0 );
    }

  public void testTrapEachAll() throws Exception
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileApache );

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileApache );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    // always fail
    pipe = new Each( pipe, new Fields( "ip" ), new TestFunction( new Fields( "test" ), null ), Fields.ALL );

    pipe = new Group( pipe, new Fields( "ip" ) );
    pipe = new Every( pipe, new Count(), new Fields( "ip", "count" ) );

    Tap sink = new Hfs( new TextLine(), outputPath + "all/tap", true );
    Tap trap = new Hfs( new TextLine(), outputPath + "all/trap", true );

    Flow flow = new FlowConnector( jobConf ).connect( "trap test", source, sink, trap, pipe );

    flow.complete();

    validateLength( flow, 0, null );
    validateLength( flow.openTrap(), 10 );
    }

  public void testTrapEveryAllAtStart() throws Exception
    {
    runTrapEveryAll( 0, "everystart", 8 );
    }

  public void testTrapEveryAllAtAggregate() throws Exception
    {
    runTrapEveryAll( 1, "everyaggregate", 10 ); // fails at all values
    }

  public void testTrapEveryAllAtComplete() throws Exception
    {
    runTrapEveryAll( 2, "everycomplete", 8 );
    }

  private void runTrapEveryAll( int failAt, String path, int failSize ) throws IOException
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileApache );

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileApache );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    pipe = new Group( pipe, new Fields( "ip" ) );
    pipe = new Every( pipe, new Count(), new Fields( "ip", "count" ) );
    pipe = new Every( pipe, new TestFailAggregator( new Fields( "fail" ), failAt ), new Fields( "ip", "count" ) );

    Tap sink = new Hfs( new TextLine(), outputPath + path + "/tap", true );
    Tap trap = new Hfs( new TextLine(), outputPath + path + "/trap", true );

    Flow flow = new FlowConnector( jobConf ).connect( "trap test", source, sink, trap, pipe );

    flow.complete();

    validateLength( flow, 0, null );
    validateLength( flow.openTrap(), failSize );
    }

  }
