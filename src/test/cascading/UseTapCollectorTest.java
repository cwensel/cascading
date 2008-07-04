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

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexParser;
import cascading.pipe.Each;
import cascading.pipe.EndPipe;
import cascading.pipe.Every;
import cascading.pipe.Group;
import cascading.pipe.Pipe;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/** @version $Id: //depot/calku/cascading/src/test/cascading/FieldedPipesTest.java#4 $ */
public class UseTapCollectorTest extends ClusterTestCase
  {
  String inputFileApache = "build/test/data/apache.10.txt";
  String outputPath = "build/test/output/tapcollector/";

  public UseTapCollectorTest()
    {
    super( "use tap collector tests", true );
    }

  public void testViaEndPipe() throws Exception
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileApache );

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileApache );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );
    pipe = new Group( pipe, new Fields( "ip" ) );
    pipe = new Every( pipe, new Count(), new Fields( "ip", "count" ) );

    pipe = new EndPipe( pipe );

    Tap sink = new Hfs( new TextLine(), outputPath + "endpipe", true );

//    sink.setUseTapCollector( true );

    Flow flow = new FlowConnector( jobConf ).connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 8, null );
    }

  public void testViaTap() throws Exception
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileApache );

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileApache );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );
    pipe = new Group( pipe, new Fields( "ip" ) );
    pipe = new Every( pipe, new Count(), new Fields( "ip", "count" ) );

//    pipe = new EndPipe( pipe );

    Tap sink = new Hfs( new TextLine(), outputPath + "tap", true );

    sink.setUseTapCollector( true );

    Flow flow = new FlowConnector( jobConf ).connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 8, null );
    }

  public void testNoGroup() throws Exception
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileApache );

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileApache );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    Tap sink = new Hfs( new TextLine( 1 ), outputPath + "/tapnogroup", true );

    sink.setUseTapCollector( true );

    Flow flow = new FlowConnector( jobConf ).connect( source, sink, pipe );

//    flow.writeDOT( "testnogroup.dot" );

    flow.complete();

    validateLength( flow, 10, null );
    }

  }