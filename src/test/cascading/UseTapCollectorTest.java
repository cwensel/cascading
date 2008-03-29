/*
 * Copyright (c) 2008, Your Corporation. All Rights Reserved.
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
    super( "fielded pipes", false );
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
  }