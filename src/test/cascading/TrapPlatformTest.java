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

import java.io.IOException;
import java.util.Map;

import cascading.cascade.Cascades;
import cascading.flow.Flow;
import cascading.operation.AssertionLevel;
import cascading.operation.aggregator.Count;
import cascading.operation.assertion.AssertNotEquals;
import cascading.operation.regex.RegexParser;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.test.HadoopPlatform;
import cascading.test.LocalPlatform;
import cascading.test.PlatformRunner;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import org.junit.Test;

import static data.InputData.inputFileApache;

/**
 *
 */
@PlatformRunner.Platform({LocalPlatform.class, HadoopPlatform.class})
public class TrapPlatformTest extends PlatformTestCase
  {
  public TrapPlatformTest()
    {
    super( true, 4, 4 );
    }

  @Test
  public void testTrapNone() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( inputFileApache );

    Pipe pipe = new Pipe( "map" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );
    pipe = new GroupBy( "reduce", pipe, new Fields( "ip" ) );
    pipe = new Every( pipe, new Count(), new Fields( "ip", "count" ) );

    Tap sink = getPlatform().getTextFile( getOutputPath( "none/tap" ), SinkMode.REPLACE );
    Tap trap = getPlatform().getTextFile( getOutputPath( "none/trap" ), SinkMode.REPLACE );

    Flow flow = getPlatform().getFlowConnector().connect( "trap test", source, sink, trap, pipe );

    flow.complete();

    validateLength( flow, 8, null );
    validateLength( flow.openTrap(), 0 );
    }

  @Test
  public void testTrapEachAll() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( inputFileApache );

    Pipe pipe = new Pipe( "map" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    // always fail
    pipe = new Each( pipe, new Fields( "ip" ), new TestFunction( new Fields( "test" ), null ), Fields.ALL );

    pipe = new GroupBy( "reduce", pipe, new Fields( "ip" ) );
    pipe = new Every( pipe, new Count(), new Fields( "ip", "count" ) );

    Tap sink = getPlatform().getTextFile( getOutputPath( "all/tap" ), SinkMode.REPLACE );
    Tap trap = getPlatform().getTextFile( getOutputPath( "all/trap" ), SinkMode.REPLACE );

    Flow flow = getPlatform().getFlowConnector().connect( "trap test", source, sink, trap, pipe );

    flow.complete();

    validateLength( flow, 0, null );
    validateLength( flow.openTrap(), 10 );
    }

  @Test
  public void testTrapEachAllSequence() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( inputFileApache );

    Pipe pipe = new Pipe( "map" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    // always fail
    pipe = new Each( pipe, new Fields( "ip" ), new TestFunction( new Fields( "test" ), null ), Fields.ALL );

    pipe = new GroupBy( "reduce", pipe, new Fields( "ip" ) );
    pipe = new Every( pipe, new Count(), new Fields( "ip", "count" ) );

    Tap sink = getPlatform().getDelimitedFile( Fields.ALL, getOutputPath( "allseq/tap" ), SinkMode.REPLACE );
    Tap trap = getPlatform().getDelimitedFile( Fields.ALL, getOutputPath( "allseq/trap" ), SinkMode.REPLACE );

    Flow flow = getPlatform().getFlowConnector().connect( "trap test", source, sink, trap, pipe );

    flow.complete();

    validateLength( flow, 0, null );
    validateLength( flow.openTrap(), 10 );
    }

  @Test
  public void testTrapEveryAllAtStart() throws Exception
    {
    runTrapEveryAll( 0, "everystart", 8 );
    }

  @Test
  public void testTrapEveryAllAtAggregate() throws Exception
    {
    runTrapEveryAll( 1, "everyaggregate", 10 ); // fails at all values
    }

  @Test
  public void testTrapEveryAllAtComplete() throws Exception
    {
    runTrapEveryAll( 2, "everycomplete", 8 );
    }

  private void runTrapEveryAll( int failAt, String path, int failSize ) throws IOException
    {
    getPlatform().copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( inputFileApache );

    Pipe pipe = new Pipe( "map" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    pipe = new GroupBy( "reduce", pipe, new Fields( "ip" ) );
    pipe = new Every( pipe, new Count(), new Fields( "ip", "count" ) );
    pipe = new Every( pipe, new TestFailAggregator( new Fields( "fail" ), failAt ), new Fields( "ip", "count" ) );

    Tap sink = getPlatform().getTextFile( getOutputPath( path + "/tap" ), SinkMode.REPLACE );
    Tap trap = getPlatform().getTextFile( getOutputPath( path + "/trap" ), SinkMode.REPLACE );

    Map<String, Tap> traps = Cascades.tapsMap( "reduce", trap );

    Flow flow = getPlatform().getFlowConnector().connect( "trap test", source, sink, traps, pipe );

    flow.complete();

    validateLength( flow, 0, null );
    validateLength( flow.openTrap(), failSize );
    }

  /**
   * verify we can fail in randome places into the same trap
   *
   * @throws Exception
   */
  @Test
  public void testTrapEachAllChained() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( inputFileApache );

    Pipe pipe = new Pipe( "map" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    // always fail
    pipe = new Each( pipe, new TestFunction( new Fields( "test" ), new Tuple( 1 ), 1 ), Fields.ALL );
    pipe = new Each( pipe, new TestFunction( new Fields( "test2" ), new Tuple( 2 ), 2 ), Fields.ALL );
    pipe = new Each( pipe, new TestFunction( new Fields( "test3" ), new Tuple( 3 ), 3 ), Fields.ALL );
    pipe = new Each( pipe, new TestFunction( new Fields( "test4" ), new Tuple( 4 ), 4 ), Fields.ALL );

    Tap sink = getPlatform().getTextFile( getOutputPath( "allchain/tap" ), SinkMode.REPLACE );
    Tap trap = getPlatform().getTextFile( getOutputPath( "allchain/trap" ), SinkMode.REPLACE );

    Flow flow = getPlatform().getFlowConnector().connect( "trap test", source, sink, trap, pipe );

    flow.complete();

    validateLength( flow, 6, null );
    validateLength( flow.openTrap(), 4 );
    }


  /**
   * This test verifies traps can cross m/r and step boundaries.
   *
   * @throws Exception
   */
  @Test
  public void testTrapEachEveryAllChained() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( inputFileApache );

    Pipe pipe = new Pipe( "map" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    // always fail
    pipe = new Each( pipe, AssertionLevel.VALID, new AssertNotEquals( "75.185.76.245" ) );
    pipe = new GroupBy( pipe, new Fields( "ip" ) );
    pipe = new Each( pipe, AssertionLevel.VALID, new AssertNotEquals( "68.46.103.112" ) );
    pipe = new GroupBy( pipe, new Fields( "ip" ) );
    pipe = new Each( pipe, AssertionLevel.VALID, new AssertNotEquals( "76.197.151.0" ) );
    pipe = new Each( pipe, AssertionLevel.VALID, new AssertNotEquals( "12.215.138.88" ) );

    Tap sink = getPlatform().getTextFile( getOutputPath( "eacheverychain/tap" ), SinkMode.REPLACE );
    Tap trap = getPlatform().getTextFile( getOutputPath( "eacheverychain/trap" ), SinkMode.REPLACE );

    Flow flow = getPlatform().getFlowConnector().connect( "trap test", source, sink, trap, pipe );

    flow.complete();

    validateLength( flow, 6, null );
    validateLength( flow.openTrap(), 4 );
    }

  @Test
  public void testTrapToSequenceFile() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( inputFileApache );

    Pipe pipe = new Pipe( "map" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    // always fail
    pipe = new Each( pipe, new Fields( "ip" ), new TestFunction( new Fields( "test" ), null ), Fields.ALL );

    pipe = new GroupBy( "reduce", pipe, new Fields( "ip" ) );
    pipe = new Every( pipe, new Count(), new Fields( "ip", "count" ) );

    Tap sink = getPlatform().getTextFile( getOutputPath( "seq/tap" ), SinkMode.REPLACE );
    Tap trap = getPlatform().getDelimitedFile( new Fields( "ip" ), getOutputPath( "seq/trap" ), SinkMode.REPLACE );

    Flow flow = getPlatform().getFlowConnector().connect( "trap test", source, sink, trap, pipe );

    flow.complete();

    validateLength( flow, 0, null );
    validateLength( flow.openTrap(), 10 );
    }

//  private static class FailScheme extends TextLine
//    {
//    boolean sourceFired = false;
//    boolean sinkFired = false;
//
//    public FailScheme()
//      {
//      }
//
//    public FailScheme( Fields sourceFields )
//      {
//      super( sourceFields );
//      }
//
//    @Override
//    public boolean source( HadoopFlowProcess flowProcess, SourceCall sourceCall ) throws IOException
//      {
//      if( !sourceFired )
//        {
//        sourceFired = true;
//        throw new TapException( "fail" );
//        }
//
//      return super.source( flowProcess, sourceCall );
//      }
//
//    @Override
//    public void sink( HadoopFlowProcess flowProcess, SinkCall<Object[], OutputCollector> sinkCall ) throws IOException
//      {
//      if( !sinkFired )
//        {
//        sinkFired = true;
//        throw new TapException( "fail" );
//        }
//
//      super.sink( flowProcess, sinkCall );
//      }
//    }

/**  public void testTrapTapSourceSink() throws Exception
 {
 if( !new File( inputFileApache ).exists() )
 fail( "data file not found" );

 copyFromLocal( inputFileApache );

 Tap source = new Hfs( new FailScheme( new Fields( "offset", "line" ) ), inputFileApache );

 Pipe pipe = new Pipe( "map" );

 pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );
 pipe = new GroupBy( pipe, new Fields( "ip" ) );
 pipe = new Every( pipe, new Count(), new Fields( "ip", "count" ) );

 Tap sink = new Hfs( new FailScheme(), outputPath + "sink/tap", true );
 Tap trap = new Hfs( new TextLine(), outputPath + "sink/trap", true );

 Map<Object, Object> properties = getProperties();

 // compensate for running in cluster mode
 properties.put( "mapred.map.tasks", 1 );
 properties.put( "mapred.reduce.tasks", 1 );

 Flow flow = new HadoopFlowConnector( properties ).connect( "trap test", source, sink, trap, pipe );

 flow.complete();

 validateLength( flow.openTapForRead( new Hfs( new TextLine(), outputPath + "sink/tap", true ) ), 6, null );
 validateLength( flow.openTrap(), 2 );
 } **/
  }
