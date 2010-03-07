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
import java.util.Map;

import cascading.cascade.Cascades;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.AssertionLevel;
import cascading.operation.aggregator.Count;
import cascading.operation.assertion.AssertNotEquals;
import cascading.operation.regex.RegexParser;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.SequenceFile;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.apache.hadoop.mapred.OutputCollector;

/**
 *
 */
public class TrapTest extends ClusterTestCase
  {
  String inputFileApache = "build/test/data/apache.10.txt";
  String outputPath = "build/test/output/traps/";

  public TrapTest()
    {
    super( "trap tests", true, 4, 4 );
    }

  public void testTrapNone() throws Exception
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileApache );

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileApache );

    Pipe pipe = new Pipe( "map" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );
    pipe = new GroupBy( "reduce", pipe, new Fields( "ip" ) );
    pipe = new Every( pipe, new Count(), new Fields( "ip", "count" ) );

    Tap sink = new Hfs( new TextLine(), outputPath + "none/tap", true );
    Tap trap = new Hfs( new TextLine(), outputPath + "none/trap", true );

    Flow flow = new FlowConnector( getProperties() ).connect( "trap test", source, sink, trap, pipe );

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

    Pipe pipe = new Pipe( "map" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    // always fail
    pipe = new Each( pipe, new Fields( "ip" ), new TestFunction( new Fields( "test" ), null ), Fields.ALL );

    pipe = new GroupBy( "reduce", pipe, new Fields( "ip" ) );
    pipe = new Every( pipe, new Count(), new Fields( "ip", "count" ) );

    Tap sink = new Hfs( new TextLine(), outputPath + "all/tap", true );
    Tap trap = new Hfs( new TextLine(), outputPath + "all/trap", true );

    Flow flow = new FlowConnector( getProperties() ).connect( "trap test", source, sink, trap, pipe );

    flow.complete();

    validateLength( flow, 0, null );
    validateLength( flow.openTrap(), 10 );
    }

  public void testTrapEachAllSequence() throws Exception
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileApache );

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileApache );

    Pipe pipe = new Pipe( "map" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    // always fail
    pipe = new Each( pipe, new Fields( "ip" ), new TestFunction( new Fields( "test" ), null ), Fields.ALL );

    pipe = new GroupBy( "reduce", pipe, new Fields( "ip" ) );
    pipe = new Every( pipe, new Count(), new Fields( "ip", "count" ) );

    Tap sink = new Hfs( new SequenceFile( Fields.ALL ), outputPath + "allseq/tap", true );
    Tap trap = new Hfs( new SequenceFile( Fields.ALL ), outputPath + "allseq/trap", true );

    Flow flow = new FlowConnector( getProperties() ).connect( "trap test", source, sink, trap, pipe );

//    flow.writeDOT( "traps.dot" );

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

    Pipe pipe = new Pipe( "map" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    pipe = new GroupBy( "reduce", pipe, new Fields( "ip" ) );
    pipe = new Every( pipe, new Count(), new Fields( "ip", "count" ) );
    pipe = new Every( pipe, new TestFailAggregator( new Fields( "fail" ), failAt ), new Fields( "ip", "count" ) );

    Tap sink = new Hfs( new TextLine(), outputPath + path + "/tap", true );
    Tap trap = new Hfs( new TextLine(), outputPath + path + "/trap", true );

    Map<String, Tap> traps = Cascades.tapsMap( "reduce", trap );

    Flow flow = new FlowConnector( getProperties() ).connect( "trap test", source, sink, traps, pipe );

    flow.complete();

    validateLength( flow, 0, null );
    validateLength( flow.openTrap(), failSize );
    }

  /**
   * verify we can fail in randome places into the same trap
   *
   * @throws Exception
   */
  public void testTrapEachAllChained() throws Exception
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileApache );

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileApache );

    Pipe pipe = new Pipe( "map" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    // always fail
    pipe = new Each( pipe, new TestFunction( new Fields( "test" ), new Tuple( 1 ), 1 ), Fields.ALL );
    pipe = new Each( pipe, new TestFunction( new Fields( "test2" ), new Tuple( 2 ), 2 ), Fields.ALL );
    pipe = new Each( pipe, new TestFunction( new Fields( "test3" ), new Tuple( 3 ), 3 ), Fields.ALL );
    pipe = new Each( pipe, new TestFunction( new Fields( "test4" ), new Tuple( 4 ), 4 ), Fields.ALL );

    Tap sink = new Hfs( new TextLine(), outputPath + "allchain/tap", true );
    Tap trap = new Hfs( new TextLine(), outputPath + "allchain/trap", true );

    Flow flow = new FlowConnector( getProperties() ).connect( "trap test", source, sink, trap, pipe );

//    flow.writeDOT( "traps.dot" );

    flow.complete();

    validateLength( flow, 6, null );
    validateLength( flow.openTrap(), 4 );
    }


  /**
   * This test verifies traps can cross m/r and step boundaries.
   *
   * @throws Exception
   */
  public void testTrapEachEveryAllChained() throws Exception
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileApache );

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileApache );

    Pipe pipe = new Pipe( "map" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    // always fail
    pipe = new Each( pipe, AssertionLevel.VALID, new AssertNotEquals( "75.185.76.245" ) );
    pipe = new GroupBy( pipe, new Fields( "ip" ) );
    pipe = new Each( pipe, AssertionLevel.VALID, new AssertNotEquals( "68.46.103.112" ) );
    pipe = new GroupBy( pipe, new Fields( "ip" ) );
    pipe = new Each( pipe, AssertionLevel.VALID, new AssertNotEquals( "76.197.151.0" ) );
    pipe = new Each( pipe, AssertionLevel.VALID, new AssertNotEquals( "12.215.138.88" ) );

    Tap sink = new Hfs( new TextLine(), outputPath + "eacheverychain/tap", true );
    Tap trap = new Hfs( new TextLine(), outputPath + "eacheverychain/trap", true );

    Flow flow = new FlowConnector( getProperties() ).connect( "trap test", source, sink, trap, pipe );

//    flow.writeDOT( "traps.dot" );

    flow.complete();

    validateLength( flow, 6, null );
    validateLength( flow.openTrap(), 4 );
    }

  public void testTrapToSequenceFile() throws Exception
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileApache );

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileApache );

    Pipe pipe = new Pipe( "map" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    // always fail
    pipe = new Each( pipe, new Fields( "ip" ), new TestFunction( new Fields( "test" ), null ), Fields.ALL );

    pipe = new GroupBy( "reduce", pipe, new Fields( "ip" ) );
    pipe = new Every( pipe, new Count(), new Fields( "ip", "count" ) );

    Tap sink = new Hfs( new TextLine(), outputPath + "seq/tap", true );
    Tap trap = new Hfs( new SequenceFile( new Fields( "ip" ) ), outputPath + "seq/trap", true );

    Flow flow = new FlowConnector( getProperties() ).connect( "trap test", source, sink, trap, pipe );

    flow.complete();

    validateLength( flow, 0, null );
    validateLength( flow.openTrap(), 10 );
    }

  private static class FailScheme extends TextLine
    {
    boolean sourceFired = false;
    boolean sinkFired = false;

    public FailScheme()
      {
      }

    public FailScheme( Fields sourceFields )
      {
      super( sourceFields );
      }

    @Override
    public Tuple source( Object key, Object value )
      {
      if( !sourceFired )
        {
        sourceFired = true;
        throw new TapException( "fail" );
        }

      return super.source( key, value );
      }

    @Override
    public void sink( TupleEntry tupleEntry, OutputCollector outputCollector ) throws IOException
      {
      if( !sinkFired )
        {
        sinkFired = true;
        throw new TapException( "fail" );
        }

      super.sink( tupleEntry, outputCollector );
      }
    }

  public void testTrapTapSource() throws Exception
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileApache );

    Tap source = new Hfs( new FailScheme( new Fields( "offset", "line" ) ), inputFileApache );

    Pipe pipe = new Pipe( "map" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );
    pipe = new GroupBy( "reduce", pipe, new Fields( "ip" ) );
    pipe = new Every( pipe, new Count(), new Fields( "ip", "count" ) );

    Tap sink = new Hfs( new TextLine(), outputPath + "source/tap", true );
    Tap trap = new Hfs( new TextLine(), outputPath + "source/trap", true );

    Flow flow = new FlowConnector( getProperties() ).connect( "trap test", source, sink, trap, pipe );

    flow.complete();

    validateLength( flow, 7, null );
    validateLength( flow.openTrap(), 1 );
    }

  public void testTrapTapSink() throws Exception
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileApache );

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileApache );

    Pipe pipe = new Pipe( "map" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );
    pipe = new GroupBy( pipe, new Fields( "ip" ) );
    pipe = new Every( pipe, new Count(), new Fields( "ip", "count" ) );

    Tap sink = new Hfs( new FailScheme(), outputPath + "sink/tap", true );
    Tap trap = new Hfs( new TextLine(), outputPath + "sink/trap", true );

    Flow flow = new FlowConnector( getProperties() ).connect( "trap test", source, sink, trap, pipe );

    flow.complete();

    validateLength( flow.openTapForRead( new Hfs( new TextLine(), outputPath + "sink/tap", true ) ), 7, null );
    validateLength( flow.openTrap(), 1 );
    }
  }
