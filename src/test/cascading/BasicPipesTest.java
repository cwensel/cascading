/*
 * Copyright (c) 2007-2009 Concurrent, Inc. All Rights Reserved.
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

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.Aggregator;
import cascading.operation.ConcreteCall;
import cascading.operation.Filter;
import cascading.operation.Function;
import cascading.operation.aggregator.Count;
import cascading.operation.function.UnGroup;
import cascading.operation.regex.RegexFilter;
import cascading.operation.regex.RegexParser;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;
import cascading.tuple.TupleListCollector;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * These tests execute basic function using field positions, not names. so there will be duplicates with
 * FieldedPipestest
 */
public class BasicPipesTest extends CascadingTestCase
  {
  String inputFileApache = "build/test/data/apache.10.txt";
  String inputFileIps = "build/test/data/ips.20.txt";
  String inputFileNums = "build/test/data/nums.20.txt";

  String inputFileUpper = "build/test/data/upper.txt";
  String inputFileLower = "build/test/data/lower.txt";
  String inputFileJoined = "build/test/data/lower+upper.txt";

  String outputPath = "build/test/output/results";

  public BasicPipesTest()
    {
    super( "build pipes" );
    }

  /**
   * Test the count aggregator function
   *
   * @throws IOException
   */
  public void testCount() throws Exception
    {
    if( !new File( inputFileIps ).exists() )
      fail( "data file not found" );

    Tap source = new Hfs( new TextLine(), inputFileIps );
    Tap sink = new Hfs( new TextLine(), outputPath + "/count", true );

    Pipe pipe = new Pipe( "count" );
    pipe = new GroupBy( pipe, new Fields( 1 ) );
    pipe = new Every( pipe, new Fields( 1 ), new Count(), new Fields( 0, 1 ) );

    Flow flow = new FlowConnector().connect( source, sink, pipe );

    flow.start();
    flow.complete();

    TupleEntryIterator iterator = flow.openSink();
    Function splitter = new RegexSplitter( Fields.size( 2 ) );

    boolean found = false;

    while( iterator.hasNext() )
      {
      Tuple tuple = iterator.next().getTuple();

//      System.out.println( "tuple = " + tuple );

      TupleListCollector tupleEntryCollector = new TupleListCollector( Fields.size( 2 ) );
      Tuple tuple1 = tuple.get( new int[]{1} );
      ConcreteCall operationCall = new ConcreteCall( new TupleEntry( tuple1 ), tupleEntryCollector );
      splitter.operate( null, operationCall );

      Tuple tupleEntry = tupleEntryCollector.iterator().next();

      if( tupleEntry.get( 0 ).equals( "63.123.238.8" ) )
        {
        found = true;
        assertEquals( "wrong count", "2", tupleEntry.get( 1 ) );
        }
      }

    iterator.close();

    if( !found )
      fail( "never found ip" );

    validateLength( flow, 17 );
    }

  /**
   * A slightly more complex pipe
   *
   * @throws IOException
   */
  public void testSimple() throws Exception
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    Tap source = new Hfs( new TextLine(), inputFileApache );
    Tap sink = new Hfs( new TextLine(), outputPath + "/simple", true );

    Pipe pipe = new Pipe( "test" );

    Function parser = new RegexParser( "^[^ ]*" );

    pipe = new Each( pipe, new Fields( 1 ), parser, new Fields( 2 ) );

    pipe = new GroupBy( pipe, new Fields( 0 ) );

    Aggregator counter = new Count();

    pipe = new Every( pipe, new Fields( 0 ), counter, new Fields( 0, 1 ) );

    Flow flow = new FlowConnector().connect( source, sink, pipe );

//    flow.writeDOT( "simple.dot" );

    flow.complete();

    validateLength( flow, 8 );
    }

  public void testSimpleRelative() throws Exception
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    Tap source = new Hfs( new TextLine(), inputFileApache );
    Tap sink = new Hfs( new TextLine(), outputPath + "/simplerelative", true );

    Pipe pipe = new Pipe( "test" );

    Function parser = new RegexParser( "^[^ ]*" );

    pipe = new Each( pipe, new Fields( -1 ), parser, new Fields( -1 ) );

    pipe = new GroupBy( pipe, new Fields( 0 ) );

    Aggregator counter = new Count();

    pipe = new Every( pipe, new Fields( 0 ), counter, new Fields( 0, 1 ) );

    Flow flow = new FlowConnector().connect( source, sink, pipe );

//    flow.writeDOT( "simple.dot" );

    flow.complete();

    validateLength( flow, 8 );
    }

  public void testCoGroup() throws Exception
    {
    if( !new File( inputFileLower ).exists() )
      fail( "data file not found" );

    Tap sourceLower = new Hfs( new TextLine(), inputFileLower );
    Tap sourceUpper = new Hfs( new TextLine(), inputFileUpper );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    // using null pos so all fields are written
    Tap sink = new Hfs( new TextLine(), outputPath + "/complex/cogroup/", true );

    Function splitter = new RegexSplitter( Fields.size( 2 ), " " );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( 1 ), splitter, Fields.RESULTS );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( 1 ), splitter, Fields.RESULTS );

    Pipe splice = new CoGroup( pipeLower, new Fields( 0 ), pipeUpper, new Fields( 0 ) );

    Flow countFlow = new FlowConnector().connect( sources, sink, splice );

//    System.out.println( "countFlow =\n" + countFlow );
//    countFlow.writeDOT( "cogroup.dot" );

    countFlow.complete();

    validateLength( countFlow, 5 );

    TupleEntryIterator iterator = countFlow.openSink();

    assertEquals( "not equal: tuple.get(1)", "1\ta\t1\tA", iterator.next().get( 1 ) );
    assertEquals( "not equal: tuple.get(1)", "2\tb\t2\tB", iterator.next().get( 1 ) );

    iterator.close();
    }

  public void testUnGroup() throws Exception
    {
    if( !new File( inputFileJoined ).exists() )
      fail( "data file not found" );

    Tap source = new Hfs( new TextLine(), inputFileJoined );
    Tap sink = new Hfs( new TextLine(), outputPath + "/ungrouped", true );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( 1 ), new RegexSplitter( Fields.size( 3 ) ) );

    pipe = new Each( pipe, new UnGroup( Fields.size( 2 ), new Fields( 0 ), Fields.fields( new Fields( 1 ), new Fields( 2 ) ) ) );

    Flow flow = new FlowConnector().connect( source, sink, pipe );

//    flow.writeDOT( "ungroup.dot" );

    flow.complete();

    validateLength( flow, 10 );
    }

  public void testFilterAll() throws Exception
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    Tap source = new Hfs( new TextLine(), inputFileApache );
    Tap sink = new Hfs( new TextLine(), outputPath + "/filterall", true );

    Pipe pipe = new Pipe( "test" );

    Filter filter = new RegexFilter( ".*", true );

    pipe = new Each( pipe, new Fields( 1 ), filter );

    Flow flow = new FlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 0 );
    }

  public void testFilter() throws Exception
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    Tap source = new Hfs( new TextLine(), inputFileApache );
    Tap sink = new Hfs( new TextLine(), outputPath + "/filter", true );

    Pipe pipe = new Pipe( "test" );

    Filter filter = new RegexFilter( "^68.*" );

    pipe = new Each( pipe, new Fields( 1 ), filter );

    Flow flow = new FlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 3 );
    }

  public void testSimpleChain() throws Exception
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    Tap source = new Hfs( new TextLine(), inputFileApache );
    Tap sink = new Hfs( new TextLine(), outputPath + "/simple", true );

    Pipe pipe = new Pipe( "test" );

    Function parser = new RegexParser( "^[^ ]*" );

    pipe = new Each( pipe, new Fields( 1 ), parser, new Fields( 2 ) );

    pipe = new GroupBy( pipe, new Fields( 0 ) );

    pipe = new Every( pipe, new Fields( 0 ), new Count(), new Fields( 0, 1 ) );

    // add a second group to force a new map/red
    pipe = new GroupBy( pipe, new Fields( 0 ) );

    Flow flow = new FlowConnector().connect( source, sink, pipe );

//    flow.writeDOT( "simplechain.dot" );

    flow.complete();

    validateLength( flow, 8 );
    }

  public void testReplace() throws Exception
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    Tap source = new Hfs( new TextLine(), inputFileApache );
    Tap sink = new Hfs( new TextLine(), outputPath + "/replace", true );

    Pipe pipe = new Pipe( "test" );

    Function parser = new RegexParser( Fields.ARGS, "^[^ ]*" );
    pipe = new Each( pipe, new Fields( 1 ), parser, Fields.REPLACE );

    Flow flow = new FlowConnector().connect( source, sink, pipe );

//    flow.writeDOT( "simple.dot" );

    flow.complete();

    validateLength( flow, 10, 2, Pattern.compile( "\\d*\\s\\d*\\s[\\d]{1,3}\\.[\\d]{1,3}\\.[\\d]{1,3}\\.[\\d]{1,3}" ) );
    }

  }
