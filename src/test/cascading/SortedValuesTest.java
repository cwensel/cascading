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
import java.text.ParseException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.MultiMapReducePlanner;
import cascading.operation.Identity;
import cascading.operation.Insert;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexParser;
import cascading.TestConstants;
import cascading.operation.text.DateParser;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.TextLine;
import cascading.tap.Lfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryIterator;
import org.apache.hadoop.mapred.JobConf;

/** @version $Id: //depot/calku/cascading/src/test/cascading/ArrivalUseCaseTest.java#2 $ */
public class SortedValuesTest extends ClusterTestCase
  {
  String inputFileApache = "build/test/data/apache.200.txt";

  String outputPath = "build/test/output/sorting/";
  private String apacheCommonRegex = TestConstants.APACHE_COMMON_REGEX;
  private RegexParser apacheCommonParser = new RegexParser( new Fields( "ip", "time", "method", "event", "status", "size" ), apacheCommonRegex, new int[]{1, 2, 3, 4, 5, 6} );

  public SortedValuesTest()
    {
    super( "sorted values", true );
    }

  public void testSortedValues() throws Exception
    {
    runSortTest( "forward", false );
    }

  public void testSortedValuesReversed() throws Exception
    {
    runSortTest( "reversed", true );
    }

  private void runSortTest( String path, boolean sorted ) throws IOException, ParseException
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    Tap source = new Lfs( new TextLine(), inputFileApache );
    Tap sink = new Lfs( new TextLine(), outputPath + path, true );

    Pipe pipe = new Pipe( "apache" );

    // RegexParser.APACHE declares: "time", "method", "event", "status", "size"
    pipe = new Each( pipe, new Fields( "line" ), apacheCommonParser );

    pipe = new Each( pipe, new Insert( new Fields( "col" ), 1 ), Fields.ALL );

    // DateParser.APACHE declares: "ts"
    pipe = new Each( pipe, new Fields( "time" ), new DateParser( "dd/MMM/yyyy:HH:mm:ss Z" ), new Fields( "col", "status", "ts", "event", "ip", "size" ) );

    pipe = new GroupBy( pipe, new Fields( "col" ), new Fields( "status" ), sorted );

    pipe = new Each( pipe, new Identity() ); // let's force the stack to be exercised

    Map<Object, Object> properties = getProperties();

    if( MultiMapReducePlanner.getJobConf( properties ) != null )
      MultiMapReducePlanner.getJobConf( properties ).setNumMapTasks( 13 );

    Flow flow = new FlowConnector( properties ).connect( source, sink, pipe );

    flow.complete();

    validateFile( sink, 200, 6, sorted, 1 );
    }

  public void testSortedValues2() throws Exception
    {
    runSortTest2( "forward2", false );
    }

  public void testSortedValuesReversed2() throws Exception
    {
    runSortTest2( "reversed2", true );
    }

  private void runSortTest2( String path, boolean sorted ) throws IOException, ParseException
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    Tap source = new Lfs( new TextLine(), inputFileApache );
    Tap sink = new Lfs( new TextLine(), outputPath + path, true );

    Pipe pipe = new Pipe( "apache" );

    // RegexParser.APACHE declares: "time", "method", "event", "status", "size"
    pipe = new Each( pipe, new Fields( "line" ), apacheCommonParser );

    pipe = new GroupBy( pipe, new Fields( "status" ) );

    pipe = new Every( pipe, new Fields( "status" ), new Count() );

    // since status will be unique, sorting on count really won't happen.
    // perfect opportunity for planner optimization
    pipe = new GroupBy( pipe, new Fields( "status" ), new Fields( "count" ), sorted );

    Map<Object, Object> properties = getProperties();

    MultiMapReducePlanner.getJobConf( properties ).setNumMapTasks( 13 );

    Flow flow = new FlowConnector( properties ).connect( source, sink, pipe );

    flow.complete();

    validateFile( sink, 6, 6, sorted, 0 );
    }

  public void testSortFails() throws Exception
    {
    String path = "fails";

    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    Tap source = new Lfs( new TextLine(), inputFileApache );
    Tap sink = new Lfs( new TextLine(), outputPath + path, true );

    Pipe pipe = new Pipe( "apache" );

    // RegexParser.APACHE declares: "time", "method", "event", "status", "size"
    pipe = new Each( pipe, new Fields( "line" ), apacheCommonParser );

    pipe = new Each( pipe, new Insert( new Fields( "col" ), 1 ), Fields.ALL );

    // DateParser.APACHE declares: "ts"
    pipe = new Each( pipe, new Fields( "time" ), new DateParser( "dd/MMM/yyyy:HH:mm:ss Z" ), new Fields( "col", "status", "ts", "event", "ip", "size" ) );

    pipe = new GroupBy( pipe, new Fields( "col" ), new Fields( "does-not-exist" ) );

    pipe = new Each( pipe, new Identity() ); // let's force the stack to be exercised

    Map<Object, Object> properties = getProperties();

    MultiMapReducePlanner.getJobConf( properties ).setNumMapTasks( 13 );

    try
      {
      new FlowConnector( properties ).connect( source, sink, pipe );
      fail( "did not throw exception" );
      }
    catch( Exception exception )
      {
      // passes
      }
    }

  private void validateFile( Tap tap, int length, int uniqueValues, boolean isReversed, int comparePosition ) throws IOException, ParseException
    {
    TupleEntryIterator iterator = tap.openForRead( new JobConf() );

    Set<Integer> values = new HashSet<Integer>();

    int lastValue = isReversed ? Integer.MAX_VALUE : Integer.MIN_VALUE;
    int count = 0;

    while( iterator.hasNext() )
      {
      Tuple tuple = iterator.next().getTuple();
      count++;

      tuple = new Tuple( tuple.getString( 1 ).split( "\t" ) );

      int value = tuple.getInteger( comparePosition );

      values.add( value );

      if( isReversed )
        assertTrue( "out of order in " + tap, lastValue >= value );
      else
        assertTrue( "out of order in " + tap, lastValue <= value );

      lastValue = value;
      }

    if( length != -1 )
      assertEquals( "length of " + tap, length, count );

    if( uniqueValues != -1 )
      assertEquals( "unique values of" + tap, uniqueValues, values.size() );
    }
  }