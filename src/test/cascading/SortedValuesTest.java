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

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
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
import cascading.operation.text.DateParser;
import cascading.pipe.CoGroup;
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

public class SortedValuesTest extends ClusterTestCase
  {
  String inputFileApache = "build/test/data/apache.200.txt";
  String inputFileIps = "build/test/data/ips.20.txt";

  String outputPath = "build/test/output/sorting/";
  private String apacheCommonRegex = TestConstants.APACHE_COMMON_REGEX;
  private RegexParser apacheCommonParser = new RegexParser( new Fields( "ip", "time", "method", "event", "status", "size" ), apacheCommonRegex, new int[]{
    1, 2, 3, 4, 5, 6} );

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

    if( MultiMapReducePlanner.getConfiguration( properties ) != null )
      MultiMapReducePlanner.getConfiguration( properties ).setNumMapTasks( 13 );

    Flow flow = new FlowConnector( properties ).connect( source, sink, pipe );

    flow.complete();

    validateFile( sink, 200, 6, sorted, 1 );
    }

  public void testSortedValues2() throws Exception
    {
    runSortTest2( "forward2", false, true );
    }

  public void testSortedValuesReversed2() throws Exception
    {
    runSortTest2( "reversed2", true, true );
    }

  public void testSortedValuesReversed3() throws Exception
    {
    runSortTest2( "reversed2nosortfields", true, false );
    }

  private void runSortTest2( String path, boolean sorted, boolean useSortFields ) throws IOException, ParseException
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
    Fields sortFields = useSortFields ? new Fields( "count" ) : null;
    pipe = new GroupBy( pipe, new Fields( "status" ), sortFields, sorted );

    Map<Object, Object> properties = getProperties();

    MultiMapReducePlanner.getConfiguration( properties ).setNumMapTasks( 13 );

    Flow flow = new FlowConnector( properties ).connect( source, sink, pipe );

    flow.complete();

    validateFile( sink, 6, 6, sorted, 0 );
    }

  public void testComparatorValues() throws Exception
    {
    runComparatorTest( "compareforward", false );
    }

  public void testComparatorValuesReversed() throws Exception
    {
    runComparatorTest( "comparereversed", true );
    }

  private void runComparatorTest( String path, boolean reverseSort ) throws IOException, ParseException
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

    Fields groupFields = new Fields( "ts" );

    groupFields.setComparator( "ts", new TestLongComparator() );

    pipe = new GroupBy( pipe, groupFields, null, reverseSort );

    pipe = new Each( pipe, new Identity() ); // let's force the stack to be exercised

    Map<Object, Object> properties = getProperties();

    if( MultiMapReducePlanner.getConfiguration( properties ) != null )
      MultiMapReducePlanner.getConfiguration( properties ).setNumMapTasks( 13 );

    Flow flow = new FlowConnector( properties ).connect( source, sink, pipe );

    flow.complete();

    validateFile( sink, 200, 161, !reverseSort, 2 );
    }

  public void testComparatorSortedValues() throws Exception
    {
    runComparatorSortTest( "comparesortforward", false );
    }

  public void testComparatorSortedValuesReversed() throws Exception
    {
    runComparatorSortTest( "comparesortreversed", true );
    }

  private void runComparatorSortTest( String path, boolean reverseSort ) throws IOException, ParseException
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    Tap source = new Lfs( new TextLine(), inputFileApache );
    Tap sink = new Lfs( new TextLine(), outputPath + path, true );

    Pipe pipe = new Pipe( "apache" );

    // RegexParser.APACHE declares: "time", "method", "event", "status", "size"
    pipe = new Each( pipe, new Fields( "line" ), apacheCommonParser );

    pipe = new Each( pipe, new Insert( new Fields( "col" ), 1l ), Fields.ALL );

    // DateParser.APACHE declares: "ts"
    pipe = new Each( pipe, new Fields( "time" ), new DateParser( "dd/MMM/yyyy:HH:mm:ss Z" ), new Fields( "col", "status", "ts", "event", "ip", "size" ) );

    Fields groupFields = new Fields( "col" );

    groupFields.setComparator( "col", new TestLongComparator() );

    Fields sortFields = new Fields( "status" );

    sortFields.setComparator( "status", new TestStringComparator() );

    pipe = new GroupBy( pipe, groupFields, sortFields, reverseSort );

    pipe = new Each( pipe, new Identity() ); // let's force the stack to be exercised

    Map<Object, Object> properties = getProperties();

    if( MultiMapReducePlanner.getConfiguration( properties ) != null )
      MultiMapReducePlanner.getConfiguration( properties ).setNumMapTasks( 13 );

    Flow flow = new FlowConnector( properties ).connect( source, sink, pipe );

    flow.complete();

    validateFile( sink, 200, 6, !reverseSort, 1 );
    }


  public void testCoGroupComparatorValues() throws Exception
    {
    runCoGroupComparatorTest( "cogroupcompareforward", false );
    }

  public void testCoGroupComparatorValuesReversed() throws Exception
    {
    runCoGroupComparatorTest( "cogroupcomparereversed", true );
    }

  private void runCoGroupComparatorTest( String path, boolean reverseSort ) throws IOException, ParseException
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    Tap sourceApache = new Lfs( new TextLine(), inputFileApache );
    Tap sourceIP = new Lfs( new TextLine(), inputFileIps );
    Tap sink = new Lfs( new TextLine(), outputPath + path, true );

    Pipe apachePipe = new Pipe( "apache" );

    apachePipe = new Each( apachePipe, new Fields( "line" ), apacheCommonParser );
    apachePipe = new Each( apachePipe, new Insert( new Fields( "col" ), 1 ), Fields.ALL );
    apachePipe = new Each( apachePipe, new Fields( "ip" ), new RegexParser( new Fields( "octet" ), "^[^.]*" ), new Fields( "col", "status", "event", "octet", "size" ) );
    apachePipe = new Each( apachePipe, new Fields( "octet" ), new Identity( long.class ), Fields.REPLACE );

    Fields groupApache = new Fields( "octet" );
    groupApache.setComparator( "octet", new TestLongComparator( reverseSort ) );

    Pipe ipPipe = new Pipe( "ip" );

    ipPipe = new Each( ipPipe, new Fields( "line" ), new Identity( new Fields( "rawip" ) ) );
    ipPipe = new Each( ipPipe, new Fields( "rawip" ), new RegexParser( new Fields( "rawoctet" ), "^[^.]*" ), new Fields( "rawoctet" ) );
    ipPipe = new Each( ipPipe, new Fields( "rawoctet" ), new Identity( long.class ), Fields.REPLACE );

    Fields groupIP = new Fields( "rawoctet" );
    groupIP.setComparator( "rawoctet", new TestLongComparator( reverseSort ) );

    Pipe pipe = new CoGroup( apachePipe, groupApache, ipPipe, groupIP );

    pipe = new Each( pipe, new Identity() ); // let's force the stack to be exercised

    Map<Object, Object> properties = getProperties();

    if( MultiMapReducePlanner.getConfiguration( properties ) != null )
      MultiMapReducePlanner.getConfiguration( properties ).setNumMapTasks( 13 );

    Map sources = new HashMap();

    sources.put( "apache", sourceApache );
    sources.put( "ip", sourceIP );

    Flow flow = new FlowConnector( properties ).connect( sources, sink, pipe );

    flow.complete();

    validateFile( sink, 199, 16, reverseSort, 5 );
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

    MultiMapReducePlanner.getConfiguration( properties ).setNumMapTasks( 13 );

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

    Set<Long> values = new HashSet<Long>();

    long lastValue = isReversed ? Long.MAX_VALUE : Long.MIN_VALUE;
    int count = 0;

    while( iterator.hasNext() )
      {
      Tuple tuple = iterator.next().getTuple();
      count++;

      tuple = new Tuple( (Object[]) tuple.getString( 1 ).split( "\t" ) );

      long value = tuple.getLong( comparePosition );

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
      assertEquals( "unique values of " + tap, uniqueValues, values.size() );
    }
  }