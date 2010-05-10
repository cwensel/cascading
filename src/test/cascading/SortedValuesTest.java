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
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.MultiMapReducePlanner;
import cascading.operation.Identity;
import cascading.operation.Insert;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexParser;
import cascading.operation.regex.RegexSplitter;
import cascading.operation.text.DateParser;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.Lfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryIterator;
import cascading.util.Util;
import org.apache.hadoop.mapred.JobConf;

public class SortedValuesTest extends ClusterTestCase
  {
  String inputFileApache = "build/test/data/apache.200.txt";
  String inputFileIps = "build/test/data/ips.20.txt";
  String inputFileCross = "build/test/data/lhs+rhs-cross.txt";

  String outputPath = "build/test/output/sorting/";
  private String apacheCommonRegex = TestConstants.APACHE_COMMON_REGEX;
  private RegexParser apacheCommonParser = new RegexParser( new Fields( "ip", "time", "method", "event", "status", "size" ), apacheCommonRegex, new int[]{
    1, 2, 3, 4, 5, 6} );

  public SortedValuesTest()
    {
    super( "sorted values", true );
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

    copyFromLocal( inputFileApache );
    copyFromLocal( inputFileIps );

    Tap sourceApache = new Hfs( new TextLine(), inputFileApache );
    Tap sourceIP = new Hfs( new TextLine(), inputFileIps );
    Tap sink = new Hfs( new TextLine(), outputPath + path, true );

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

    if( MultiMapReducePlanner.getJobConf( properties ) != null )
      MultiMapReducePlanner.getJobConf( properties ).setNumMapTasks( 13 );

    Map sources = new HashMap();

    sources.put( "apache", sourceApache );
    sources.put( "ip", sourceIP );

    Flow flow = new FlowConnector( properties ).connect( sources, sink, pipe );

    flow.complete();

    validateFile( sink, 199, 16, reverseSort, 5 );
    }


  public void testComprehensiveGroupBy() throws IOException
    {
    Boolean[][] testArray = new Boolean[][]{
      // test group comparators
      {false, null, false},
      {true, null, false},

      // test group, reversed
      {false, null, true},
      {true, null, true},

      // test group and sort comparators
      {false, false, false},
      {true, false, false},
      {true, true, false},
      {false, true, false},

      // test group and sort comparators, reversed
      {false, false, true},
      {true, false, true},
      {true, true, true},
      {false, true, true}
    };

    for( int i = 0; i < testArray.length; i++ )
      runComprehensiveCase( testArray[ i ] );

    for( int i = 0; i < testArray.length; i++ )
      runComprehensiveCaseComplex( testArray[ i ] );
    }

  private void runComprehensiveCase( Boolean[] testCase ) throws IOException
    {
    if( !new File( inputFileCross ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileCross );

    String test = Util.join( testCase, "_", true );
    String path = "comprehensive/" + test;

    Tap source = new Hfs( new TextLine( new Fields( "line" ) ), inputFileCross );
    Tap sink = new Hfs( new TextLine( new Fields( "line" ), new Fields( "num", "lower", "upper" ), 1 ), outputPath + path, true );

    Pipe pipe = new Pipe( "comprehensivesort" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexSplitter( new Fields( "num", "lower", "upper" ), "\\s" ) );

    pipe = new Each( pipe, new Fields( "num" ), new Identity( long.class ), Fields.REPLACE );

    Fields groupFields = new Fields( "num" );

    if( testCase[ 0 ] )
      groupFields.setComparator( "num", new TestLongComparator() );

    Fields sortFields = null;

    if( testCase[ 1 ] != null )
      {
      sortFields = new Fields( "upper" );

      if( testCase[ 1 ] )
        sortFields.setComparator( "upper", new TestStringComparator() );
      }

    pipe = new GroupBy( pipe, groupFields, sortFields, testCase[ 2 ] );


    Map<Object, Object> properties = getProperties();

    if( MultiMapReducePlanner.getJobConf( properties ) != null )
      MultiMapReducePlanner.getJobConf( properties ).setNumMapTasks( 13 );

    Flow flow = new FlowConnector( properties ).connect( source, sink, pipe );

    flow.complete();

    validateCase( test, testCase, sink );
    }

  private void runComprehensiveCaseComplex( Boolean[] testCase ) throws IOException
    {
    if( !new File( inputFileCross ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileCross );

    String test = Util.join( testCase, "_", true );
    String path = "comprehensive/" + test;

    Tap source = new Hfs( new TextLine( new Fields( "line" ) ), inputFileCross );
    Tap sink = new Hfs( new TextLine( new Fields( "line" ), new Fields( "upper", "count" ), 1 ), outputPath + path, true );

    Pipe pipe = new Pipe( "comprehensivesortcomplex" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexSplitter( new Fields( "num", "lower", "upper" ), "\\s" ) );

    pipe = new GroupBy( pipe, new Fields( "upper" ) );

    pipe = new Every( pipe, new Fields( "upper" ), new Count( new Fields( "count" ) ) );

    pipe = new Each( pipe, new Fields( "count" ), new Identity( long.class ), Fields.REPLACE );

    Fields groupFields = new Fields( "upper" );

    if( testCase[ 0 ] )
      groupFields.setComparator( "upper", new TestStringComparator() );

    Fields sortFields = null;

    if( testCase[ 1 ] != null )
      {
      sortFields = new Fields( "count" );

      if( testCase[ 1 ] )
        sortFields.setComparator( "count", new TestLongComparator() );
      }

    pipe = new GroupBy( pipe, groupFields, sortFields, testCase[ 2 ] );

    Map<Object, Object> properties = getProperties();

    if( MultiMapReducePlanner.getJobConf( properties ) != null )
      MultiMapReducePlanner.getJobConf( properties ).setNumMapTasks( 13 );

    Flow flow = new FlowConnector( properties ).connect( source, sink, pipe );

    flow.complete();

    validateCaseComplex( test, testCase, sink );
    }

  private void validateCase( String test, Boolean[] testCase, Tap sink ) throws IOException
    {
    TupleEntryIterator iterator = sink.openForRead( new JobConf() );

    LinkedHashMap<Long, List<String>> group = new LinkedHashMap<Long, List<String>>();

    while( iterator.hasNext() )
      {
      Tuple tuple = iterator.next().getTuple();

      String[] values = tuple.getString( 0 ).split( "\\s" );

      long num = Long.parseLong( values[ 0 ] );

      if( !group.containsKey( num ) )
        group.put( num, new ArrayList<String>() );

      group.get( num ).add( values[ 2 ] );
      }

    boolean groupIsReversed = testCase[ 0 ];

    if( testCase[ 2 ] )
      groupIsReversed = !groupIsReversed;

    compare( "grouping+" + test, groupIsReversed, group.keySet() );

    if( testCase[ 1 ] == null )
      return;

    boolean valueIsReversed = testCase[ 1 ];

    if( testCase[ 2 ] )
      valueIsReversed = !valueIsReversed;

    for( Long grouping : group.keySet() )
      compare( "values+" + test, valueIsReversed, group.get( grouping ) );

    }

  private void validateCaseComplex( String test, Boolean[] testCase, Tap sink ) throws IOException
    {
    TupleEntryIterator iterator = sink.openForRead( new JobConf() );

    LinkedHashMap<String, List<Long>> group = new LinkedHashMap<String, List<Long>>();

    while( iterator.hasNext() )
      {
      Tuple tuple = iterator.next().getTuple();

      String[] values = tuple.getString( 0 ).split( "\\s" );

      String value = values[ 0 ];

      if( !group.containsKey( value ) )
        group.put( value, new ArrayList<Long>() );

      group.get( value ).add( Long.parseLong( values[ 1 ] ) );
      }

    boolean groupIsReversed = testCase[ 0 ];

    if( testCase[ 2 ] )
      groupIsReversed = !groupIsReversed;

    compare( "grouping+" + test, groupIsReversed, group.keySet() );

    if( testCase[ 1 ] == null )
      return;

    boolean valueIsReversed = testCase[ 1 ];

    if( testCase[ 2 ] )
      valueIsReversed = !valueIsReversed;

    for( String grouping : group.keySet() )
      compare( "values+" + test, valueIsReversed, group.get( grouping ) );
    }

  private void compare( String test, boolean isReversed, Collection values )
    {
    List<Object> groups = new ArrayList<Object>( values );
    List<Object> sortedGroups = new ArrayList<Object>( groups );

    Collections.sort( sortedGroups, isReversed ? Collections.reverseOrder() : null );

    assertEquals( test, sortedGroups, groups );
    }

  public void testSortFails() throws Exception
    {
    String path = "fails";

    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileApache );

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