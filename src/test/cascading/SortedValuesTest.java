/*
 * Copyright (c) 2008, Your Corporation. All Rights Reserved.
 */

package cascading;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.Insert;
import cascading.operation.regex.Regexes;
import cascading.operation.text.Texts;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.TextLine;
import cascading.tap.Lfs;
import cascading.tap.Tap;
import cascading.tap.TapIterator;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import org.apache.hadoop.mapred.JobConf;

/** @version $Id: //depot/calku/cascading/src/test/cascading/ArrivalUseCaseTest.java#2 $ */
public class SortedValuesTest extends ClusterTestCase
  {
  String inputFileApache = "build/test/data/apache.200.txt";

  String outputPath = "build/test/output/sorting/";

  public SortedValuesTest()
    {
    super( "sorted values", true );
    }

  public void testSortedValue() throws Exception
    {
    if( true )
      return;

    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    Tap source = new Lfs( new TextLine(), inputFileApache );
    Tap sink = new Lfs( new TextLine(), outputPath, true );

    Pipe pipe = new Pipe( "apache" );

    // RegexParser.APACHE declares: "time", "method", "event", "status", "size"
    pipe = new Each( pipe, new Fields( "line" ), Regexes.APACHE_COMMON_PARSER );

    pipe = new Each( pipe, new Insert( new Fields( "col" ), 1 ), Fields.ALL );

    // DateParser.APACHE declares: "ts"
    pipe = new Each( pipe, new Fields( "time" ), Texts.APACHE_DATE_PARSER, new Fields( "col", "status", "ts", "event", "ip", "size" ) );

    pipe = new GroupBy( pipe, new Fields( "col" ) );

    jobConf.setNumMapTasks( 13 );

    Flow flow = new FlowConnector( jobConf ).connect( source, sink, pipe );

    flow.complete();

    validateFile( sink, 200 );
    }

  private void validateFile( Tap tap, int length ) throws IOException, ParseException
    {
    TapIterator iterator = tap.openForRead( new JobConf() );

    int lastValue = -1;
    int count = 0;

    while( iterator.hasNext() )
      {
      Tuple tuple = iterator.next();
      count++;

      tuple = new Tuple( tuple.getString( 1 ).split( "\t" ) );

      int value = tuple.getInteger( 1 );

      assertTrue( "out of order in " + tap, lastValue < value );

      lastValue = value;
      }

    if( length != -1 )
      assertEquals( "length of " + tap, length, count );
    }

  }