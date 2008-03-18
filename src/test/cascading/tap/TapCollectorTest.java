/*
 * Copyright (c) 2008, Your Corporation. All Rights Reserved.
 */

package cascading.tap;

import java.io.IOException;

import cascading.CascadingTestCase;
import cascading.scheme.TextLine;
import cascading.tuple.Tuple;
import org.apache.hadoop.mapred.JobConf;

/**
 *
 */
public class TapCollectorTest extends CascadingTestCase
  {
  String outputPath = "build/test/output/fields/";

  public TapCollectorTest()
    {
    super( "tap collector tests" );
    }


  public void testTapCollector() throws IOException
    {
    Tap tap = new Lfs( new TextLine(), outputPath + "tapcollector" );

    JobConf conf = new JobConf();

    TapCollector collector = tap.openForWrite( conf );

    for( int i = 0; i < 100; i++ )
      collector.collect( new Tuple( "string", "" + i, i ) );

    collector.close();

    TapIterator iterator = tap.openForRead( conf );

    int count = 0;
    while( iterator.hasNext() )
      {
      iterator.next();
      count++;
      }

    iterator.close();

    assertEquals( "wrong size", 100, count );
    }
  }
