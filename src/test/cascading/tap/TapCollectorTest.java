/*
 * Copyright (c) 2007-2008 Chris K Wensel. All Rights Reserved.
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
