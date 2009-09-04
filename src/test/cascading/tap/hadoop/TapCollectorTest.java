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

package cascading.tap.hadoop;

import java.io.IOException;

import cascading.CascadingTestCase;
import cascading.flow.hadoop.ConfFlowContext;
import cascading.scheme.SequenceFile;
import cascading.scheme.TextLine;
import cascading.tap.Lfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryIterator;

/**
 *
 */
public class TapCollectorTest extends CascadingTestCase
  {
  String outputPath = "build/test/output/tap/";

  public TapCollectorTest()
    {
    super( "tap collector tests" );
    }


  public void testTapCollectorText() throws IOException
    {
    Tap tap = new Lfs( new TextLine(), outputPath + "tapcollectortext" );

    runTest( tap );
    }

  public void testTapCollectorSequence() throws IOException
    {
    Tap tap = new Lfs( new SequenceFile( new Fields( "string", "value", "number" ) ), outputPath + "tapcollectorseq" );

    runTest( tap );
    }

  private void runTest( Tap tap ) throws IOException
    {
    ConfFlowContext conf = new ConfFlowContext();

    HfsCollector collector = (HfsCollector) tap.openForWrite( conf ); // casting for test

    for( int i = 0; i < 100; i++ )
      collector.collect( new Tuple( "string", "" + i, i ) );

    collector.close();

    TupleEntryIterator iterator = tap.openForRead( conf );

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
