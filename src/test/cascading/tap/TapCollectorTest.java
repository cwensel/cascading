/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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

import cascading.PlatformTestCase;
import cascading.test.PlatformTest;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

/**
 *
 */
@PlatformTest(platforms = {"local", "hadoop"})
public class TapCollectorTest extends PlatformTestCase
  {
  public TapCollectorTest()
    {
    }

  public void testTapCollectorText() throws IOException
    {
    Tap tap = getPlatform().getTextFile( getOutputPath( "tapcollectortext" ) );

    runTest( tap );
    }

  public void testTapCollectorSequence() throws IOException
    {
    Tap tap = getPlatform().getDelimitedFile( new Fields( "string", "value", "number" ), "\t", getOutputPath( "tapcollectorseq" ) );

    runTest( tap );
    }

  private void runTest( Tap tap ) throws IOException
    {
    TupleEntryCollector collector = tap.openForWrite( getPlatform().getFlowProcess() ); // casting for test

    for( int i = 0; i < 100; i++ )
      collector.add( new Tuple( "string", "" + i, i ) );

    collector.close();

    TupleEntryIterator iterator = tap.openForRead( getPlatform().getFlowProcess() );

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
