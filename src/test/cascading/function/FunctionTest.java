/*
 * Copyright (c) 2007-2008 Vinculum Technologies, Inc. All Rights Reserved.
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

package cascading.function;

import java.io.File;
import java.io.IOException;

import cascading.CascadingTestCase;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.Insert;
import cascading.pipe.Each;
import cascading.pipe.Group;
import cascading.pipe.Pipe;
import cascading.scheme.TextLine;
import cascading.tap.Lfs;
import cascading.tap.Tap;
import cascading.tap.TapIterator;
import cascading.tuple.Fields;

/**
 *
 */
public class FunctionTest extends CascadingTestCase
  {
  String inputFileApache = "build/test/data/apache.200.txt";
  String outputPath = "build/test/output/function/";

  public FunctionTest()
    {
    super( "function tests" );
    }

  public void testInsert() throws IOException
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    Tap source = new Lfs( new TextLine(), inputFileApache );
    Tap sink = new Lfs( new TextLine(), outputPath, true );

    Pipe pipe = new Pipe( "apache" );

    pipe = new Each( pipe, new Insert( new Fields( "A", "B" ), "a", "b" ) );

    pipe = new Group( pipe, new Fields( "A" ) );

    Flow flow = new FlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 200 );

    TapIterator iterator = flow.openSink();

    assertEquals( "not equal: tuple.get(1)", "a\tb", iterator.next().get( 1 ) );
    assertEquals( "not equal: tuple.get(1)", "a\tb", iterator.next().get( 1 ) );
    }

  }
