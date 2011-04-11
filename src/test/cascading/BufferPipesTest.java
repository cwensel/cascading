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

package cascading;

import java.util.List;

import cascading.flow.Flow;
import cascading.operation.Insert;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.test.PlatformTest;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

import static data.InputData.inputFileJoined;
import static data.InputData.inputFileLhs;

@PlatformTest(platforms = {"local", "hadoop"})
public class BufferPipesTest extends PlatformTestCase
  {
  public BufferPipesTest()
    {
    super( false ); // no need for clustering
    }

  public void testSimpleBuffer() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLhs );

    Tap source = getPlatform().getTextFile( inputFileLhs );
    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "simple" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexSplitter( new Fields( "num", "lower" ), "\\s" ) );

    pipe = new GroupBy( pipe, new Fields( "num" ) );

    pipe = new Every( pipe, new TestBuffer( new Fields( "next" ), 2, true, true, "next" ) );

    pipe = new Each( pipe, new Insert( new Fields( "final" ), "final" ), Fields.ALL );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 23, null );

    List<Tuple> results = getSinkAsList( flow );

    assertTrue( results.contains( new Tuple( "1\tnull\tnext\tfinal" ) ) );
    assertTrue( results.contains( new Tuple( "1\ta\tnext\tfinal" ) ) );
    assertTrue( results.contains( new Tuple( "1\tb\tnext\tfinal" ) ) );
    assertTrue( results.contains( new Tuple( "1\tc\tnext\tfinal" ) ) );
    assertTrue( results.contains( new Tuple( "1\tnull\tnext\tfinal" ) ) );
    }

  public void testSimpleBuffer2() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLhs );

    Tap source = getPlatform().getTextFile( inputFileLhs );
    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "simple2" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexSplitter( new Fields( "num", "lower" ), "\\s" ) );

    pipe = new GroupBy( pipe, new Fields( "num" ) );

    pipe = new Every( pipe, new Fields( "lower" ), new TestBuffer( new Fields( "next" ), 1, true, "next" ), Fields.RESULTS );

    pipe = new Each( pipe, new Insert( new Fields( "final" ), "final" ), Fields.ALL );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 18, null );

    List<Tuple> results = getSinkAsList( flow );

    assertTrue( results.contains( new Tuple( "next\tfinal" ) ) );
    assertTrue( results.contains( new Tuple( "next\tfinal" ) ) );
    assertTrue( results.contains( new Tuple( "next\tfinal" ) ) );
    }

  public void testSimpleBuffer3() throws Exception
    {
    getPlatform().copyFromLocal( inputFileJoined );

    Tap source = getPlatform().getTextFile( inputFileJoined );
    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "simple3" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexSplitter( new Fields( "num", "lower", "upper" ), "\\s" ) );

    pipe = new GroupBy( pipe, new Fields( "num" ) );

    pipe = new Every( pipe, new TestBuffer( new Fields( "new" ), new Tuple( "new" ) ), new Fields( "new", "lower", "upper" ) );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 5, null );

    List<Tuple> results = getSinkAsList( flow );

    assertTrue( results.contains( new Tuple( "new\ta\tA" ) ) );
    assertTrue( results.contains( new Tuple( "new\tb\tB" ) ) );
    assertTrue( results.contains( new Tuple( "new\tc\tC" ) ) );
    }
  }