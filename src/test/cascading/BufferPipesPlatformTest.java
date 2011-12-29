/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import cascading.test.HadoopPlatform;
import cascading.test.LocalPlatform;
import cascading.test.PlatformRunner;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import org.junit.Test;

import static data.InputData.inputFileJoined;
import static data.InputData.inputFileLhs;

@PlatformRunner.Platform({LocalPlatform.class, HadoopPlatform.class})
public class BufferPipesPlatformTest extends PlatformTestCase
  {
  public BufferPipesPlatformTest()
    {
    super( false ); // no need for clustering
    }

  @Test
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

  @Test
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

  @Test
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