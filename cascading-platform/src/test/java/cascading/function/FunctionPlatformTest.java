/*
 * Copyright (c) 2007-2022 The Cascading Authors. All Rights Reserved.
 *
 * Project and contact information: https://cascading.wensel.net/
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

package cascading.function;

import java.io.IOException;
import java.util.List;

import cascading.PlatformTestCase;
import cascading.flow.Flow;
import cascading.operation.Insert;
import cascading.operation.function.SetValue;
import cascading.operation.regex.RegexFilter;
import cascading.operation.regex.RegexSplitter;
import cascading.operation.text.FieldFormatter;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import org.junit.Test;

import static data.InputData.inputFileApache200;
import static data.InputData.inputFileUpper;

/**
 *
 */
public class FunctionPlatformTest extends PlatformTestCase
  {
  public FunctionPlatformTest()
    {
    }

  @Test
  public void testInsert() throws IOException
    {
    getPlatform().copyFromLocal( inputFileApache200 );

    Tap source = getPlatform().getTextFile( inputFileApache200 );
    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "insert" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "apache" );

    pipe = new Each( pipe, new Insert( new Fields( "A", "B" ), "a", "b" ) );

    pipe = new GroupBy( pipe, new Fields( "A" ) );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 200 );

    List<Tuple> results = getSinkAsList( flow );

    assertTrue( results.contains( new Tuple( "a\tb" ) ) );
    assertTrue( results.contains( new Tuple( "a\tb" ) ) );
    }

  @Test
  public void testFieldFormatter() throws IOException
    {
    getPlatform().copyFromLocal( inputFileUpper );

    Tap source = getPlatform().getTextFile( inputFileUpper );
    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "formatter" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "formatter" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexSplitter( new Fields( "a", "b" ), "\\s" ) );
    pipe = new Each( pipe, new FieldFormatter( new Fields( "result" ), "%s and %s" ) );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 5 );

    List<Tuple> results = getSinkAsList( flow );

    assertTrue( results.contains( new Tuple( "1 and A" ) ) );
    assertTrue( results.contains( new Tuple( "2 and B" ) ) );
    }

  @Test
  public void testSetValue() throws IOException
    {
    getPlatform().copyFromLocal( inputFileUpper );

    Tap source = getPlatform().getTextFile( inputFileUpper );
    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "setvalue" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "setvalue" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexSplitter( new Fields( "num", "char" ), "\\s" ) );

    pipe = new Each( pipe, new SetValue( new Fields( "result" ), new RegexFilter( "[A-C]" ) ) );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 5 );

    List<Tuple> results = getSinkAsList( flow );

    assertTrue( results.contains( new Tuple( "true" ) ) );
    assertTrue( results.contains( new Tuple( "true" ) ) );
    assertTrue( results.contains( new Tuple( "true" ) ) );
    assertTrue( results.contains( new Tuple( "false" ) ) );
    assertTrue( results.contains( new Tuple( "false" ) ) );
    }
  }
