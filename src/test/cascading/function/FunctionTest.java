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

package cascading.function;

import cascading.CascadingTestCase;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.Insert;
import cascading.operation.function.SetValue;
import cascading.operation.regex.RegexFilter;
import cascading.operation.regex.RegexSplitter;
import cascading.operation.text.FieldFormatter;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.TextLine;
import cascading.tap.Lfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryIterator;

import java.io.File;
import java.io.IOException;

/**
 *
 */
public class FunctionTest extends CascadingTestCase
  {
  String inputFileApache = "build/test/data/apache.200.txt";
  String inputFileUpper = "build/test/data/upper.txt";

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
    Tap sink = new Lfs( new TextLine(), outputPath + "insert", true );

    Pipe pipe = new Pipe( "apache" );

    pipe = new Each( pipe, new Insert( new Fields( "A", "B" ), "a", "b" ) );

    pipe = new GroupBy( pipe, new Fields( "A" ) );

    Flow flow = new FlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 200 );

    TupleEntryIterator iterator = flow.openSink();

    assertEquals( "not equal: tuple.get(1)", "a\tb", iterator.next().get( 1 ) );
    assertEquals( "not equal: tuple.get(1)", "a\tb", iterator.next().get( 1 ) );
    }

  public void testFieldFormatter() throws IOException
    {
    if( !new File( inputFileUpper ).exists() )
      fail( "data file not found" );

    Tap source = new Lfs( new TextLine(), inputFileUpper );
    Tap sink = new Lfs( new TextLine(), outputPath + "formatter", true );

    Pipe pipe = new Pipe( "formatter" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexSplitter( new Fields( "a", "b" ), "\\s" ) );
    pipe = new Each( pipe, new FieldFormatter( new Fields( "result" ), "%s and %s" ) );

    Flow flow = new FlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 5 );

    TupleEntryIterator iterator = flow.openSink();

    assertEquals( "not equal: tuple.get(1)", "1 and A", iterator.next().get( 1 ) );
    assertEquals( "not equal: tuple.get(1)", "2 and B", iterator.next().get( 1 ) );
    }

  public void testSetValue() throws IOException
    {
    if( !new File( inputFileUpper ).exists() )
      fail( "data file not found" );

    Tap source = new Lfs( new TextLine(), inputFileUpper );
    Tap sink = new Lfs( new TextLine(), outputPath + "setvalue", true );

    Pipe pipe = new Pipe( "setvalue" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexSplitter( new Fields( "num", "char" ), "\\s" ) );

    pipe = new Each( pipe, new SetValue( new Fields( "result" ), new RegexFilter( "[A-C]" ) ) );

    Flow flow = new FlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 5 );

    TupleEntryIterator iterator = flow.openSink();

    assertEquals( "not equal: tuple.get(1)", "true", iterator.next().get( 1 ) );
    assertEquals( "not equal: tuple.get(1)", "true", iterator.next().get( 1 ) );
    assertEquals( "not equal: tuple.get(1)", "true", iterator.next().get( 1 ) );
    assertEquals( "not equal: tuple.get(1)", "false", iterator.next().get( 1 ) );
    assertEquals( "not equal: tuple.get(1)", "false", iterator.next().get( 1 ) );
    }
  }
