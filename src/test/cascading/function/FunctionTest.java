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

package cascading.function;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import cascading.PlatformTestCase;
import cascading.flow.Flow;
import cascading.operation.Function;
import cascading.operation.Insert;
import cascading.operation.function.SetValue;
import cascading.operation.regex.RegexFilter;
import cascading.operation.regex.RegexSplitter;
import cascading.operation.text.FieldFormatter;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.AggregateBy;
import cascading.pipe.assembly.CountBy;
import cascading.pipe.assembly.SumBy;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.test.PlatformTest;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleListCollector;

import static data.InputData.inputFileApache200;
import static data.InputData.inputFileUpper;

/**
 *
 */
@PlatformTest(platforms = {"local", "hadoop"})
public class FunctionTest extends PlatformTestCase
  {
  public FunctionTest()
    {
    }

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

  public void testPartialCounts()
    {
    Function function = new AggregateBy.CompositeFunction( new Fields( "value" ), Fields.ALL, new CountBy.CountPartials( new Fields( "count" ) ), 2 );

    Fields incoming = new Fields( "value" );
    TupleEntry[] tuples = new TupleEntry[]{
      new TupleEntry( incoming, new Tuple( "a" ) ),
      new TupleEntry( incoming, new Tuple( "a" ) ),
      new TupleEntry( incoming, new Tuple( "b" ) ),
      new TupleEntry( incoming, new Tuple( "b" ) ),
      new TupleEntry( incoming, new Tuple( "c" ) ),
      new TupleEntry( incoming, new Tuple( "c" ) ),
      new TupleEntry( incoming, new Tuple( "a" ) ),
      new TupleEntry( incoming, new Tuple( "a" ) ),
      new TupleEntry( incoming, new Tuple( "d" ) ),
      new TupleEntry( incoming, new Tuple( "d" ) ),
    };

    Tuple[] expected = new Tuple[]{
      new Tuple( "a", 2L ),
      new Tuple( "b", 2L ),
      new Tuple( "c", 2L ),
      new Tuple( "a", 2L ),
      new Tuple( "d", 2L ),
    };

    TupleListCollector collector = invokeFunction( function, tuples, new Fields( "value", "count" ) );

    Iterator<Tuple> iterator = collector.iterator();

    int count = 0;
    while( iterator.hasNext() )
      assertEquals( expected[ count++ ], iterator.next() );
    }

  public void testPartialSums()
    {
    Function function = new AggregateBy.CompositeFunction( new Fields( "key" ), new Fields( "value" ), new SumBy.SumPartials( new Fields( "sum" ), float.class ), 2 );

    Fields incoming = new Fields( "key", "value" );
    TupleEntry[] tuples = new TupleEntry[]{
      new TupleEntry( incoming, new Tuple( "a", 1 ) ),
      new TupleEntry( incoming, new Tuple( "a", 1 ) ),
      new TupleEntry( incoming, new Tuple( "b", 1 ) ),
      new TupleEntry( incoming, new Tuple( "b", 1 ) ),
      new TupleEntry( incoming, new Tuple( "c", 1 ) ),
      new TupleEntry( incoming, new Tuple( "c", 1 ) ),
      new TupleEntry( incoming, new Tuple( "a", 1 ) ),
      new TupleEntry( incoming, new Tuple( "a", 1 ) ),
      new TupleEntry( incoming, new Tuple( "d", 1 ) ),
      new TupleEntry( incoming, new Tuple( "d", 1 ) ),
    };

    Tuple[] expected = new Tuple[]{
      new Tuple( "a", 2F ),
      new Tuple( "b", 2F ),
      new Tuple( "c", 2F ),
      new Tuple( "a", 2F ),
      new Tuple( "d", 2F ),
    };

    TupleListCollector collector = invokeFunction( function, tuples, new Fields( "key", "sum" ) );

    Iterator<Tuple> iterator = collector.iterator();

    int count = 0;
    while( iterator.hasNext() )
      assertEquals( expected[ count++ ], iterator.next() );
    }
  }
