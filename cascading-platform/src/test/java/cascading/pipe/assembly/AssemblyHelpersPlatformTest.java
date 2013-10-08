/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
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

package cascading.pipe.assembly;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.regex.Pattern;

import cascading.PlatformTestCase;
import cascading.cascade.Cascades;
import cascading.flow.Flow;
import cascading.operation.AssertionLevel;
import cascading.operation.Function;
import cascading.operation.Identity;
import cascading.operation.assertion.AssertExpression;
import cascading.operation.expression.ExpressionFunction;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Merge;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryIterator;
import org.junit.Test;

import static data.InputData.*;

/**
 *
 */
public class AssemblyHelpersPlatformTest extends PlatformTestCase
  {
  public AssemblyHelpersPlatformTest()
    {
    }

  @Test
  public void testCoerce() throws IOException
    {
    getPlatform().copyFromLocal( inputFileLower );

    Tap source = getPlatform().getTextFile( inputFileLower );
    Tap sink = getPlatform().getTextFile( new Fields( "line" ), new Fields( "num", "char" ), getOutputPath( "coerce" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "coerce" );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );
    pipe = new Each( pipe, new Fields( "line" ), splitter );

    pipe = new Coerce( pipe, new Fields( "num" ), Integer.class );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 5, 1, Pattern.compile( "^\\d+\\s\\w+$" ) );
    }

  @Test
  public void testCoerceFields() throws IOException
    {
    getPlatform().copyFromLocal( inputFileLower );

    Tap source = getPlatform().getTextFile( inputFileLower );
    Tap sink = getPlatform().getTextFile( new Fields( "line" ), new Fields( "num", "char" ), getOutputPath( "coercefields" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "coerce" );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ).applyTypes( String.class, String.class ), " " );
    pipe = new Each( pipe, new Fields( "line" ), splitter );

    pipe = new Coerce( pipe, new Fields( "num" ).applyTypes( Integer.class ) );

    pipe = new Each( pipe, new Fields( "num" ), AssertionLevel.STRICT, new AssertExpression( "num instanceof Integer", Object.class ) );

    pipe = new Coerce( pipe, new Fields( "num" ).applyTypes( String.class ) );

    pipe = new Each( pipe, new Fields( "num" ), AssertionLevel.STRICT, new AssertExpression( "num instanceof String", Object.class ) );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 5, 1, Pattern.compile( "^\\d+\\s\\w+$" ) );
    }

  @Test
  public void testRetainNarrow() throws IOException
    {
    getPlatform().copyFromLocal( inputFileLower );

    Tap source = getPlatform().getTextFile( inputFileLower );
    Tap sink = getPlatform().getTextFile( new Fields( "num" ), new Fields( "num" ), getOutputPath( "retainnarrow" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "shape" );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );
    pipe = new Each( pipe, new Fields( "line" ), splitter );

    pipe = new Retain( pipe, new Fields( "num" ) );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 5, 1, Pattern.compile( "^\\d+$" ) );
    }

  @Test
  public void testDiscardNarrow() throws IOException
    {
    getPlatform().copyFromLocal( inputFileLower );

    Tap source = getPlatform().getTextFile( inputFileLower );
    Tap sink = getPlatform().getTextFile( new Fields( "num" ), new Fields( "num" ), getOutputPath( "discardnarrow" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "shape" );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );
    pipe = new Each( pipe, new Fields( "line" ), splitter );

    pipe = new Discard( pipe, new Fields( "char" ) );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 5, 1, Pattern.compile( "^\\d+$" ) );
    }

  @Test
  public void testRenameNamed() throws IOException
    {
    getPlatform().copyFromLocal( inputFileLower );

    Tap source = getPlatform().getTextFile( inputFileLower );
    Tap sink = getPlatform().getTextFile( new Fields( "line" ), new Fields( "item", "element" ), getOutputPath( "rename" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "shape" );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );
    pipe = new Each( pipe, new Fields( "line" ), splitter );

    pipe = new Rename( pipe, new Fields( "num", "char" ), new Fields( "item", "element" ) );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 5, 1, Pattern.compile( "^\\d+\\s\\w+$" ) );
    }

  @Test
  public void testRenameAll() throws IOException
    {
    getPlatform().copyFromLocal( inputFileLower );

    Tap source = getPlatform().getTextFile( inputFileLower );
    Tap sink = getPlatform().getTextFile( new Fields( "line" ), new Fields( "item", "element" ), getOutputPath( "renameall" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "shape" );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );
    pipe = new Each( pipe, new Fields( "line" ), splitter );

    pipe = new Rename( pipe, Fields.ALL, new Fields( "item", "element" ) );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 5, 1, Pattern.compile( "^\\d+\\s\\w+$" ) );
    }

  @Test
  public void testRenameNarrow() throws IOException
    {
    getPlatform().copyFromLocal( inputFileLower );

    Tap source = getPlatform().getTextFile( inputFileLower );
    Tap sink = getPlatform().getTextFile( new Fields( "item" ), new Fields( "char", "item" ), getOutputPath( "renamenarrow" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "shape" );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );
    pipe = new Each( pipe, new Fields( "line" ), splitter );

    pipe = new Rename( pipe, new Fields( "num" ), new Fields( "item" ) );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 5, 1, Pattern.compile( "^\\w+\\s\\d+$" ) );
    }

  @Test
  public void testUnique() throws IOException
    {
    getPlatform().copyFromLocal( inputFileLhs );

    Tap source = getPlatform().getTextFile( inputFileLhs );
    Tap sink = getPlatform().getTextFile( new Fields( "item" ), new Fields( "num", "char" ), getOutputPath( "unique" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "shape" );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );
    pipe = new Each( pipe, new Fields( "line" ), splitter );

    pipe = new Unique( pipe, new Fields( "num" ) );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 5, 1, Pattern.compile( "^\\d+\\s\\w+$" ) );
    }

  @Test
  public void testUniqueMerge() throws IOException
    {
    getPlatform().copyFromLocal( inputFileLhs );
    getPlatform().copyFromLocal( inputFileRhs );

    Tap sourceLhs = getPlatform().getTextFile( inputFileLhs );
    Tap sourceRhs = getPlatform().getTextFile( inputFileRhs );
    Tap sink = getPlatform().getTextFile( new Fields( "item" ), new Fields( "num", "char" ), getOutputPath( "uniquemerge-nondeterministic" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );
    Pipe lhsPipe = new Pipe( "lhs" );
    lhsPipe = new Each( lhsPipe, new Fields( "line" ), splitter );

    Pipe rhsPipe = new Pipe( "rhs" );
    rhsPipe = new Each( rhsPipe, new Fields( "line" ), splitter );

    Pipe pipe = new Unique( Pipe.pipes( lhsPipe, rhsPipe ), new Fields( "num" ) );

    Map<String, Tap> sources = Cascades.tapsMap( Pipe.pipes( lhsPipe, rhsPipe ), Tap.taps( sourceLhs, sourceRhs ) );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, pipe );

    flow.complete();

    validateLength( flow, 5, 1, Pattern.compile( "^\\d+\\s\\w+$" ) );
    }

  @Test
  public void testCount() throws IOException
    {
    getPlatform().copyFromLocal( inputFileLhs );

    Tap source = getPlatform().getDelimitedFile( new Fields( "num", "char" ), " ", inputFileLhs );
    Tap sink = getPlatform().getDelimitedFile( new Fields( "char", "count" ), "\t",
      new Class[]{String.class, Integer.TYPE}, getOutputPath( "count" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "count" );

    pipe = new CountBy( pipe, new Fields( "char" ), new Fields( "count" ), 2 );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 5, 2, Pattern.compile( "^\\w+\\s\\d+$" ) );

    Tuple[] results = new Tuple[]{
      new Tuple( "a", 2 ),
      new Tuple( "b", 4 ),
      new Tuple( "c", 4 ),
      new Tuple( "d", 2 ),
      new Tuple( "e", 1 ),
    };

    TupleEntryIterator iterator = flow.openSink();
    int count = 0;

    while( iterator.hasNext() )
      assertEquals( results[ count++ ], iterator.next().getTuple() );

    iterator.close();
    }

  @Test
  public void testCountAll() throws IOException
    {
    getPlatform().copyFromLocal( inputFileLhs );

    Tap source = getPlatform().getDelimitedFile( new Fields( "num", "char" ), " ", inputFileLhs );
    Tap sink = getPlatform().getDelimitedFile( new Fields( "count" ), "\t",
      new Class[]{Integer.TYPE}, getOutputPath( "countall" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "count" );

    CountBy countBy = new CountBy( new Fields( "char" ), new Fields( "count" ) );

    pipe = new AggregateBy( pipe, Fields.NONE, 2, countBy );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 1, 1, Pattern.compile( "^\\d+$" ) );

    Tuple[] results = new Tuple[]{
      new Tuple( 13 )
    };

    TupleEntryIterator iterator = flow.openSink();
    int count = 0;

    while( iterator.hasNext() )
      assertEquals( results[ count++ ], iterator.next().getTuple() );

    iterator.close();
    }

  @Test
  public void testCountNullNotNull() throws IOException
    {
    getPlatform().copyFromLocal( inputFileLhs );

    Tap source = getPlatform().getDelimitedFile( new Fields( "num", "char" ), " ", inputFileLhs );
    Tap sink = getPlatform().getDelimitedFile( new Fields( "notnull", "null" ), "\t",
      new Class[]{Integer.TYPE, Integer.TYPE}, getOutputPath( "countnullnotnull" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "count" );

    ExpressionFunction function = new ExpressionFunction( Fields.ARGS, "\"c\".equals($0) ? null : $0", String.class );
    pipe = new Each( pipe, new Fields( "char" ), function, Fields.REPLACE );

    CountBy countNotNull = new CountBy( new Fields( "char" ), new Fields( "notnull" ), CountBy.Include.NO_NULLS );
    CountBy countNull = new CountBy( new Fields( "char" ), new Fields( "null" ), CountBy.Include.ONLY_NULLS );

    pipe = new AggregateBy( pipe, Fields.NONE, 2, countNotNull, countNull );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 1, 2, Pattern.compile( "^\\d+\t\\d+$" ) );

    Tuple[] results = new Tuple[]{
      new Tuple( 9, 4 )
    };

    TupleEntryIterator iterator = flow.openSink();
    int count = 0;

    while( iterator.hasNext() )
      assertEquals( results[ count++ ], iterator.next().getTuple() );

    iterator.close();
    }

  @Test
  public void testCountMerge() throws IOException
    {
    getPlatform().copyFromLocal( inputFileLhs );
    getPlatform().copyFromLocal( inputFileRhs );

    Tap lhs = getPlatform().getDelimitedFile( new Fields( "num", "char" ), " ", inputFileLhs );
    Tap rhs = getPlatform().getDelimitedFile( new Fields( "num", "char" ), " ", inputFileRhs );
    Tap sink = getPlatform().getDelimitedFile( new Fields( "char", "count" ), "\t",
      new Class[]{String.class, Integer.TYPE}, getOutputPath( "mergecount" ), SinkMode.REPLACE );

    Pipe lhsPipe = new Pipe( "count-lhs" );
    Pipe rhsPipe = new Pipe( "count-rhs" );

    rhsPipe = new Each( rhsPipe, new Fields( "char" ), new ExpressionFunction( Fields.ARGS, "$0.toLowerCase()", String.class ), Fields.REPLACE );

    Pipe countPipe = new CountBy( Pipe.pipes( lhsPipe, rhsPipe ), new Fields( "char" ), new Fields( "count" ), 2 );

    Map<String, Tap> tapMap = Cascades.tapsMap( Pipe.pipes( lhsPipe, rhsPipe ), Tap.taps( lhs, rhs ) );

    Flow flow = getPlatform().getFlowConnector().connect( tapMap, sink, countPipe );

    flow.complete();

    validateLength( flow, 5, 2, Pattern.compile( "^\\w+\\s\\d+$" ) );

    Tuple[] results = new Tuple[]{
      new Tuple( "a", 4 ),
      new Tuple( "b", 8 ),
      new Tuple( "c", 8 ),
      new Tuple( "d", 4 ),
      new Tuple( "e", 2 ),
    };

    TupleEntryIterator iterator = flow.openSink();
    int count = 0;

    while( iterator.hasNext() )
      assertEquals( results[ count++ ], iterator.next().getTuple() );

    iterator.close();
    }

  @Test
  public void testSumBy() throws IOException
    {
    getPlatform().copyFromLocal( inputFileLhs );

    Tap source = getPlatform().getDelimitedFile( new Fields( "num", "char" ), " ", inputFileLhs );
    Tap sink = getPlatform().getDelimitedFile( new Fields( "char", "sum" ), "\t",
      new Class[]{String.class, Integer.TYPE}, getOutputPath( "sum" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "sum" );

    pipe = new SumBy( pipe, new Fields( "char" ), new Fields( "num" ), new Fields( "sum" ), long.class, 2 );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 5, 2, Pattern.compile( "^\\w+\\s\\d+$" ) );

    Tuple[] results = new Tuple[]{
      new Tuple( "a", 6 ),
      new Tuple( "b", 12 ),
      new Tuple( "c", 10 ),
      new Tuple( "d", 6 ),
      new Tuple( "e", 5 ),
    };

    TupleEntryIterator iterator = flow.openSink();
    int count = 0;

    while( iterator.hasNext() )
      assertEquals( results[ count++ ], iterator.next().getTuple() );

    iterator.close();
    }

  @Test
  public void testSumByNulls() throws IOException
    {
    getPlatform().copyFromLocal( inputFileLhs );

    Tap source = getPlatform().getDelimitedFile( new Fields( "num", "char" ), " ", inputFileLhs );
    Tap sink = getPlatform().getDelimitedFile( new Fields( "char", "sum" ), "\t",
      new Class[]{String.class, Integer.class}, getOutputPath( "sumnulls" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "sum" );

    ExpressionFunction function = new ExpressionFunction( Fields.ARGS, "5 == $0 ? null : $0", Integer.class );
    pipe = new Each( pipe, new Fields( "num" ), function, Fields.REPLACE );

    // Long.class denotes return null for null, not zero
    pipe = new SumBy( pipe, new Fields( "char" ), new Fields( "num" ), new Fields( "sum" ), Integer.class, 2 );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 5, 2, Pattern.compile( "^\\w+\\s(\\d+|null)$" ) );

    Tuple[] results = new Tuple[]{
      new Tuple( "a", 1 ),
      new Tuple( "b", 7 ),
      new Tuple( "c", 10 ),
      new Tuple( "d", 6 ),
      new Tuple( "e", null ),
    };

    TupleEntryIterator iterator = flow.openSink();
    int count = 0;

    while( iterator.hasNext() )
      assertEquals( results[ count++ ], iterator.next().getTuple() );

    iterator.close();
    }

  @Test
  public void testSumMerge() throws IOException
    {
    getPlatform().copyFromLocal( inputFileLhs );
    getPlatform().copyFromLocal( inputFileRhs );

    Tap lhs = getPlatform().getDelimitedFile( new Fields( "num", "char" ), " ", inputFileLhs );
    Tap rhs = getPlatform().getDelimitedFile( new Fields( "num", "char" ), " ", inputFileRhs );
    Tap sink = getPlatform().getDelimitedFile( new Fields( "char", "sum" ), "\t",
      new Class[]{String.class, Integer.TYPE}, getOutputPath( "mergesum" ), SinkMode.REPLACE );

    Pipe lhsPipe = new Pipe( "sum-lhs" );
    Pipe rhsPipe = new Pipe( "sum-rhs" );

    rhsPipe = new Each( rhsPipe, new Fields( "char" ), new ExpressionFunction( Fields.ARGS, "$0.toLowerCase()", String.class ), Fields.REPLACE );

    Pipe sumPipe = new SumBy( Pipe.pipes( lhsPipe, rhsPipe ), new Fields( "char" ), new Fields( "num" ), new Fields( "sum" ), long.class, 2 );

    Map<String, Tap> tapMap = Cascades.tapsMap( Pipe.pipes( lhsPipe, rhsPipe ), Tap.taps( lhs, rhs ) );

    Flow flow = getPlatform().getFlowConnector().connect( tapMap, sink, sumPipe );

    flow.complete();

    validateLength( flow, 5, 2, Pattern.compile( "^\\w+\\s\\d+$" ) );

    Tuple[] results = new Tuple[]{
      new Tuple( "a", 12 ),
      new Tuple( "b", 24 ),
      new Tuple( "c", 20 ),
      new Tuple( "d", 12 ),
      new Tuple( "e", 10 ),
    };

    TupleEntryIterator iterator = flow.openSink();
    int count = 0;

    while( iterator.hasNext() )
      assertEquals( results[ count++ ], iterator.next().getTuple() );

    iterator.close();
    }

  @Test
  public void testAverageBy() throws IOException
    {
    getPlatform().copyFromLocal( inputFileLhs );

    Tap source = getPlatform().getDelimitedFile( new Fields( "num", "char" ), " ", inputFileLhs );
    Tap sink = getPlatform().getDelimitedFile( new Fields( "char", "average" ), "\t",
      new Class[]{String.class, Double.TYPE}, getOutputPath( "average" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "average" );

    pipe = new AverageBy( pipe, new Fields( "char" ), new Fields( "num" ), new Fields( "average" ), 2 );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 5, 2, Pattern.compile( "^\\w+\\s[\\d.]+$" ) );

    Tuple[] results = new Tuple[]{
      new Tuple( "a", (double) 6 / 2 ),
      new Tuple( "b", (double) 12 / 4 ),
      new Tuple( "c", (double) 10 / 4 ),
      new Tuple( "d", (double) 6 / 2 ),
      new Tuple( "e", (double) 5 / 1 ),
    };

    TupleEntryIterator iterator = flow.openSink();
    int count = 0;

    while( iterator.hasNext() )
      assertEquals( results[ count++ ], iterator.next().getTuple() );

    iterator.close();
    }

  @Test
  public void testAverageByNull() throws IOException
    {
    getPlatform().copyFromLocal( inputFileLhs );

    Tap source = getPlatform().getDelimitedFile( new Fields( "num", "char" ), " ", inputFileLhs );
    Tap sink = getPlatform().getDelimitedFile( new Fields( "char", "average" ), "\t",
      new Class[]{String.class, Double.TYPE}, getOutputPath( "averagenull" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "average" );

    ExpressionFunction function = new ExpressionFunction( Fields.ARGS, "3 == $0 ? null : $0", Integer.class );
    pipe = new Each( pipe, new Fields( "num" ), function, Fields.REPLACE );

    pipe = new AverageBy( pipe, new Fields( "char" ), new Fields( "num" ), new Fields( "average" ), AverageBy.Include.NO_NULLS, 2 );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 5, 2, Pattern.compile( "^\\w+\\s[\\d.]+$" ) );

    Tuple[] results = new Tuple[]{
      new Tuple( "a", (double) 6 / 2 ),
      new Tuple( "b", (double) 12 / 4 ),
      new Tuple( "c", (double) 7 / 3 ),
      new Tuple( "d", (double) 6 / 2 ),
      new Tuple( "e", (double) 5 / 1 ),
    };

    TupleEntryIterator iterator = flow.openSink();
    int count = 0;

    while( iterator.hasNext() )
      assertEquals( results[ count++ ], iterator.next().getTuple() );

    iterator.close();
    }

  @Test
  public void testAverageMerge() throws IOException
    {
    getPlatform().copyFromLocal( inputFileLhs );
    getPlatform().copyFromLocal( inputFileRhs );

    Tap lhs = getPlatform().getDelimitedFile( new Fields( "num", "char" ), " ", inputFileLhs );
    Tap rhs = getPlatform().getDelimitedFile( new Fields( "num", "char" ), " ", inputFileRhs );
    Tap sink = getPlatform().getDelimitedFile( new Fields( "char", "average" ), "\t",
      new Class[]{String.class, Double.TYPE}, getOutputPath( "mergeaverage" ), SinkMode.REPLACE );

    Pipe lhsPipe = new Pipe( "average-lhs" );
    Pipe rhsPipe = new Pipe( "average-rhs" );

    rhsPipe = new Each( rhsPipe, new Fields( "char" ), new ExpressionFunction( Fields.ARGS, "$0.toLowerCase()", String.class ), Fields.REPLACE );

    Pipe sumPipe = new AverageBy( Pipe.pipes( lhsPipe, rhsPipe ), new Fields( "char" ), new Fields( "num" ), new Fields( "average" ), 2 );

    Map<String, Tap> tapMap = Cascades.tapsMap( Pipe.pipes( lhsPipe, rhsPipe ), Tap.taps( lhs, rhs ) );

    Flow flow = getPlatform().getFlowConnector().connect( tapMap, sink, sumPipe );

    flow.complete();

    validateLength( flow, 5, 2, Pattern.compile( "^\\w+\\s[\\d.]+$" ) );

    Tuple[] results = new Tuple[]{
      new Tuple( "a", (double) 12 / 4 ),
      new Tuple( "b", (double) 24 / 8 ),
      new Tuple( "c", (double) 20 / 8 ),
      new Tuple( "d", (double) 12 / 4 ),
      new Tuple( "e", (double) 10 / 2 ),
    };

    TupleEntryIterator iterator = flow.openSink();
    int count = 0;

    while( iterator.hasNext() )
      assertEquals( results[ count++ ], iterator.next().getTuple() );

    iterator.close();
    }

  @Test
  public void testFirstBy() throws IOException
    {
    getPlatform().copyFromLocal( inputFileLhs );

    Tap source = getPlatform().getDelimitedFile( new Fields( "num", "char" ), " ", inputFileLhs );
    Tap sink = getPlatform().getDelimitedFile( new Fields( "num", "char" ), "\t",
      new Class[]{Integer.TYPE, String.class}, getOutputPath( "firstn" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "first" );

    Fields charFields = new Fields( "char" );
    charFields.setComparator( "char", Collections.reverseOrder() );

    pipe = new FirstBy( pipe, new Fields( "num" ), charFields, 2 );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    Tuple[] results = new Tuple[]{
      new Tuple( 1, "c" ),
      new Tuple( 2, "d" ),
      new Tuple( 3, "c" ),
      new Tuple( 4, "d" ),
      new Tuple( 5, "e" )
    };

    TupleEntryIterator iterator = flow.openSink();
    int count = 0;

    while( iterator.hasNext() )
      assertEquals( results[ count++ ], iterator.next().getTuple() );

    assertTrue( !iterator.hasNext() );

    iterator.close();
    }

  @Test
  public void testParallelAggregates() throws IOException
    {
    getPlatform().copyFromLocal( inputFileLhs );

    Tap source = getPlatform().getDelimitedFile( new Fields( "num", "char" ), " ", inputFileLhs );
    Tap sink = getPlatform().getDelimitedFile( new Fields( "char", "sum", "count", "average", "average2", "first" ), "\t",
      new Class[]{
        String.class,
        Integer.TYPE,
        Integer.TYPE,
        Double.TYPE,
        Double.TYPE,
        Integer.TYPE}, getOutputPath( "multi" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "multi" );

    Fields num = new Fields( "num" );

    num.setComparator( "num", Collections.reverseOrder() );

    SumBy sumPipe = new SumBy( num, new Fields( "sum" ), long.class );
    CountBy countPipe = new CountBy( new Fields( "count" ) );
    AverageBy averagePipe = new AverageBy( num, new Fields( "average" ) );
    AverageBy averagePipe2 = new AverageBy( num, new Fields( "average2" ) );
    FirstBy firstBy = new FirstBy( num, new Fields( "first" ) );

    pipe = new AggregateBy( "name", Pipe.pipes( pipe ), new Fields( "char" ), 2, sumPipe, countPipe, averagePipe, averagePipe2, firstBy );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 5, 6, Pattern.compile( "^\\w+\\s\\d+\\s\\d+\\s[\\d.]+\\s[\\d.]+\\s\\d+$" ) );

    Tuple[] results = new Tuple[]{
      new Tuple( "a", 6, 2, (double) 6 / 2, (double) 6 / 2, 5 ),
      new Tuple( "b", 12, 4, (double) 12 / 4, (double) 12 / 4, 5 ),
      new Tuple( "c", 10, 4, (double) 10 / 4, (double) 10 / 4, 4 ),
      new Tuple( "d", 6, 2, (double) 6 / 2, (double) 6 / 2, 4 ),
      new Tuple( "e", 5, 1, (double) 5 / 1, (double) 5 / 1, 5 )
    };

    TupleEntryIterator iterator = flow.openSink();
    int count = 0;

    while( iterator.hasNext() )
      assertEquals( results[ count++ ], iterator.next().getTuple() );

    iterator.close();
    }

  @Test
  public void testParallelAggregatesMerge() throws IOException
    {
    performParallelAggregatesMerge( false );
    }

  @Test
  public void testParallelAggregatesPriorMerge() throws IOException
    {
    performParallelAggregatesMerge( true );
    }

  private void performParallelAggregatesMerge( boolean priorMerge ) throws IOException
    {
    getPlatform().copyFromLocal( inputFileLhs );
    getPlatform().copyFromLocal( inputFileRhs );

    Tap lhs = getPlatform().getDelimitedFile( new Fields( "num", "char" ), " ", inputFileLhs );
    Tap rhs = getPlatform().getDelimitedFile( new Fields( "num", "char" ), " ", inputFileRhs );

    Tap sink = getPlatform().getDelimitedFile( new Fields( "char", "sum", "count", "average" ), "\t",
      new Class[]{
        String.class,
        Integer.TYPE,
        Integer.TYPE, Double.TYPE},
      getOutputPath( "multimerge+" + priorMerge ), SinkMode.REPLACE );

    Pipe lhsPipe = new Pipe( "multi-lhs" );
    Pipe rhsPipe = new Pipe( "multi-rhs" );

    rhsPipe = new Each( rhsPipe, new Fields( "char" ), new ExpressionFunction( Fields.ARGS, "$0.toLowerCase()", String.class ), Fields.REPLACE );

    SumBy sumPipe = new SumBy( new Fields( "num" ), new Fields( "sum" ), long.class );
    CountBy countPipe = new CountBy( new Fields( "count" ) );
    AverageBy averagePipe = new AverageBy( new Fields( "num" ), new Fields( "average" ) );

    Pipe pipe;
    if( priorMerge )
      {
      Merge merge = new Merge( Pipe.pipes( lhsPipe, rhsPipe ) );

      pipe = new AggregateBy( "name", merge, new Fields( "char" ), 2, sumPipe, countPipe, averagePipe );
      }
    else
      {
      pipe = new AggregateBy( "name", Pipe.pipes( lhsPipe, rhsPipe ), new Fields( "char" ), 2, sumPipe, countPipe, averagePipe );
      }

    Map<String, Tap> tapMap = Cascades.tapsMap( Pipe.pipes( lhsPipe, rhsPipe ), Tap.taps( lhs, rhs ) );

    Flow flow = getPlatform().getFlowConnector().connect( tapMap, sink, pipe );

    flow.complete();

    validateLength( flow, 5, 4, Pattern.compile( "^\\w+\\s\\d+\\s\\d+\\s[\\d.]+$" ) );

    Tuple[] results = new Tuple[]{
      new Tuple( "a", 12, 4, (double) 12 / 4 ),
      new Tuple( "b", 24, 8, (double) 24 / 8 ),
      new Tuple( "c", 20, 8, (double) 20 / 8 ),
      new Tuple( "d", 12, 4, (double) 12 / 4 ),
      new Tuple( "e", 10, 2, (double) 10 / 2 ),
    };

    TupleEntryIterator iterator = flow.openSink();
    int count = 0;

    while( iterator.hasNext() )
      assertEquals( results[ count++ ], iterator.next().getTuple() );

    iterator.close();
    }

  @Test
  public void testCountCount() throws IOException
    {
    getPlatform().copyFromLocal( inputFileLhs );

    Tap source = getPlatform().getDelimitedFile( new Fields( "num", "char" ), " ", inputFileLhs );
    Tap sink = getPlatform().getDelimitedFile( new Fields( "count", "count2" ), "\t",
      new Class[]{Integer.TYPE, Integer.TYPE}, getOutputPath( "countcount" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "count" );

    pipe = new CountBy( pipe, new Fields( "char" ), new Fields( "count" ), 2 );
    pipe = new CountBy( pipe, new Fields( "count" ), new Fields( "count2" ), 2 );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 3, 2, Pattern.compile( "^\\d+\\s\\d+$" ) );

    Tuple[] results = new Tuple[]{
      new Tuple( 1, 1 ),
      new Tuple( 2, 2 ),
      new Tuple( 4, 2 ),
    };

    TupleEntryIterator iterator = flow.openSink();
    int count = 0;

    while( iterator.hasNext() )
      assertEquals( results[ count++ ], iterator.next().getTuple() );

    iterator.close();
    }

  /**
   * Tests chained merge + AggregateBy, which really tests the complete() calls within the pipeline. when failing
   * the map side functor function will fail during a flush
   * <p/>
   * todo: push merge upstream
   *
   * @throws IOException
   */
  @Test
  public void testSameSourceMerge() throws IOException
    {
    getPlatform().copyFromLocal( inputFileLhs );

    Tap source = getPlatform().getDelimitedFile( new Fields( "num", "char" ), " ", inputFileLhs );
    Tap sink = getPlatform().getDelimitedFile( new Fields( "char", "count" ), "\t",
      new Class[]{String.class, Integer.TYPE}, getOutputPath( "samesourcemergecount" ), SinkMode.REPLACE );

    Pipe sourcePipe = new Pipe( "source" );
    Pipe lhsPipe = new Pipe( "count-lhs", sourcePipe );
    Pipe rhsPipe = new Pipe( "count-rhs", sourcePipe );

    // redundant, but illustrates case
    rhsPipe = new Each( rhsPipe, new Fields( "char" ), new ExpressionFunction( Fields.ARGS, "$0.toLowerCase()", String.class ), Fields.REPLACE );

    Pipe merge = new Merge( "first", lhsPipe, rhsPipe );
    Pipe countPipe = new CountBy( merge, new Fields( "char" ), new Fields( "count" ), 2 );

    lhsPipe = new Each( new Pipe( "lhs", countPipe ), new Identity() );
    rhsPipe = new Each( new Pipe( "rhs", countPipe ), new Identity() );

    merge = new Merge( "second", lhsPipe, rhsPipe );

    countPipe = new CountBy( merge, new Fields( "char" ), new Fields( "count" ), 2 );

    Map<String, Tap> tapMap = Cascades.tapsMap( sourcePipe, source );

    Flow flow = getPlatform().getFlowConnector().connect( tapMap, sink, countPipe );

//    flow.writeDOT( "pushmerge.dot" );

    flow.complete();

    validateLength( flow, 5, 2, Pattern.compile( "^\\w+\\s\\d+$" ) );

    Tuple[] results = new Tuple[]{
      new Tuple( "a", 2 ),
      new Tuple( "b", 2 ),
      new Tuple( "c", 2 ),
      new Tuple( "d", 2 ),
      new Tuple( "e", 2 ),
    };

    TupleEntryIterator iterator = flow.openSink();
    int count = 0;

    while( iterator.hasNext() )
      assertEquals( results[ count++ ], iterator.next().getTuple() );

    iterator.close();
    }

  /**
   * tests the case where a intermediate tap can be reached from a group from two paths resulting
   * in difficulty distinguishing the pipe pos the pipelining is coming down.
   *
   * @throws IOException
   */
  @Test
  public void testSameSourceMergeThreeWay() throws IOException
    {
    getPlatform().copyFromLocal( inputFileLhs );

    Tap source = getPlatform().getDelimitedFile( new Fields( "num", "char" ), " ", inputFileLhs );
    Tap sink = getPlatform().getDelimitedFile( new Fields( "char", "count" ), "\t",
      new Class[]{String.class, Integer.TYPE}, getOutputPath( "samesourcemergethreeway" ), SinkMode.REPLACE );

    Pipe sourcePipe = new Pipe( "source" );

    sourcePipe = new CountBy( sourcePipe, new Fields( "char" ), new Fields( "count" ), 2 );

    Pipe lhsPipe = new Pipe( "count-lhs", sourcePipe );

    lhsPipe = new CountBy( lhsPipe, new Fields( "char" ), new Fields( "count" ), 2 );

    Pipe rhsPipe = new Pipe( "count-rhs", sourcePipe );

    rhsPipe = new CountBy( rhsPipe, new Fields( "char" ), new Fields( "count" ), 2 );

//    sourcePipe = new Each( sourcePipe, new Debug("source", true ) );
//    rhsPipe = new Each( rhsPipe, new Debug( "rhs", true ) );

    Pipe groupPipe = new CoGroup( sourcePipe, new Fields( "char" ), rhsPipe, new Fields( "char" ), new Fields( "char", "num", "char2", "count" ) );

//    groupPipe = new Each( groupPipe, new Debug( "cogroup", true ) );

    groupPipe = new CoGroup( lhsPipe, new Fields( "char" ), groupPipe, new Fields( "char" ), new Fields( "char", "count", "char2", "num2", "char3", "count3" ) );

    Map<String, Tap> tapMap = Cascades.tapsMap( sourcePipe, source );

    Flow flow = getPlatform().getFlowConnector().connect( tapMap, sink, groupPipe );

    flow.complete();

    validateLength( flow, 5, 2, Pattern.compile( "^\\w+\\s\\d+$" ) );

    Tuple[] results = new Tuple[]{
      new Tuple( "a", 1 ),
      new Tuple( "b", 1 ),
      new Tuple( "c", 1 ),
      new Tuple( "d", 1 ),
      new Tuple( "e", 1 ),
    };

    TupleEntryIterator iterator = flow.openSink();
    int count = 0;

    while( iterator.hasNext() )
      assertEquals( results[ count++ ], iterator.next().getTuple() );

    iterator.close();
    }

  @Test
  public void testMinBy() throws IOException
    {
    getPlatform().copyFromLocal( inputFileLhs );

    Tap source = getPlatform().getDelimitedFile( new Fields( "num", "char" ), " ", inputFileLhs );
    Tap sink = getPlatform().getDelimitedFile( new Fields( "char", "min" ), "\t",
      new Class[]{String.class, Integer.TYPE}, getOutputPath( "minby" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "min" );

    pipe = new MinBy( pipe, new Fields( "char" ), new Fields( "num" ), new Fields( "min" ), 2 );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 5, 2, Pattern.compile( "^\\w+\\s\\d+$" ) );

    Tuple[] results = new Tuple[]{
      new Tuple( "a", 1 ),
      new Tuple( "b", 1 ),
      new Tuple( "c", 1 ),
      new Tuple( "d", 2 ),
      new Tuple( "e", 5 ),
    };

    TupleEntryIterator iterator = flow.openSink();
    int count = 0;

    while( iterator.hasNext() )
      assertEquals( results[ count++ ], iterator.next().getTuple() );

    iterator.close();
    }

  @Test
  public void testMinByString() throws IOException
    {
    getPlatform().copyFromLocal( inputFileLhs );

    Tap source = getPlatform().getDelimitedFile( new Fields( "num", "char" ), " ", inputFileLhs );
    Tap sink = getPlatform().getDelimitedFile( new Fields( "num", "min" ), "\t",
      new Class[]{Integer.TYPE, String.class}, getOutputPath( "minbystring" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "max" );

    pipe = new MinBy( pipe, new Fields( "num" ), new Fields( "char" ), new Fields( "min" ), 2 );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 5, 2, Pattern.compile( "^\\d+\\s\\w+$" ) );

    Tuple[] results = new Tuple[]{
      new Tuple( 1, "a" ),
      new Tuple( 2, "b" ),
      new Tuple( 3, "c" ),
      new Tuple( 4, "b" ),
      new Tuple( 5, "a" ),
    };

    TupleEntryIterator iterator = flow.openSink();
    int count = 0;

    while( iterator.hasNext() )
      assertEquals( results[ count++ ], iterator.next().getTuple() );

    iterator.close();
    }

  @Test
  public void testMaxBy() throws IOException
    {
    getPlatform().copyFromLocal( inputFileLhs );

    Tap source = getPlatform().getDelimitedFile( new Fields( "num", "char" ), " ", inputFileLhs );
    Tap sink = getPlatform().getDelimitedFile( new Fields( "char", "max" ), "\t",
      new Class[]{String.class, Integer.TYPE}, getOutputPath( "maxby" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "max" );

    pipe = new MaxBy( pipe, new Fields( "char" ), new Fields( "num" ), new Fields( "max" ), 2 );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 5, 2, Pattern.compile( "^\\w+\\s\\d+$" ) );

    Tuple[] results = new Tuple[]{
      new Tuple( "a", 5 ),
      new Tuple( "b", 5 ),
      new Tuple( "c", 4 ),
      new Tuple( "d", 4 ),
      new Tuple( "e", 5 ),
    };

    TupleEntryIterator iterator = flow.openSink();
    int count = 0;

    while( iterator.hasNext() )
      assertEquals( results[ count++ ], iterator.next().getTuple() );

    iterator.close();
    }

  @Test
  public void testMaxByString() throws IOException
    {
    getPlatform().copyFromLocal( inputFileLhs );

    Tap source = getPlatform().getDelimitedFile( new Fields( "num", "char" ), " ", inputFileLhs );
    Tap sink = getPlatform().getDelimitedFile( new Fields( "num", "max" ), "\t",
      new Class[]{Integer.TYPE, String.class}, getOutputPath( "maxbystring" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "max" );

    pipe = new MaxBy( pipe, new Fields( "num" ), new Fields( "char" ), new Fields( "max" ), 2 );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 5, 2, Pattern.compile( "^\\d+\\s\\w+$" ) );

    Tuple[] results = new Tuple[]{
      new Tuple( 1, "c" ),
      new Tuple( 2, "d" ),
      new Tuple( 3, "c" ),
      new Tuple( 4, "d" ),
      new Tuple( 5, "e" ),
    };

    TupleEntryIterator iterator = flow.openSink();
    int count = 0;

    while( iterator.hasNext() )
      assertEquals( results[ count++ ], iterator.next().getTuple() );

    iterator.close();
    }
  }
