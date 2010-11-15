/*
 * Copyright (c) 2007-2010 Concurrent, Inc. All Rights Reserved.
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

package cascading.pipe.assembly;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.regex.Pattern;

import cascading.ClusterTestCase;
import cascading.cascade.Cascades;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.Function;
import cascading.operation.expression.ExpressionFunction;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.TextDelimited;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryIterator;

/**
 *
 */
public class AssemblyHelpersTest extends ClusterTestCase
  {
  String inputFileUpper = "build/test/data/upper.txt";
  String inputFileLower = "build/test/data/lower.txt";
  String inputFileLhs = "build/test/data/lhs.txt";
  String inputFileRhs = "build/test/data/rhs.txt";

  String outputPath = "build/test/output/assembly/";

  public AssemblyHelpersTest()
    {
    super( "assembly helper tests", false );
    }

  public void testCoerce() throws IOException
    {
    if( !new File( inputFileLower ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileLower );

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileLower );
    Tap sink = new Hfs( new TextLine( new Fields( "line" ), new Fields( "num", "char" ) ), outputPath + "/coerce", true );

    Pipe pipe = new Pipe( "coerce" );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );
    pipe = new Each( pipe, new Fields( "line" ), splitter );

    pipe = new Coerce( pipe, new Fields( "num" ), Integer.class );

    Flow flow = new FlowConnector( getProperties() ).connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 5, 1, Pattern.compile( "^\\d+\\s\\w+$" ) );
    }

  public void testShapeNarrow() throws IOException
    {
    if( !new File( inputFileLower ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileLower );

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileLower );
    Tap sink = new Hfs( new TextLine( new Fields( "num" ), new Fields( "num" ) ), outputPath + "/shapenarrow", true );

    Pipe pipe = new Pipe( "shape" );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );
    pipe = new Each( pipe, new Fields( "line" ), splitter );

    pipe = new Shape( pipe, new Fields( "num" ) );

    Flow flow = new FlowConnector( getProperties() ).connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 5, 1, Pattern.compile( "^\\d+$" ) );
    }

  public void testRenameNamed() throws IOException
    {
    if( !new File( inputFileLower ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileLower );

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileLower );
    Tap sink = new Hfs( new TextLine( new Fields( "line" ), new Fields( "item", "element" ) ), outputPath + "/renameall", true );

    Pipe pipe = new Pipe( "shape" );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );
    pipe = new Each( pipe, new Fields( "line" ), splitter );

    pipe = new Rename( pipe, new Fields( "num", "char" ), new Fields( "item", "element" ) );

    Flow flow = new FlowConnector( getProperties() ).connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 5, 1, Pattern.compile( "^\\d+\\s\\w+$" ) );
    }

  public void testRenameAll() throws IOException
    {
    if( !new File( inputFileLower ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileLower );

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileLower );
    Tap sink = new Hfs( new TextLine( new Fields( "line" ), new Fields( "item", "element" ) ), outputPath + "/renameall", true );

    Pipe pipe = new Pipe( "shape" );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );
    pipe = new Each( pipe, new Fields( "line" ), splitter );

    pipe = new Rename( pipe, Fields.ALL, new Fields( "item", "element" ) );

    Flow flow = new FlowConnector( getProperties() ).connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 5, 1, Pattern.compile( "^\\d+\\s\\w+$" ) );
    }

  public void testRenameNarrow() throws IOException
    {
    if( !new File( inputFileLower ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileLower );

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileLower );
    Tap sink = new Hfs( new TextLine( new Fields( "item" ), new Fields( "char", "item" ) ), outputPath + "/renamenarrow", true );

    Pipe pipe = new Pipe( "shape" );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );
    pipe = new Each( pipe, new Fields( "line" ), splitter );

    pipe = new Rename( pipe, new Fields( "num" ), new Fields( "item" ) );

    Flow flow = new FlowConnector( getProperties() ).connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 5, 1, Pattern.compile( "^\\w+\\s\\d+$" ) );
    }

  public void testUnique() throws IOException
    {
    if( !new File( inputFileLhs ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileLhs );

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileLhs );
    Tap sink = new Hfs( new TextLine( new Fields( "item" ), new Fields( "num", "char" ) ), outputPath + "/unique", true );

    Pipe pipe = new Pipe( "shape" );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );
    pipe = new Each( pipe, new Fields( "line" ), splitter );

    pipe = new Unique( pipe, new Fields( "num" ) );

    Flow flow = new FlowConnector( getProperties() ).connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 5, 1, Pattern.compile( "^\\d+\\s\\w+$" ) );
    }

  public void testUniqueMerge() throws IOException
    {
    if( !new File( inputFileLhs ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileLhs );
    copyFromLocal( inputFileRhs );

    Tap sourceLhs = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileLhs );
    Tap sourceRhs = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileRhs );
    Tap sink = new Hfs( new TextLine( new Fields( "item" ), new Fields( "num", "char" ) ), outputPath + "/unique", true );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );
    Pipe lhsPipe = new Pipe( "lhs" );
    lhsPipe = new Each( lhsPipe, new Fields( "line" ), splitter );

    Pipe rhsPipe = new Pipe( "rhs" );
    rhsPipe = new Each( rhsPipe, new Fields( "line" ), splitter );

    Pipe pipe = new Unique( Pipe.pipes( lhsPipe, rhsPipe ), new Fields( "num" ) );

    Map<String, Tap> sources = Cascades.tapsMap( Pipe.pipes( lhsPipe, rhsPipe ), Tap.taps( sourceLhs, sourceRhs ) );

    Flow flow = new FlowConnector( getProperties() ).connect( sources, sink, pipe );

    flow.complete();

    validateLength( flow, 5, 1, Pattern.compile( "^\\d+\\s\\w+$" ) );
    }

  public void testCount() throws IOException
    {
    if( !new File( inputFileLhs ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileLhs );

    Tap source = new Hfs( new TextDelimited( new Fields( "num", "char" ), " " ), inputFileLhs );
    Tap sink = new Hfs( new TextDelimited( new Fields( "char", "count" ), "\t", new Class[]{String.class,
                                                                                            Integer.TYPE} ), outputPath + "/count", SinkMode.REPLACE );

    Pipe pipe = new Pipe( "count" );

    pipe = new CountBy( pipe, new Fields( "char" ), new Fields( "count" ), 2 );

    Flow flow = new FlowConnector( getProperties() ).connect( source, sink, pipe );

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

  public void testCountMerge() throws IOException
    {
    if( !new File( inputFileLhs ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileLhs );
    copyFromLocal( inputFileRhs );

    Tap lhs = new Hfs( new TextDelimited( new Fields( "num", "char" ), " " ), inputFileLhs );
    Tap rhs = new Hfs( new TextDelimited( new Fields( "num", "char" ), " " ), inputFileRhs );
    Tap sink = new Hfs( new TextDelimited( new Fields( "char", "count" ), "\t", new Class[]{String.class,
                                                                                            Integer.TYPE} ), outputPath + "/mergecount", SinkMode.REPLACE );

    Pipe lhsPipe = new Pipe( "count-lhs" );
    Pipe rhsPipe = new Pipe( "count-rhs" );

    rhsPipe = new Each( rhsPipe, new Fields( "char" ), new ExpressionFunction( Fields.ARGS, "$0.toLowerCase()", String.class ), Fields.REPLACE );

    Pipe countPipe = new CountBy( Pipe.pipes( lhsPipe, rhsPipe ), new Fields( "char" ), new Fields( "count" ), 2 );

    Map<String, Tap> tapMap = Cascades.tapsMap( Pipe.pipes( lhsPipe, rhsPipe ), Tap.taps( lhs, rhs ) );

    Flow flow = new FlowConnector( getProperties() ).connect( tapMap, sink, countPipe );

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

  public void testSum() throws IOException
    {
    if( !new File( inputFileLhs ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileLhs );

    Tap source = new Hfs( new TextDelimited( new Fields( "num", "char" ), " " ), inputFileLhs );
    Tap sink = new Hfs( new TextDelimited( new Fields( "char", "sum" ), "\t", new Class[]{String.class,
                                                                                          Integer.TYPE} ), outputPath + "/sum", SinkMode.REPLACE );

    Pipe pipe = new Pipe( "sum" );

    pipe = new SumBy( pipe, new Fields( "char" ), new Fields( "num" ), new Fields( "sum" ), long.class, 2 );

    Flow flow = new FlowConnector( getProperties() ).connect( source, sink, pipe );

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

  public void testSumMerge() throws IOException
    {
    if( !new File( inputFileLhs ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileLhs );
    copyFromLocal( inputFileRhs );

    Tap lhs = new Hfs( new TextDelimited( new Fields( "num", "char" ), " " ), inputFileLhs );
    Tap rhs = new Hfs( new TextDelimited( new Fields( "num", "char" ), " " ), inputFileRhs );
    Tap sink = new Hfs( new TextDelimited( new Fields( "char", "sum" ), "\t", new Class[]{String.class,
                                                                                          Integer.TYPE} ), outputPath + "/mergesum", SinkMode.REPLACE );

    Pipe lhsPipe = new Pipe( "sum-lhs" );
    Pipe rhsPipe = new Pipe( "sum-rhs" );

    rhsPipe = new Each( rhsPipe, new Fields( "char" ), new ExpressionFunction( Fields.ARGS, "$0.toLowerCase()", String.class ), Fields.REPLACE );

    Pipe sumPipe = new SumBy( Pipe.pipes( lhsPipe, rhsPipe ), new Fields( "char" ), new Fields( "num" ), new Fields( "sum" ), long.class, 2 );

    Map<String, Tap> tapMap = Cascades.tapsMap( Pipe.pipes( lhsPipe, rhsPipe ), Tap.taps( lhs, rhs ) );

    Flow flow = new FlowConnector( getProperties() ).connect( tapMap, sink, sumPipe );

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

  public void testAverage() throws IOException
    {
    if( !new File( inputFileLhs ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileLhs );

    Tap source = new Hfs( new TextDelimited( new Fields( "num", "char" ), " " ), inputFileLhs );
    Tap sink = new Hfs( new TextDelimited( new Fields( "char", "average" ), "\t", new Class[]{String.class,
                                                                                              Double.TYPE} ), outputPath + "/average", SinkMode.REPLACE );

    Pipe pipe = new Pipe( "average" );

    pipe = new AverageBy( pipe, new Fields( "char" ), new Fields( "num" ), new Fields( "average" ), 2 );

    Flow flow = new FlowConnector( getProperties() ).connect( source, sink, pipe );

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

  public void testAverageMerge() throws IOException
    {
    if( !new File( inputFileLhs ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileLhs );
    copyFromLocal( inputFileRhs );

    Tap lhs = new Hfs( new TextDelimited( new Fields( "num", "char" ), " " ), inputFileLhs );
    Tap rhs = new Hfs( new TextDelimited( new Fields( "num", "char" ), " " ), inputFileRhs );
    Tap sink = new Hfs( new TextDelimited( new Fields( "char", "average" ), "\t", new Class[]{String.class,
                                                                                              Double.TYPE} ), outputPath + "/mergeaverage", SinkMode.REPLACE );

    Pipe lhsPipe = new Pipe( "average-lhs" );
    Pipe rhsPipe = new Pipe( "average-rhs" );

    rhsPipe = new Each( rhsPipe, new Fields( "char" ), new ExpressionFunction( Fields.ARGS, "$0.toLowerCase()", String.class ), Fields.REPLACE );

    Pipe sumPipe = new AverageBy( Pipe.pipes( lhsPipe, rhsPipe ), new Fields( "char" ), new Fields( "num" ), new Fields( "average" ), 2 );

    Map<String, Tap> tapMap = Cascades.tapsMap( Pipe.pipes( lhsPipe, rhsPipe ), Tap.taps( lhs, rhs ) );

    Flow flow = new FlowConnector( getProperties() ).connect( tapMap, sink, sumPipe );

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

  public void testParallelAggregates() throws IOException
    {
    if( !new File( inputFileLhs ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileLhs );

    Tap source = new Hfs( new TextDelimited( new Fields( "num", "char" ), " " ), inputFileLhs );
    Tap sink = new Hfs( new TextDelimited( new Fields( "char", "sum", "count", "average" ), "\t", new Class[]{
      String.class,
      Integer.TYPE,
      Integer.TYPE,
      Double.TYPE} ), outputPath + "/multi", SinkMode.REPLACE );

    Pipe pipe = new Pipe( "multi" );

    SumBy sumPipe = new SumBy( new Fields( "num" ), new Fields( "sum" ), long.class );
    CountBy countPipe = new CountBy( new Fields( "count" ) );
    AverageBy averagePipe = new AverageBy( new Fields( "num" ), new Fields( "average" ) );

    pipe = new AggregateBy( "name", Pipe.pipes( pipe ), new Fields( "char" ), 2, sumPipe, countPipe, averagePipe );

    Flow flow = new FlowConnector( getProperties() ).connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 5, 4, Pattern.compile( "^\\w+\\s\\d+\\s\\d+\\s[\\d.]+$" ) );

    Tuple[] results = new Tuple[]{
      new Tuple( "a", 6, 2, (double) 6 / 2 ),
      new Tuple( "b", 12, 4, (double) 12 / 4 ),
      new Tuple( "c", 10, 4, (double) 10 / 4 ),
      new Tuple( "d", 6, 2, (double) 6 / 2 ),
      new Tuple( "e", 5, 1, (double) 5 / 1 ),
    };

    TupleEntryIterator iterator = flow.openSink();
    int count = 0;

    while( iterator.hasNext() )
      assertEquals( results[ count++ ], iterator.next().getTuple() );

    iterator.close();
    }

  public void testParallelAggregatesMerge() throws IOException
    {
    if( !new File( inputFileLhs ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileLhs );
    copyFromLocal( inputFileRhs );

    Tap lhs = new Hfs( new TextDelimited( new Fields( "num", "char" ), " " ), inputFileLhs );
    Tap rhs = new Hfs( new TextDelimited( new Fields( "num", "char" ), " " ), inputFileRhs );

    Tap sink = new Hfs( new TextDelimited( new Fields( "char", "sum", "count", "average" ), "\t", new Class[]{
      String.class,
      Integer.TYPE,
      Integer.TYPE, Double.TYPE} ),
      outputPath + "/multimerge", SinkMode.REPLACE );

    Pipe lhsPipe = new Pipe( "multi-lhs" );
    Pipe rhsPipe = new Pipe( "multi-rhs" );

    rhsPipe = new Each( rhsPipe, new Fields( "char" ), new ExpressionFunction( Fields.ARGS, "$0.toLowerCase()", String.class ), Fields.REPLACE );

    SumBy sumPipe = new SumBy( new Fields( "num" ), new Fields( "sum" ), long.class );
    CountBy countPipe = new CountBy( new Fields( "count" ) );
    AverageBy averagePipe = new AverageBy( new Fields( "num" ), new Fields( "average" ) );

    Pipe pipe = new AggregateBy( "name", Pipe.pipes( lhsPipe, rhsPipe ), new Fields( "char" ), 2, sumPipe, countPipe, averagePipe );

    Map<String, Tap> tapMap = Cascades.tapsMap( Pipe.pipes( lhsPipe, rhsPipe ), Tap.taps( lhs, rhs ) );

    Flow flow = new FlowConnector( getProperties() ).connect( tapMap, sink, pipe );

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
  }
