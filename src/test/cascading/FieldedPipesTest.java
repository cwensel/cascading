/*
 * Copyright (c) 2007-2008 Concurrent, Inc. All Rights Reserved.
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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import cascading.cascade.Cascades;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowException;
import cascading.operation.Filter;
import cascading.operation.Function;
import cascading.operation.Identity;
import cascading.operation.aggregator.Count;
import cascading.operation.aggregator.First;
import cascading.operation.expression.ExpressionFunction;
import cascading.operation.generator.UnGroup;
import cascading.operation.regex.RegexFilter;
import cascading.operation.regex.RegexParser;
import cascading.operation.regex.RegexSplitter;
import cascading.operation.regex.Regexes;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.Group;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.cogroup.InnerJoin;
import cascading.pipe.cogroup.LeftJoin;
import cascading.pipe.cogroup.MixedJoin;
import cascading.pipe.cogroup.OuterJoin;
import cascading.pipe.cogroup.RightJoin;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.MultiTap;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleIterator;

/** @version $Id: //depot/calku/cascading/src/test/cascading/FieldedPipesTest.java#4 $ */
public class FieldedPipesTest extends ClusterTestCase
  {
  String inputFileApache = "build/test/data/apache.10.txt";
  String inputFileIps = "build/test/data/ips.20.txt";
  String inputFileNums20 = "build/test/data/nums.20.txt";
  String inputFileNums10 = "build/test/data/nums.10.txt";
  String inputFileCritics = "build/test/data/critics.txt";

  String inputFileUpper = "build/test/data/upper.txt";
  String inputFileLower = "build/test/data/lower.txt";
  String inputFileLowerOffset = "build/test/data/lower-offset.txt";
  String inputFileJoined = "build/test/data/lower+upper.txt";

  String inputFileLhs = "build/test/data/lhs.txt";
  String inputFileRhs = "build/test/data/rhs.txt";
  String inputFileCross = "build/test/data/lhs+rhs-cross.txt";

  String outputPath = "build/test/output/fields/";

  public FieldedPipesTest()
    {
    super( "fielded pipes", true ); // leave cluster testing enabled
    }

  public void testSimpleGroup() throws Exception
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileApache );

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileApache );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    pipe = new Group( pipe, new Fields( "ip" ) );

    pipe = new Every( pipe, new Count(), new Fields( "ip", "count" ) );

    Tap sink = new Hfs( new TextLine(), outputPath + "/simple", true );

    Flow flow = new FlowConnector( getProperties() ).connect( source, sink, pipe );

//    flow.writeDOT( "groupcount.dot" );

    flow.complete();

    validateLength( flow.openSource(), 10 ); // validate source, this once, as a sanity check
    validateLength( flow, 8, null );
    }

  public void testSimpleChain() throws Exception
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileApache );

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileApache );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    pipe = new Group( pipe, new Fields( "ip" ) );

    pipe = new Every( pipe, new Count( new Fields( "count1" ) ) );
    pipe = new Every( pipe, new Count( new Fields( "count2" ) ) );

    Tap sink = new Hfs( new TextLine(), outputPath + "/simplechain", true );

    Flow flow = new FlowConnector( getProperties() ).connect( source, sink, pipe );

//    flow.writeDOT( "chainedevery.dot" );

    flow.complete();

    validateLength( flow, 8, null );
    }

  public void testChainEndingWithEach() throws Exception
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileApache );

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileApache );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    pipe = new Group( pipe, new Fields( "ip" ) );

    pipe = new Every( pipe, new Count( new Fields( "count1" ) ) );
    pipe = new Every( pipe, new Count( new Fields( "count2" ) ) );

    pipe = new Each( pipe, new Fields( "count1", "count2" ), new ExpressionFunction( new Fields( "sum" ), "count1 + count2", int.class, int.class ), Fields.ALL );
    Tap sink = new Hfs( new TextLine(), outputPath + "/chaineach", true );

    Flow flow = new FlowConnector( getProperties() ).connect( source, sink, pipe );

//    flow.writeDOT( "chainedevery.dot" );

    flow.complete();

    validateLength( flow, 8, null );
    }

  // also tests the RegexSplitter
  public void testNoGroup() throws Exception
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileApache );

    Tap source = new Hfs( new TextLine( new Fields( "line" ) ), inputFileApache );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new RegexSplitter( "\\s+" ), new Fields( 1 ) );

    Tap sink = new Hfs( new TextLine( 1 ), outputPath + "/simplesplit", true );

    Flow flow = new FlowConnector( getProperties() ).connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 10, null );

    TupleIterator iterator = flow.openSink();

    assertEquals( "not equal: tuple.get(1)", "75.185.76.245", iterator.next().get( 1 ) );

    iterator.close();
    }

  public void testSimpleMerge() throws Exception
    {
    if( !new File( inputFileLower ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileLower );
    copyFromLocal( inputFileUpper );

    Tap sourceLower = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileLower );
    Tap sourceUpper = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileUpper );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    // using null pos so all fields are written
    Tap sink = new Hfs( new TextLine(), outputPath + "/complex/merge/", true );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );

    Pipe splice = new Group( "merge", Pipe.pipes( pipeLower, pipeUpper ), new Fields( "num" ), null, false );

    Flow flow = new FlowConnector( getProperties() ).connect( sources, sink, splice );

    flow.complete();

    validateLength( flow, 10, null );

    TupleIterator iterator = flow.openSink();

    assertEquals( "not equal: tuple.get(1)", "1\ta", iterator.next().get( 1 ) );
    assertEquals( "not equal: tuple.get(1)", "1\tA", iterator.next().get( 1 ) );

    iterator.close();
    }

  public void testCoGroup() throws Exception
    {
    if( !new File( inputFileLower ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileLower );
    copyFromLocal( inputFileUpper );

    Tap sourceLower = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileLower );
    Tap sourceUpper = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileUpper );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    // using null pos so all fields are written
    Tap sink = new Hfs( new TextLine(), outputPath + "/complex/cogroup/", true );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );

    Pipe splice = new CoGroup( pipeLower, new Fields( "num" ), pipeUpper, new Fields( "num" ), Fields.size( 4 ) );

    Flow countFlow = new FlowConnector( getProperties() ).connect( sources, sink, splice );

//    countFlow.writeDOT( "cogroup.dot" );
//    System.out.println( "countFlow =\n" + countFlow );

    countFlow.complete();

    validateLength( countFlow, 5, null );

    TupleIterator iterator = countFlow.openSink();

    assertEquals( "not equal: tuple.get(1)", "1\ta\t1\tA", iterator.next().get( 1 ) );
    assertEquals( "not equal: tuple.get(1)", "2\tb\t2\tB", iterator.next().get( 1 ) );

    iterator.close();
    }

  /**
   * Method testCoGroupAfterEvery tests that a tmp tap is inserted after the Every in the cogroup join
   *
   * @throws Exception when
   */
  public void testCoGroupAfterEvery() throws Exception
    {
    if( !new File( inputFileLower ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileLower );
    copyFromLocal( inputFileUpper );

    Tap sourceLower = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileLower );
    Tap sourceUpper = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileUpper );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    // using null pos so all fields are written
    Tap sink = new Hfs( new TextLine(), outputPath + "/complex/cogroup/", true );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    pipeLower = new GroupBy( pipeLower, new Fields( "num" ) );
    pipeLower = new Every( pipeLower, new Fields( "char" ), new First(), Fields.ALL );

    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );
    pipeUpper = new GroupBy( pipeUpper, new Fields( "num" ) );
    pipeUpper = new Every( pipeUpper, new Fields( "char" ), new First(), Fields.ALL );

    Pipe splice = new CoGroup( pipeLower, new Fields( "num" ), pipeUpper, new Fields( "num" ), Fields.size( 4 ) );

    Flow countFlow = null;

    try
      {
      countFlow = new FlowConnector( getProperties() ).connect( sources, sink, splice );
      }
    catch( FlowException exception )
      {
      exception.writeDOT( "cogroupedevery.dot" );
      throw exception;
      }

//    countFlow.writeDOT( "cogroup.dot" );
//    System.out.println( "countFlow =\n" + countFlow );

    countFlow.complete();

    validateLength( countFlow, 5, null );

    TupleIterator iterator = countFlow.openSink();

    assertEquals( "not equal: tuple.get(1)", "1\ta\t1\tA", iterator.next().get( 1 ) );
    assertEquals( "not equal: tuple.get(1)", "2\tb\t2\tB", iterator.next().get( 1 ) );

    iterator.close();
    }


  /**
   * 1 a
   * 5 b
   * 6 c
   * 5 b
   * 5 e
   * <p/>
   * 1 A
   * 2 B
   * 3 C
   * 4 D
   * 5 E
   * <p/>
   * 1	a	1	A
   * 5	b	5	E
   * 5	e	5	E
   *
   * @throws Exception
   */
  public void testCoGroupInner() throws Exception
    {
    if( !new File( inputFileLowerOffset ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileLowerOffset );
    copyFromLocal( inputFileUpper );

    Tap sourceLower = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileLowerOffset );
    Tap sourceUpper = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileUpper );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    // using null pos so all fields are written
    Tap sink = new Hfs( new TextLine(), outputPath + "/complex/cogroupinner/", true );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );

    Pipe splice = new CoGroup( pipeLower, new Fields( "num" ), pipeUpper, new Fields( "num" ), Fields.size( 4 ) );

    Flow countFlow = new FlowConnector( getProperties() ).connect( sources, sink, splice );

//    countFlow.writeDOT( "cogroup.dot" );
//    System.out.println( "countFlow =\n" + countFlow );

    countFlow.complete();

    validateLength( countFlow, 3, null );

    TupleIterator iterator = countFlow.openSink();

    assertEquals( "not equal: tuple.get(1)", "1\ta\t1\tA", iterator.next().get( 1 ) );
    assertEquals( "not equal: tuple.get(1)", "5\tb\t5\tE", iterator.next().get( 1 ) );
    assertEquals( "not equal: tuple.get(1)", "5\te\t5\tE", iterator.next().get( 1 ) );

    iterator.close();
    }

  /**
   * 1 a
   * 5 b
   * 6 c
   * 5 b
   * 5 e
   * <p/>
   * 1 A
   * 2 B
   * 3 C
   * 4 D
   * 5 E
   * <p/>
   * 1	a	1	A
   * -  -   2   B
   * -  -   3   C
   * -  -   4   D
   * 5	b	5	E
   * 5	e	5	E
   * 6  c   -   -
   *
   * @throws Exception
   */
  public void testCoGroupOuter() throws Exception
    {
    if( !new File( inputFileLowerOffset ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileLowerOffset );
    copyFromLocal( inputFileUpper );

    Tap sourceLower = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileLowerOffset );
    Tap sourceUpper = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileUpper );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    // using null pos so all fields are written
    Tap sink = new Hfs( new TextLine(), outputPath + "/complex/cogroupouter/", true );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );

    Pipe splice = new CoGroup( pipeLower, new Fields( "num" ), pipeUpper, new Fields( "num" ), Fields.size( 4 ), new OuterJoin() );

    Flow countFlow = new FlowConnector( getProperties() ).connect( sources, sink, splice );

//    countFlow.writeDOT( "cogroup.dot" );
//    System.out.println( "countFlow =\n" + countFlow );

    countFlow.complete();

    validateLength( countFlow, 7, null );

    TupleIterator iterator = countFlow.openSink();

    assertEquals( "not equal: tuple.get(1)", "1\ta\t1\tA", iterator.next().get( 1 ) );
    assertEquals( "not equal: tuple.get(1)", "null\tnull\t2\tB", iterator.next().get( 1 ) );
    assertEquals( "not equal: tuple.get(1)", "null\tnull\t3\tC", iterator.next().get( 1 ) );
    assertEquals( "not equal: tuple.get(1)", "null\tnull\t4\tD", iterator.next().get( 1 ) );
    assertEquals( "not equal: tuple.get(1)", "5\tb\t5\tE", iterator.next().get( 1 ) );
    assertEquals( "not equal: tuple.get(1)", "5\te\t5\tE", iterator.next().get( 1 ) );
    assertEquals( "not equal: tuple.get(1)", "6\tc\tnull\tnull", iterator.next().get( 1 ) );

    iterator.close();
    }

  /**
   * 1 a
   * 5 b
   * 6 c
   * 5 b
   * 5 e
   * <p/>
   * 1 A
   * 2 B
   * 3 C
   * 4 D
   * 5 E
   * <p/>
   * 1	a	1	A
   * 5	b	5	E
   * 5	e	5	E
   * 6  c   -   -
   *
   * @throws Exception
   */
  public void testCoGroupInnerOuter() throws Exception
    {
    if( !new File( inputFileLowerOffset ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileLowerOffset );
    copyFromLocal( inputFileUpper );

    Tap sourceLower = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileLowerOffset );
    Tap sourceUpper = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileUpper );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    // using null pos so all fields are written
    Tap sink = new Hfs( new TextLine(), outputPath + "/complex/cogroupinnerouter/", true );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );

    Pipe splice = new CoGroup( pipeLower, new Fields( "num" ), pipeUpper, new Fields( "num" ), Fields.size( 4 ), new LeftJoin() );

    Flow countFlow = new FlowConnector( getProperties() ).connect( sources, sink, splice );

//    countFlow.writeDOT( "cogroup.dot" );
//    System.out.println( "countFlow =\n" + countFlow );

    countFlow.complete();

    validateLength( countFlow, 4, null );

    TupleIterator iterator = countFlow.openSink();

    assertEquals( "not equal: tuple.get(1)", "1\ta\t1\tA", iterator.next().get( 1 ) );
    assertEquals( "not equal: tuple.get(1)", "5\tb\t5\tE", iterator.next().get( 1 ) );
    assertEquals( "not equal: tuple.get(1)", "5\te\t5\tE", iterator.next().get( 1 ) );
    assertEquals( "not equal: tuple.get(1)", "6\tc\tnull\tnull", iterator.next().get( 1 ) );

    iterator.close();
    }

  /**
   * 1 a
   * 5 b
   * 6 c
   * 5 b
   * 5 e
   * <p/>
   * 1 A
   * 2 B
   * 3 C
   * 4 D
   * 5 E
   * <p/>
   * 1	a	1	A
   * -  -   2   B
   * -  -   3   C
   * -  -   4   D
   * 5	b	5	E
   * 5	e	5	E
   *
   * @throws Exception
   */
  public void testCoGroupOuterInner() throws Exception
    {
    if( !new File( inputFileLowerOffset ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileLowerOffset );
    copyFromLocal( inputFileUpper );

    Tap sourceLower = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileLowerOffset );
    Tap sourceUpper = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileUpper );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    // using null pos so all fields are written
    Tap sink = new Hfs( new TextLine(), outputPath + "/complex/cogroupouterinner/", true );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );

    Pipe splice = new CoGroup( pipeLower, new Fields( "num" ), pipeUpper, new Fields( "num" ), Fields.size( 4 ), new RightJoin() );

    Flow countFlow = new FlowConnector( getProperties() ).connect( sources, sink, splice );

//    countFlow.writeDOT( "cogroup.dot" );
//    System.out.println( "countFlow =\n" + countFlow );

    countFlow.complete();

    validateLength( countFlow, 6, null );

    TupleIterator iterator = countFlow.openSink();

    assertEquals( "not equal: tuple.get(1)", "1\ta\t1\tA", iterator.next().get( 1 ) );
    assertEquals( "not equal: tuple.get(1)", "null\tnull\t2\tB", iterator.next().get( 1 ) );
    assertEquals( "not equal: tuple.get(1)", "null\tnull\t3\tC", iterator.next().get( 1 ) );
    assertEquals( "not equal: tuple.get(1)", "null\tnull\t4\tD", iterator.next().get( 1 ) );
    assertEquals( "not equal: tuple.get(1)", "5\tb\t5\tE", iterator.next().get( 1 ) );
    assertEquals( "not equal: tuple.get(1)", "5\te\t5\tE", iterator.next().get( 1 ) );

    iterator.close();
    }

  /**
   * 1 a
   * 5 b
   * 6 c
   * 5 b
   * 5 e
   * <p/>
   * 1 A
   * 2 B
   * 3 C
   * 4 D
   * 5 E
   * <p/>
   * 1 a
   * 2 b
   * 3 c
   * 4 d
   * 5 e
   * <p/>
   * 1	a	1	A  1  a
   * -  -   2   B  2  b
   * -  -   3   C  3  c
   * -  -   4   D  4  d
   * 5	b	5   E  5  e
   * 5	e	5   E  5  e
   *
   * @throws Exception
   */
  public void testCoGroupMixed() throws Exception
    {
    if( !new File( inputFileLowerOffset ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileLowerOffset );
    copyFromLocal( inputFileLower );
    copyFromLocal( inputFileUpper );

    Tap sourceLowerOffset = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileLowerOffset );
    Tap sourceUpper = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileUpper );
    Tap sourceLower = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileLower );

    Map sources = new HashMap();

    sources.put( "loweroffset", sourceLowerOffset );
    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    // using null pos so all fields are written
    Tap sink = new Hfs( new TextLine(), outputPath + "/complex/cogroupmixed/", true );

    Pipe pipeLowerOffset = new Each( new Pipe( "loweroffset" ), new Fields( "line" ), splitter );
    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );


    Pipe[] pipes = Pipe.pipes( pipeLowerOffset, pipeUpper, pipeLower );
    Fields[] fields = Fields.fields( new Fields( "num" ), new Fields( "num" ), new Fields( "num" ) );

    MixedJoin join = new MixedJoin( new boolean[]{MixedJoin.OUTER, MixedJoin.INNER, MixedJoin.OUTER} );
    Pipe splice = new CoGroup( pipes, fields, Fields.size( 6 ), join );

    Flow countFlow = new FlowConnector( getProperties() ).connect( sources, sink, splice );

//    countFlow.writeDOT( "cogroup.dot" );
//    System.out.println( "countFlow =\n" + countFlow );

    countFlow.complete();

    validateLength( countFlow, 6, null );

    TupleIterator iterator = countFlow.openSink();

    assertEquals( "not equal: tuple.get(1)", "1\ta\t1\tA\t1\ta", iterator.next().get( 1 ) );
    assertEquals( "not equal: tuple.get(1)", "null\tnull\t2\tB\t2\tb", iterator.next().get( 1 ) );
    assertEquals( "not equal: tuple.get(1)", "null\tnull\t3\tC\t3\tc", iterator.next().get( 1 ) );
    assertEquals( "not equal: tuple.get(1)", "null\tnull\t4\tD\t4\td", iterator.next().get( 1 ) );
    assertEquals( "not equal: tuple.get(1)", "5\tb\t5\tE\t5\te", iterator.next().get( 1 ) );
    assertEquals( "not equal: tuple.get(1)", "5\te\t5\tE\t5\te", iterator.next().get( 1 ) );

    iterator.close();
    }

  public void testCoGroupDiffFields() throws Exception
    {
    if( !new File( inputFileLower ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileLower );
    copyFromLocal( inputFileUpper );

    Tap sourceLower = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileLower );
    Tap sourceUpper = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileUpper );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Function splitterLower = new RegexSplitter( new Fields( "numA", "lower" ), " " );
    Function splitterUpper = new RegexSplitter( new Fields( "numB", "upper" ), " " );

    // using null pos so all fields are written
    Tap sink = new Hfs( new TextLine(), outputPath + "/complex/cogroup/", true );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitterLower );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitterUpper );

    Pipe cogroup = new Group( pipeLower, new Fields( "numA" ), pipeUpper, new Fields( "numB" ) );

    Flow flow = new FlowConnector( getProperties() ).connect( sources, sink, cogroup );

//    System.out.println( "flow =\n" + flow );

    flow.complete();

    validateLength( flow, 5, null );

    TupleIterator iterator = flow.openSink();

    assertEquals( "not equal: tuple.get(1)", "1\ta\t1\tA", iterator.next().get( 1 ) );
    assertEquals( "not equal: tuple.get(1)", "2\tb\t2\tB", iterator.next().get( 1 ) );

    iterator.close();
    }

  public void testCoGroupGroupBy() throws Exception
    {
    if( !new File( inputFileLower ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileLower );
    copyFromLocal( inputFileUpper );

    Tap sourceLower = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileLower );
    Tap sourceUpper = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileUpper );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Function splitterLower = new RegexSplitter( new Fields( "numA", "lower" ), " " );
    Function splitterUpper = new RegexSplitter( new Fields( "numB", "upper" ), " " );

    // using null pos so all fields are written
    Tap sink = new Hfs( new TextLine(), outputPath + "/complex/cogroupgroupby/", true );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitterLower );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitterUpper );

    Pipe cogroup = new CoGroup( pipeLower, new Fields( "numA" ), pipeUpper, new Fields( "numB" ) );

    //cogroup = new Each( cogroup, new Identity() );

    Pipe groupby = new GroupBy( cogroup, new Fields( "numA" ) );

    Flow flow = new FlowConnector( getProperties() ).connect( sources, sink, groupby );

//    System.out.println( "flow =\n" + flow );

    flow.complete();

    validateLength( flow, 5, null );

    TupleIterator iterator = flow.openSink();

    assertEquals( "not equal: tuple.get(1)", "1\ta\t1\tA", iterator.next().get( 1 ) );
    assertEquals( "not equal: tuple.get(1)", "2\tb\t2\tB", iterator.next().get( 1 ) );

    iterator.close();
    }

  public void testCoGroupSamePipe() throws Exception
    {
    if( !new File( inputFileLower ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileLower );

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileLower );

    Map sources = new HashMap();

    sources.put( "lower", source );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    // using null pos so all fields are written
    Tap sink = new Hfs( new TextLine(), outputPath + "/complex/cogroup/", true );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );

    Pipe cogroup = new Group( pipeLower, new Fields( "num" ), 2, new Fields( "num1", "char1", "num2", "char2" ) );

    Flow flow = new FlowConnector( getProperties() ).connect( sources, sink, cogroup );

//    System.out.println( "flow =\n" + flow );

    flow.complete();

    validateLength( flow, 5, null );

    TupleIterator iterator = flow.openSink();

    assertEquals( "not equal: tuple.get(1)", "1\ta\t1\ta", iterator.next().get( 1 ) );
    assertEquals( "not equal: tuple.get(1)", "2\tb\t2\tb", iterator.next().get( 1 ) );

    iterator.close();
    }

  public void testCoGroupAroundCoGroup() throws Exception
    {
    if( !new File( inputFileLower ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileLower );
    copyFromLocal( inputFileUpper );

    Tap sourceLower = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileLower );
    Tap sourceUpper = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileUpper );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper1", sourceUpper );
    sources.put( "upper2", sourceUpper );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    // using null pos so all fields are written
    Tap sink = new Hfs( new TextLine(), outputPath + "/complex/cogroupacogroup/", true );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper1 = new Each( new Pipe( "upper1" ), new Fields( "line" ), splitter );
    Pipe pipeUpper2 = new Each( new Pipe( "upper2" ), new Fields( "line" ), splitter );

    Pipe splice1 = new CoGroup( pipeLower, new Fields( "num" ), pipeUpper1, new Fields( "num" ), new Fields( "num1", "char1", "num2", "char2" ) );

    splice1 = new Each( splice1, new Identity() );

    Pipe splice2 = new CoGroup( splice1, new Fields( "num1" ), pipeUpper2, new Fields( "num" ), new Fields( "num1", "char1", "num2", "char2", "num3", "char3" ) );

    Flow countFlow = new FlowConnector( getProperties() ).connect( sources, sink, splice2 );

//    countFlow.writeDOT( "cogroupcogroup.dot" );
//    System.out.println( "countFlow =\n" + countFlow );

    countFlow.complete();

    validateLength( countFlow, 5, null );

    TupleIterator iterator = countFlow.openSink();

    assertEquals( "not equal: tuple.get(1)", "1\ta\t1\tA\t1\tA", iterator.next().get( 1 ) );
    assertEquals( "not equal: tuple.get(1)", "2\tb\t2\tB\t2\tB", iterator.next().get( 1 ) );

    iterator.close();
    }

  public void testCoGroupAroundCoGroupWithout() throws Exception
    {
    runCoGroupAroundCoGroup( null, outputPath + "/complex/cogroupacogroupopt1/" );
    }

  public void testCoGroupAroundCoGroupWith() throws Exception
    {
    runCoGroupAroundCoGroup( TestTextLine.class, outputPath + "/complex/cogroupacogroupopt2/" );
    }

  private void runCoGroupAroundCoGroup( Class schemeClass, String stringPath ) throws IOException
    {
    if( !new File( inputFileNums10 ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileNums20 );
    copyFromLocal( inputFileNums10 );

    Tap source10 = new Hfs( new TestTextLine( new Fields( "num" ) ), inputFileNums10 );
    Tap source20 = new Hfs( new TestTextLine( new Fields( "num" ) ), inputFileNums20 );

    Map sources = new HashMap();

    sources.put( "source20", source20 );
    sources.put( "source101", source10 );
    sources.put( "soucre102", source10 );

    // using null pos so all fields are written
    Tap sink = new Hfs( new TextLine(), stringPath, true );

    Pipe pipeNum20 = new Pipe( "source20" );
    Pipe pipeNum101 = new Pipe( "source101" );
    Pipe pipeNum102 = new Pipe( "source101" );

    Pipe splice1 = new CoGroup( pipeNum20, new Fields( "num" ), pipeNum101, new Fields( "num" ), new Fields( "num1", "num2" ) );

    Pipe splice2 = new CoGroup( splice1, new Fields( "num1" ), pipeNum102, new Fields( "num" ), new Fields( "num1", "num2", "num3" ) );

    splice2 = new Each( splice2, new Identity() );

    Map<Object, Object> properties = getProperties();

    FlowConnector.setIntermediateSchemeClass( properties, schemeClass );

    FlowConnector flowConnector = new FlowConnector( properties );

    Flow flow = flowConnector.connect( sources, sink, splice2 );

//    flow.writeDOT( "cogroupcogroupwout.dot" );
//    System.out.println( "countFlow =\n" + countFlow );

    flow.complete();

    validateLength( flow, 10, null );

    TupleIterator iterator = flow.openSink();

    assertEquals( "not equal: tuple.get(1)", "1\t1\t1", iterator.next().get( 1 ) );
    assertEquals( "not equal: tuple.get(1)", "10\t10\t10", iterator.next().get( 1 ) );

    iterator.close();
    }


  public void testUnGroup() throws Exception
    {
    if( !new File( inputFileJoined ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileJoined );

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileJoined );
    Tap sink = new Hfs( new TextLine(), outputPath + "/ungrouped", true );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexSplitter( new Fields( "num", "lower", "upper" ) ) );

    pipe = new Each( pipe, new UnGroup( new Fields( "num", "char" ), new Fields( "num" ), Fields.fields( new Fields( "lower" ), new Fields( "upper" ) ) ) );

    Flow flow = new FlowConnector( getProperties() ).connect( source, sink, pipe );

//    flow.writeDOT( "ungroup.dot" );

    flow.complete();

    validateLength( flow, 10, null );
    }

  public void testFilter() throws Exception
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileApache );

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileApache );
    Tap sink = new Hfs( new TextLine(), outputPath + "/filter", true );

    Pipe pipe = new Pipe( "test" );

    Filter filter = new RegexFilter( "^68.*" );

    pipe = new Each( pipe, new Fields( "line" ), filter );

    Flow flow = new FlowConnector( getProperties() ).connect( source, sink, pipe );

//    flow.writeDOT( "flow.dot" );

    flow.complete();

    validateLength( flow, 3, null );
    }

  public void testFilterComplex() throws Exception
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileApache );

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileApache );
    Tap sink = new Hfs( new TextLine(), outputPath + "/filtercomplex", true );

    Pipe pipe = new Pipe( "test" );

//    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );
    pipe = new Each( pipe, new Fields( "line" ), Regexes.APACHE_COMMON_PARSER );

    pipe = new Each( pipe, new Fields( "method" ), new RegexFilter( "^POST" ) );
    pipe = new Each( pipe, new Fields( "method" ), new RegexFilter( "^POST" ) );

    pipe = new Each( pipe, new Fields( "method" ), new Identity( new Fields( "value" ) ), Fields.ALL );

    pipe = new Group( pipe, new Fields( "value" ) );

    pipe = new Every( pipe, new Count(), new Fields( "value", "count" ) );


    Flow flow = new FlowConnector( getProperties() ).connect( source, sink, pipe );

//    flow.writeDOT( "filter.dot" );

    flow.complete();

    validateLength( flow, 1, null );
    }

  public void testCross() throws Exception
    {
    if( !new File( inputFileLhs ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileLhs );
    copyFromLocal( inputFileRhs );

    Map sources = new HashMap();

    sources.put( "lhs", new Hfs( new TextLine(), inputFileLhs ) );
    sources.put( "rhs", new Hfs( new TextLine(), inputFileRhs ) );

    Pipe pipeLower = new Each( "lhs", new Fields( "line" ), new RegexSplitter( new Fields( "numLHS", "charLHS" ), " " ) );
    Pipe pipeUpper = new Each( "rhs", new Fields( "line" ), new RegexSplitter( new Fields( "numRHS", "charRHS" ), " " ) );

    Pipe cross = new Group( pipeLower, new Fields( "numLHS" ), pipeUpper, new Fields( "numRHS" ), new InnerJoin() );

    // using null pos so all fields are written
    Tap sink = new Hfs( new TextLine(), outputPath + "/complex/cross/", true );

    Flow flow = new FlowConnector( getProperties() ).connect( sources, sink, cross );

//    System.out.println( "flow =\n" + flow );

    flow.complete();

    validateLength( flow, 37, null );

    TupleIterator iterator = flow.openSink();

    assertEquals( "not equal: tuple.get(1)", "1\ta\t1\tA", iterator.next().get( 1 ) );
    assertEquals( "not equal: tuple.get(1)", "1\ta\t1\tB", iterator.next().get( 1 ) );

    iterator.close();
    }

  public void testSplit() throws Exception
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileApache );

    // 46 192

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileApache );
    Tap sink1 = new Hfs( new TextLine(), outputPath + "/split1", true );
    Tap sink2 = new Hfs( new TextLine(), outputPath + "/split2", true );

    Pipe pipe = new Pipe( "split" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexFilter( "^68.*" ) );

    Pipe left = new Each( new Pipe( "left", pipe ), new Fields( "line" ), new RegexFilter( ".*46.*" ) );
    Pipe right = new Each( new Pipe( "right", pipe ), new Fields( "line" ), new RegexFilter( ".*102.*" ) );

    Map sources = new HashMap();
    sources.put( "split", source );

    Map sinks = new HashMap();
    sinks.put( "left", sink1 );
    sinks.put( "right", sink2 );

    Flow flow = new FlowConnector( getProperties() ).connect( sources, sinks, left, right );

//    flow.writeDOT( "split.dot" );

    flow.complete();

    validateLength( flow, 1, "left" );
    validateLength( flow, 2, "right" );
    }

  /**
   * verifies not inserting Identity between groups works
   *
   * @throws Exception
   */
  public void testSplitOut() throws Exception
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileApache );

    Tap sourceLower = new Hfs( new TextLine( new Fields( "num", "line" ) ), inputFileApache );

    Map sources = new HashMap();

    sources.put( "lower1", sourceLower );

    // using null pos so all fields are written
    Tap sink1 = new Hfs( new TextLine(), outputPath + "/splitout1", true );
    Tap sink2 = new Hfs( new TextLine(), outputPath + "/splitout2", true );

    Map sinks = new HashMap();

    sinks.put( "output1", sink1 );
    sinks.put( "output2", sink2 );

    Pipe pipeLower1 = new Pipe( "lower1" );

    Pipe left = new GroupBy( "output1", pipeLower1, new Fields( 0 ) );
    Pipe right = new GroupBy( "output2", left, new Fields( 0 ) );

    Flow flow = new FlowConnector().connect( sources, sinks, Pipe.pipes( left, right ) );

//    flow.writeDOT( "splitout.dot" );

    flow.complete();

    validateLength( flow, 10, "output1" );
    validateLength( flow, 10, "output2" );
    }

  public void testSplitComplex() throws Exception
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileApache );

    // 46 192

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileApache );
    Tap sink1 = new Hfs( new TextLine(), outputPath + "/splitcomp1", true );
    Tap sink2 = new Hfs( new TextLine(), outputPath + "/splitcomp2", true );

    Pipe pipe = new Pipe( "split" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    pipe = new Group( pipe, new Fields( "ip" ) );

    pipe = new Every( pipe, new Fields( "ip" ), new Count(), new Fields( "ip", "count" ) );

    pipe = new Each( pipe, new Fields( "ip" ), new RegexFilter( "^68.*" ) );

    Pipe left = new Each( new Pipe( "left", pipe ), new Fields( "ip" ), new RegexFilter( ".*46.*" ) );

    Pipe right = new Each( new Pipe( "right", pipe ), new Fields( "ip" ), new RegexFilter( ".*102.*" ) );

    Map sources = Cascades.tapsMap( "split", source );
    Map sinks = Cascades.tapsMap( Pipe.pipes( left, right ), Tap.taps( sink1, sink2 ) );

    Flow flow = new FlowConnector( getProperties() ).connect( sources, sinks, left, right );

//    flow.writeDOT( "splitcomplex.dot" );

    flow.complete();

    validateLength( flow, 1, "left" );
    validateLength( flow, 1, "right" );
    }

  public void testConcatentation() throws Exception
    {
    if( !new File( inputFileLower ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileLower );
    copyFromLocal( inputFileUpper );

    Tap sourceLower = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileLower );
    Tap sourceUpper = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileUpper );

    Tap source = new MultiTap( sourceLower, sourceUpper );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    // using null pos so all fields are written
    Tap sink = new Hfs( new TextLine(), outputPath + "/complex/concat/", true );

    Pipe pipe = new Each( new Pipe( "concat" ), new Fields( "line" ), splitter );

    Pipe splice = new GroupBy( pipe, new Fields( "num" ) );

    Flow countFlow = new FlowConnector( getProperties() ).connect( source, sink, splice );

//    countFlow.writeDOT( "cogroup.dot" );
//    System.out.println( "countFlow =\n" + countFlow );

    countFlow.complete();

    validateLength( countFlow, 10, null );
    }
  }
