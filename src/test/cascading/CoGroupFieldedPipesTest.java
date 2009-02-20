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

package cascading;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.PlannerException;
import cascading.operation.Function;
import cascading.operation.Identity;
import cascading.operation.aggregator.First;
import cascading.operation.regex.RegexFilter;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.cogroup.LeftJoin;
import cascading.pipe.cogroup.MixedJoin;
import cascading.pipe.cogroup.OuterJoin;
import cascading.pipe.cogroup.RightJoin;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryIterator;

/** @version $Id: //depot/calku/cascading/src/test/cascading/FieldedPipesTest.java#4 $ */
public class CoGroupFieldedPipesTest extends ClusterTestCase
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

  public CoGroupFieldedPipesTest()
    {
    super( "fielded pipes", true, 4, 1 ); // leave cluster testing enabled
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

    TupleEntryIterator iterator = countFlow.openSink();

    assertEquals( "not equal: tuple.get(1)", "1\ta\t1\tA", iterator.next().get( 1 ) );
    assertEquals( "not equal: tuple.get(1)", "2\tb\t2\tB", iterator.next().get( 1 ) );

    iterator.close();
    }

  public void testCoGroupWithUnkowns() throws Exception
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

    Function splitter = new RegexSplitter( Fields.UNKNOWN, " " );

    // using null pos so all fields are written
    Tap sink = new Hfs( new TextLine(), outputPath + "/complex/cogroup/", true );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );

    Pipe splice = new CoGroup( pipeLower, new Fields( 0 ), pipeUpper, new Fields( 0 ), Fields.size( 4 ) );

    Flow countFlow = new FlowConnector( getProperties() ).connect( sources, sink, splice );

//    countFlow.writeDOT( "cogroup.dot" );
//    System.out.println( "countFlow =\n" + countFlow );

    countFlow.complete();

    validateLength( countFlow, 5, null );

    TupleEntryIterator iterator = countFlow.openSink();

    assertEquals( "not equal: tuple.get(1)", "1\ta\t1\tA", iterator.next().get( 1 ) );
    assertEquals( "not equal: tuple.get(1)", "2\tb\t2\tB", iterator.next().get( 1 ) );

    iterator.close();
    }

  /**
   * this test intentionally filters out all values so the intermediate tap is empty. this tap is cogrouped with
   * a new stream using an outerjoin.
   *
   * @throws Exception
   */
  public void testCoGroupFilteredBranch() throws Exception
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
    Tap sink = new Hfs( new TextLine(), outputPath + "/complex/cogroupfilteredbranch/", true );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );
    pipeUpper = new Each( pipeUpper, new Fields( "num" ), new RegexFilter( "^fobar" ) ); // intentionally filtering all
    pipeUpper = new GroupBy( pipeUpper, new Fields( "num" ) );

    Pipe splice = new CoGroup( pipeLower, new Fields( "num" ), pipeUpper, new Fields( "num" ), Fields.size( 4 ), new OuterJoin() );

    Flow countFlow = new FlowConnector( getProperties() ).connect( sources, sink, splice );

//    countFlow.writeDOT( "cogroup.dot" );
//    System.out.println( "countFlow =\n" + countFlow );

    countFlow.complete();

    validateLength( countFlow, 5, null );

    TupleEntryIterator iterator = countFlow.openSink();

    assertEquals( "not equal: tuple.get(1)", "1\ta\tnull\tnull", iterator.next().get( 1 ) );
    assertEquals( "not equal: tuple.get(1)", "2\tb\tnull\tnull", iterator.next().get( 1 ) );

    iterator.close();
    }

  public void testCoGroupSelf() throws Exception
    {
    if( !new File( inputFileLower ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileLower );
    copyFromLocal( inputFileUpper );

    Tap sourceLower = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileLower );
    Tap sourceUpper = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileLower );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    // using null pos so all fields are written
    Tap sink = new Hfs( new TextLine(), outputPath + "/complex/cogroupself/", true );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );

    Pipe splice = new CoGroup( pipeLower, new Fields( "num" ), pipeUpper, new Fields( "num" ), Fields.size( 4 ) );

    Flow countFlow = new FlowConnector( getProperties() ).connect( sources, sink, splice );

//    countFlow.writeDOT( "cogroupself.dot" );

    countFlow.complete();

    validateLength( countFlow, 5, null );

    TupleEntryIterator iterator = countFlow.openSink();

    assertEquals( "not equal: tuple.get(1)", "1\ta\t1\ta", iterator.next().get( 1 ) );
    assertEquals( "not equal: tuple.get(1)", "2\tb\t2\tb", iterator.next().get( 1 ) );

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
    catch( PlannerException exception )
      {
//      exception.writeDOT( "cogroupedevery.dot" );
      throw exception;
      }

//    countFlow.writeDOT( "cogroup.dot" );
//    System.out.println( "countFlow =\n" + countFlow );

    countFlow.complete();

    validateLength( countFlow, 5, null );

    TupleEntryIterator iterator = countFlow.openSink();

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

    TupleEntryIterator iterator = countFlow.openSink();

    Set<String> results = new HashSet<String>();

    results.add( "1\ta\t1\tA" );
    results.add( "5\tb\t5\tE" );
    results.add( "5\te\t5\tE" );

    assertNotNull( "not equal: tuple.get(1)", results.remove( iterator.next().get( 1 ) ) );
    assertNotNull( "not equal: tuple.get(1)", results.remove( iterator.next().get( 1 ) ) );
    assertNotNull( "not equal: tuple.get(1)", results.remove( iterator.next().get( 1 ) ) );

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

    TupleEntryIterator iterator = countFlow.openSink();

    Set<String> results = new HashSet<String>();

    results.add( "1\ta\t1\tA" );
    results.add( "null\tnull\t2\tB" );
    results.add( "null\tnull\t3\tC" );
    results.add( "null\tnull\t4\tD" );
    results.add( "5\tb\t5\tE" );
    results.add( "5\te\t5\tE" );
    results.add( "6\tc\tnull\tnull" );

    assertNotNull( "not equal: tuple.get(1)", results.remove( iterator.next().get( 1 ) ) );
    assertNotNull( "not equal: tuple.get(1)", results.remove( iterator.next().get( 1 ) ) );
    assertNotNull( "not equal: tuple.get(1)", results.remove( iterator.next().get( 1 ) ) );
    assertNotNull( "not equal: tuple.get(1)", results.remove( iterator.next().get( 1 ) ) );
    assertNotNull( "not equal: tuple.get(1)", results.remove( iterator.next().get( 1 ) ) );
    assertNotNull( "not equal: tuple.get(1)", results.remove( iterator.next().get( 1 ) ) );
    assertNotNull( "not equal: tuple.get(1)", results.remove( iterator.next().get( 1 ) ) );

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

    TupleEntryIterator iterator = countFlow.openSink();

    Set<String> results = new HashSet<String>();

    results.add( "1\ta\t1\tA" );
    results.add( "5\tb\t5\tE" );
    results.add( "5\te\t5\tE" );
    results.add( "6\tc\tnull\tnull" );

    assertNotNull( "not equal: tuple.get(1)", results.remove( iterator.next().get( 1 ) ) );
    assertNotNull( "not equal: tuple.get(1)", results.remove( iterator.next().get( 1 ) ) );
    assertNotNull( "not equal: tuple.get(1)", results.remove( iterator.next().get( 1 ) ) );
    assertNotNull( "not equal: tuple.get(1)", results.remove( iterator.next().get( 1 ) ) );

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

    TupleEntryIterator iterator = countFlow.openSink();

    Set<String> results = new HashSet<String>();

    results.add( "1\ta\t1\tA" );
    results.add( "null\tnull\t2\tB" );
    results.add( "null\tnull\t3\tC" );
    results.add( "null\tnull\t4\tD" );
    results.add( "5\tb\t5\tE" );
    results.add( "5\te\t5\tE" );

    assertNotNull( "not equal: tuple.get(1)", results.remove( iterator.next().get( 1 ) ) );
    assertNotNull( "not equal: tuple.get(1)", results.remove( iterator.next().get( 1 ) ) );
    assertNotNull( "not equal: tuple.get(1)", results.remove( iterator.next().get( 1 ) ) );
    assertNotNull( "not equal: tuple.get(1)", results.remove( iterator.next().get( 1 ) ) );
    assertNotNull( "not equal: tuple.get(1)", results.remove( iterator.next().get( 1 ) ) );
    assertNotNull( "not equal: tuple.get(1)", results.remove( iterator.next().get( 1 ) ) );

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

    TupleEntryIterator iterator = countFlow.openSink();

    Set<String> results = new HashSet<String>();

    results.add( "1\ta\t1\tA\t1\ta" );
    results.add( "null\tnull\t2\tB\t2\tb" );
    results.add( "null\tnull\t3\tC\t3\tc" );
    results.add( "null\tnull\t4\tD\t4\td" );
    results.add( "5\tb\t5\tE\t5\te" );
    results.add( "5\te\t5\tE\t5\te" );

    assertNotNull( "not equal: tuple.get(1)", results.remove( iterator.next().get( 1 ) ) );
    assertNotNull( "not equal: tuple.get(1)", results.remove( iterator.next().get( 1 ) ) );
    assertNotNull( "not equal: tuple.get(1)", results.remove( iterator.next().get( 1 ) ) );
    assertNotNull( "not equal: tuple.get(1)", results.remove( iterator.next().get( 1 ) ) );
    assertNotNull( "not equal: tuple.get(1)", results.remove( iterator.next().get( 1 ) ) );
    assertNotNull( "not equal: tuple.get(1)", results.remove( iterator.next().get( 1 ) ) );

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

    Pipe cogroup = new CoGroup( pipeLower, new Fields( "numA" ), pipeUpper, new Fields( "numB" ) );

    Flow flow = new FlowConnector( getProperties() ).connect( sources, sink, cogroup );

//    System.out.println( "flow =\n" + flow );

    flow.complete();

    validateLength( flow, 5, null );

    TupleEntryIterator iterator = flow.openSink();

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

    TupleEntryIterator iterator = flow.openSink();

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

    Pipe cogroup = new CoGroup( pipeLower, new Fields( "num" ), 1, new Fields( "num1", "char1", "num2", "char2" ) );

    Flow flow = new FlowConnector( getProperties() ).connect( sources, sink, cogroup );

//    System.out.println( "flow =\n" + flow );

    flow.complete();

    validateLength( flow, 5, null );

    TupleEntryIterator iterator = flow.openSink();

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

    TupleEntryIterator iterator = countFlow.openSink();

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
    sources.put( "source102", source10 );

    // using null pos so all fields are written
    Tap sink = new Hfs( new TextLine(), stringPath, true );

    Pipe pipeNum20 = new Pipe( "source20" );
    Pipe pipeNum101 = new Pipe( "source101" );
    Pipe pipeNum102 = new Pipe( "source102" );

    Pipe splice1 = new CoGroup( pipeNum20, new Fields( "num" ), pipeNum101, new Fields( "num" ), new Fields( "num1", "num2" ) );

    Pipe splice2 = new CoGroup( splice1, new Fields( "num1" ), pipeNum102, new Fields( "num" ), new Fields( "num1", "num2", "num3" ) );

    splice2 = new Each( splice2, new Identity() );

    Map<Object, Object> properties = getProperties();

    FlowConnector.setIntermediateSchemeClass( properties, schemeClass );

    FlowConnector flowConnector = new FlowConnector( properties );

    Flow flow = flowConnector.connect( "cogroupopt", sources, sink, splice2 );

    assertEquals( "wrong number of steps", 2, flow.getSteps().size() );

//    flow.writeDOT( "cogroupcogroupwout.dot" );

    flow.complete();

    validateLength( flow, 10, null );

    TupleEntryIterator iterator = flow.openSink();

    assertEquals( "not equal: tuple.get(1)", "1\t1\t1", iterator.next().get( 1 ) );
    assertEquals( "not equal: tuple.get(1)", "10\t10\t10", iterator.next().get( 1 ) );

    iterator.close();
    }


  }