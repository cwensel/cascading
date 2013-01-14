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

package cascading;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cascading.flow.Flow;
import cascading.flow.FlowConnectorProps;
import cascading.flow.FlowProps;
import cascading.operation.Function;
import cascading.operation.Identity;
import cascading.operation.Insert;
import cascading.operation.aggregator.Count;
import cascading.operation.aggregator.First;
import cascading.operation.regex.RegexFilter;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Discard;
import cascading.pipe.joiner.InnerJoin;
import cascading.pipe.joiner.Joiner;
import cascading.pipe.joiner.LeftJoin;
import cascading.pipe.joiner.MixedJoin;
import cascading.pipe.joiner.OuterJoin;
import cascading.pipe.joiner.RightJoin;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import org.junit.Test;

import static data.InputData.*;


public class CoGroupFieldedPipesPlatformTest extends PlatformTestCase
  {
  public CoGroupFieldedPipesPlatformTest()
    {
    super( true, 4, 1 ); // leave cluster testing enabled
    }

  @Test
  public void testCross() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLhs );
    getPlatform().copyFromLocal( inputFileRhs );

    Map sources = new HashMap();

    sources.put( "lhs", getPlatform().getTextFile( inputFileLhs ) );
    sources.put( "rhs", getPlatform().getTextFile( inputFileRhs ) );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "cross" ), SinkMode.REPLACE );

    Pipe pipeLower = new Each( "lhs", new Fields( "line" ), new RegexSplitter( new Fields( "numLHS", "charLHS" ), " " ) );
    Pipe pipeUpper = new Each( "rhs", new Fields( "line" ), new RegexSplitter( new Fields( "numRHS", "charRHS" ), " " ) );

    Pipe cross = new CoGroup( pipeLower, new Fields( "numLHS" ), pipeUpper, new Fields( "numRHS" ), new InnerJoin() );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, cross );

    flow.complete();

    validateLength( flow, 37, null );

    List<Tuple> values = getSinkAsList( flow );

    assertTrue( values.contains( new Tuple( "1\ta\t1\tA" ) ) );
    assertTrue( values.contains( new Tuple( "1\ta\t1\tB" ) ) );
    }

  @Test
  public void testCoGroup() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    Tap sourceLower = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLower );
    Tap sourceUpper = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileUpper );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "cogroup" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );

    Pipe splice = new CoGroup( pipeLower, new Fields( "num" ), pipeUpper, new Fields( "num" ), Fields.size( 4 ) );

    Map<Object, Object> properties = getProperties();

    // make sure hasher is getting called, but does nothing special
    FlowProps.setDefaultTupleElementComparator( properties, getPlatform().getStringComparator( false ).getClass().getCanonicalName() );

    Flow flow = getPlatform().getFlowConnector( properties ).connect( sources, sink, splice );

    flow.complete();

    validateLength( flow, 5 );

    List<Tuple> values = getSinkAsList( flow );

    assertTrue( values.contains( new Tuple( "1\ta\t1\tA" ) ) );
    assertTrue( values.contains( new Tuple( "2\tb\t2\tB" ) ) );
    }

  @Test
  public void testCoGroupSamePipeName() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    Tap sourceLower = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLower );
    Tap sourceUpper = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileUpper );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "renamedpipes" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe pipeLower = new Pipe( "lower" );
    Pipe pipeUpper = new Pipe( "upper" );

    // these pipes will hide the source name, and could cause one to be lost
    pipeLower = new Pipe( "same", pipeLower );
    pipeUpper = new Pipe( "same", pipeUpper );

    pipeLower = new Each( pipeLower, new Fields( "line" ), splitter );
    pipeUpper = new Each( pipeUpper, new Fields( "line" ), splitter );

//    pipeLower = new Each( pipeLower, new Fields( "num", "char" ), new Identity( new Fields( "num", "char" ) ) );
//    pipeUpper = new Each( pipeUpper, new Fields( "num", "char" ), new Identity( new Fields( "num", "char" ) ) );

    pipeLower = new Pipe( "left", pipeLower );
    pipeUpper = new Pipe( "right", pipeUpper );

//    pipeLower = new Each( pipeLower, new Debug( true ) );
//    pipeUpper = new Each( pipeUpper, new Debug( true ) );

    Pipe splice = new CoGroup( pipeLower, new Fields( "num" ), pipeUpper, new Fields( "num" ), Fields.size( 4 ) );

//    splice = new Each( splice, new Debug( true ) );
    splice = new Pipe( "splice", splice );
    splice = new Pipe( "tail", splice );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, splice );

    flow.complete();

    validateLength( flow, 5 );

    List<Tuple> values = getSinkAsList( flow );

    assertTrue( values.contains( new Tuple( "1\ta\t1\tA" ) ) );
    assertTrue( values.contains( new Tuple( "2\tb\t2\tB" ) ) );
    }

  @Test
  public void testCoGroupWithUnknowns() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    Tap sourceLower = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLower );
    Tap sourceUpper = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileUpper );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "unknown" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( Fields.UNKNOWN, " " );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );

    Pipe splice = new CoGroup( pipeLower, new Fields( 0 ), pipeUpper, new Fields( 0 ), Fields.size( 4 ) );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, splice );

    flow.complete();

    validateLength( flow, 5 );

    List<Tuple> values = getSinkAsList( flow );

    assertTrue( values.contains( new Tuple( "1\ta\t1\tA" ) ) );
    assertTrue( values.contains( new Tuple( "2\tb\t2\tB" ) ) );
    }

  /**
   * this test intentionally filters out all values so the intermediate tap is empty. this tap is cogrouped with
   * a new stream using an outerjoin.
   *
   * @throws Exception
   */
  @Test
  public void testCoGroupFilteredBranch() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    Tap sourceLower = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLower );
    Tap sourceUpper = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileUpper );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "cogroupfilteredbranch" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );
    pipeUpper = new Each( pipeUpper, new Fields( "num" ), new RegexFilter( "^fobar" ) ); // intentionally filtering all
    pipeUpper = new GroupBy( pipeUpper, new Fields( "num" ) );

    Pipe splice = new CoGroup( pipeLower, new Fields( "num" ), pipeUpper, new Fields( "num" ), Fields.size( 4 ), new OuterJoin() );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, splice );

    flow.complete();

    validateLength( flow, 5 );

    List<Tuple> values = getSinkAsList( flow );

    assertTrue( values.contains( new Tuple( "1\ta\tnull\tnull" ) ) );
    assertTrue( values.contains( new Tuple( "2\tb\tnull\tnull" ) ) );
    }

  @Test
  public void testCoGroupSelf() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );

    Tap sourceLower = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLower );
    Tap sourceUpper = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLower );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "cogroupself" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );

    Pipe splice = new CoGroup( pipeLower, new Fields( "num" ), pipeUpper, new Fields( "num" ), Fields.size( 4 ) );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, splice );

    flow.complete();

    validateLength( flow, 5 );

    List<Tuple> values = getSinkAsList( flow );

    assertTrue( values.contains( new Tuple( "1\ta\t1\ta" ) ) );
    assertTrue( values.contains( new Tuple( "2\tb\t2\tb" ) ) );
    }

  /**
   * Method testCoGroupAfterEvery tests that a tmp tap is inserted after the Every in the cogroup join
   *
   * @throws Exception when
   */
  @Test
  public void testCoGroupAfterEvery() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    Tap sourceLower = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLower );
    Tap sourceUpper = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileUpper );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "afterevery" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    pipeLower = new GroupBy( pipeLower, new Fields( "num" ) );
    pipeLower = new Every( pipeLower, new Fields( "char" ), new First(), Fields.ALL );

    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );
    pipeUpper = new GroupBy( pipeUpper, new Fields( "num" ) );
    pipeUpper = new Every( pipeUpper, new Fields( "char" ), new First(), Fields.ALL );

    Pipe splice = new CoGroup( pipeLower, new Fields( "num" ), pipeUpper, new Fields( "num" ), Fields.size( 4 ) );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, splice );

    flow.complete();

    validateLength( flow, 5, null );

    List<Tuple> values = getSinkAsList( flow );

    assertTrue( values.contains( new Tuple( "1\ta\t1\tA" ) ) );
    assertTrue( values.contains( new Tuple( "2\tb\t2\tB" ) ) );
    }

  /**
   * Tests that CoGroup properly resolves fields when following an Every
   *
   * @throws Exception
   */
  @Test
  public void testCoGroupAfterEveryNoDeclared() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    Tap sourceLower = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLower );
    Tap sourceUpper = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileUpper );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "aftereverynodeclared" ), SinkMode.REPLACE );

    Function splitter1 = new RegexSplitter( new Fields( "num1", "char1" ), " " );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter1 );
    pipeLower = new Each( pipeLower, new Insert( new Fields( "one", "two", "three", "four" ), "one", "two", "three", "four" ), Fields.ALL );
    pipeLower = new GroupBy( pipeLower, new Fields( "num1" ) );
    pipeLower = new Every( pipeLower, new Fields( "char1" ), new First(), Fields.ALL );

    Function splitter2 = new RegexSplitter( new Fields( "num2", "char2" ), " " );

    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter2 );
    pipeUpper = new GroupBy( pipeUpper, new Fields( "num2" ) );
    pipeUpper = new Every( pipeUpper, new Fields( "char2" ), new First(), Fields.ALL );

    Pipe splice = new CoGroup( pipeLower, new Fields( "num1" ), pipeUpper, new Fields( "num2" ) );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, splice );

    flow.complete();

    validateLength( flow, 5, null );

    List<Tuple> values = getSinkAsList( flow );

    assertTrue( values.contains( new Tuple( "1\ta\t1\tA" ) ) );
    assertTrue( values.contains( new Tuple( "2\tb\t2\tB" ) ) );
    }

  @Test
  public void testCoGroupInnerSingleField() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLowerOffset );
    getPlatform().copyFromLocal( inputFileUpper );

    Tap sourceLower = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLowerOffset );
    Tap sourceUpper = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileUpper );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "cogroupinnersingle" ), SinkMode.REPLACE );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), new RegexSplitter( new Fields( "num1", "char" ), " " ), new Fields( "num1" ) );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), new RegexSplitter( new Fields( "num2", "char" ), " " ), new Fields( "num2" ) );

    Pipe join = new CoGroup( pipeLower, new Fields( "num1" ), pipeUpper, new Fields( "num2" ) );

    join = new Every( join, new Count() );

//    join = new Each( join, new Debug( true ) );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, join );

    flow.complete();

    validateLength( flow, 2, null );

    Set<Tuple> results = new HashSet<Tuple>();

    results.add( new Tuple( "1\t1\t1" ) );
    results.add( new Tuple( "5\t5\t2" ) );

    List<Tuple> actual = getSinkAsList( flow );

    results.removeAll( actual );

    assertEquals( 0, results.size() );
    }

  /**
   * 1 a1
   * 1 a2
   * 1 a3
   * 2 b1
   * 3 c1
   * 4 d1
   * 4 d2
   * 4 d3
   * 5 e1
   * 5 e2
   * 5 e3
   * 7 g1
   * 7 g2
   * 7 g3
   * 7 g4
   * 7 g5
   * <p/>
   * 1 A1
   * 1 A2
   * 1 A3
   * 2 B1
   * 2 B2
   * 2 B3
   * 4 D1
   * 6 F1
   * 6 F2
   * <p/>
   * 1	a1	1	A1
   * 1	a1	1	A2
   * 1	a1	1	A3
   * 1	a2	1	A1
   * 1	a2	1	A2
   * 1	a2	1	A3
   * 1	a3	1	A1
   * 1	a3	1	A2
   * 1	a3	1	A3
   * 2	b1	2	B1
   * 2	b1	2	B2
   * 2	b1	2	B3
   * 4	d1	4	D1
   * 4	d2	4	D1
   * 4	d3	4	D1
   *
   * @throws Exception
   */
  @Test
  public void testCoGroupInner() throws Exception
    {
    HashSet<Tuple> results = new HashSet<Tuple>();

    results.add( new Tuple( "1\ta1\t1\tA1" ) );
    results.add( new Tuple( "1\ta1\t1\tA2" ) );
    results.add( new Tuple( "1\ta1\t1\tA3" ) );
    results.add( new Tuple( "1\ta2\t1\tA1" ) );
    results.add( new Tuple( "1\ta2\t1\tA2" ) );
    results.add( new Tuple( "1\ta2\t1\tA3" ) );
    results.add( new Tuple( "1\ta3\t1\tA1" ) );
    results.add( new Tuple( "1\ta3\t1\tA2" ) );
    results.add( new Tuple( "1\ta3\t1\tA3" ) );
    results.add( new Tuple( "2\tb1\t2\tB1" ) );
    results.add( new Tuple( "2\tb1\t2\tB2" ) );
    results.add( new Tuple( "2\tb1\t2\tB3" ) );
    results.add( new Tuple( "4\td1\t4\tD1" ) );
    results.add( new Tuple( "4\td2\t4\tD1" ) );
    results.add( new Tuple( "4\td3\t4\tD1" ) );

    handleJoins( "cogroupinner", new InnerJoin(), results, 7, false );
    handleJoins( "cogroupinner-resultgroup", new InnerJoin(), results, 7, true );
    }

  /**
   * 1 a1
   * 1 a2
   * 1 a3
   * 2 b1
   * 3 c1
   * 4 d1
   * 4 d2
   * 4 d3
   * 5 e1
   * 5 e2
   * 5 e3
   * 7 g1
   * 7 g2
   * 7 g3
   * 7 g4
   * 7 g5
   * <p/>
   * 1 A1
   * 1 A2
   * 1 A3
   * 2 B1
   * 2 B2
   * 2 B3
   * 4 D1
   * 6 F1
   * 6 F2
   * <p/>
   * 1	a1	1	A1
   * 1	a1	1	A2
   * 1	a1	1	A3
   * 1	a2	1	A1
   * 1	a2	1	A2
   * 1	a2	1	A3
   * 1	a3	1	A1
   * 1	a3	1	A2
   * 1	a3	1	A3
   * 2	b1	2	B1
   * 2	b1	2	B2
   * 2	b1	2	B3
   * 3	c1	null	null
   * 4	d1	4	D1
   * 4	d2	4	D1
   * 4	d3	4	D1
   * 5	e1	null	null
   * 5	e2	null	null
   * 5	e3	null	null
   * null	null	6	F1
   * null	null	6	F2
   * 7	g1	null	null
   * 7	g2	null	null
   * 7	g3	null	null
   * 7	g4	null	null
   * 7	g5	null	null
   *
   * @throws Exception
   */
  @Test
  public void testCoGroupOuter() throws Exception
    {
    Set<Tuple> results = new HashSet<Tuple>();

    results.add( new Tuple( "1\ta1\t1\tA1" ) );
    results.add( new Tuple( "1\ta1\t1\tA2" ) );
    results.add( new Tuple( "1\ta1\t1\tA3" ) );
    results.add( new Tuple( "1\ta2\t1\tA1" ) );
    results.add( new Tuple( "1\ta2\t1\tA2" ) );
    results.add( new Tuple( "1\ta2\t1\tA3" ) );
    results.add( new Tuple( "1\ta3\t1\tA1" ) );
    results.add( new Tuple( "1\ta3\t1\tA2" ) );
    results.add( new Tuple( "1\ta3\t1\tA3" ) );
    results.add( new Tuple( "2\tb1\t2\tB1" ) );
    results.add( new Tuple( "2\tb1\t2\tB2" ) );
    results.add( new Tuple( "2\tb1\t2\tB3" ) );
    results.add( new Tuple( "3\tc1\tnull\tnull" ) );
    results.add( new Tuple( "4\td1\t4\tD1" ) );
    results.add( new Tuple( "4\td2\t4\tD1" ) );
    results.add( new Tuple( "4\td3\t4\tD1" ) );
    results.add( new Tuple( "5\te1\tnull\tnull" ) );
    results.add( new Tuple( "5\te2\tnull\tnull" ) );
    results.add( new Tuple( "5\te3\tnull\tnull" ) );
    results.add( new Tuple( "null\tnull\t6\tF1" ) );
    results.add( new Tuple( "null\tnull\t6\tF2" ) );
    results.add( new Tuple( "7\tg1\tnull\tnull" ) );
    results.add( new Tuple( "7\tg2\tnull\tnull" ) );
    results.add( new Tuple( "7\tg3\tnull\tnull" ) );
    results.add( new Tuple( "7\tg4\tnull\tnull" ) );
    results.add( new Tuple( "7\tg5\tnull\tnull" ) );

    handleJoins( "cogroupouter", new OuterJoin(), results, 7, false );
    handleJoins( "cogroupouter-resultgroup", new OuterJoin(), results, 7, true );
    }

  /**
   * 1 a1
   * 1 a2
   * 1 a3
   * 2 b1
   * 3 c1
   * 4 d1
   * 4 d2
   * 4 d3
   * 5 e1
   * 5 e2
   * 5 e3
   * 7 g1
   * 7 g2
   * 7 g3
   * 7 g4
   * 7 g5
   * <p/>
   * 1 A1
   * 1 A2
   * 1 A3
   * 2 B1
   * 2 B2
   * 2 B3
   * 4 D1
   * 6 F1
   * 6 F2
   * <p/>
   * 1	a1	1	A1
   * 1	a1	1	A2
   * 1	a1	1	A3
   * 1	a2	1	A1
   * 1	a2	1	A2
   * 1	a2	1	A3
   * 1	a3	1	A1
   * 1	a3	1	A2
   * 1	a3	1	A3
   * 2	b1	2	B1
   * 2	b1	2	B2
   * 2	b1	2	B3
   * 3	c1	null	null
   * 4	d1	4	D1
   * 4	d2	4	D1
   * 4	d3	4	D1
   * 5	e1	null	null
   * 5	e2	null	null
   * 5	e3	null	null
   * 7	g1	null	null
   * 7	g2	null	null
   * 7	g3	null	null
   * 7	g4	null	null
   * 7	g5	null	null
   *
   * @throws Exception
   */
  @Test
  public void testCoGroupInnerOuter() throws Exception
    {
    Set<Tuple> results = new HashSet<Tuple>();

    results.add( new Tuple( "1\ta1\t1\tA1" ) );
    results.add( new Tuple( "1\ta1\t1\tA2" ) );
    results.add( new Tuple( "1\ta1\t1\tA3" ) );
    results.add( new Tuple( "1\ta2\t1\tA1" ) );
    results.add( new Tuple( "1\ta2\t1\tA2" ) );
    results.add( new Tuple( "1\ta2\t1\tA3" ) );
    results.add( new Tuple( "1\ta3\t1\tA1" ) );
    results.add( new Tuple( "1\ta3\t1\tA2" ) );
    results.add( new Tuple( "1\ta3\t1\tA3" ) );
    results.add( new Tuple( "2\tb1\t2\tB1" ) );
    results.add( new Tuple( "2\tb1\t2\tB2" ) );
    results.add( new Tuple( "2\tb1\t2\tB3" ) );
    results.add( new Tuple( "3\tc1\tnull\tnull" ) );
    results.add( new Tuple( "4\td1\t4\tD1" ) );
    results.add( new Tuple( "4\td2\t4\tD1" ) );
    results.add( new Tuple( "4\td3\t4\tD1" ) );
    results.add( new Tuple( "5\te1\tnull\tnull" ) );
    results.add( new Tuple( "5\te2\tnull\tnull" ) );
    results.add( new Tuple( "5\te3\tnull\tnull" ) );
    results.add( new Tuple( "7\tg1\tnull\tnull" ) );
    results.add( new Tuple( "7\tg2\tnull\tnull" ) );
    results.add( new Tuple( "7\tg3\tnull\tnull" ) );
    results.add( new Tuple( "7\tg4\tnull\tnull" ) );
    results.add( new Tuple( "7\tg5\tnull\tnull" ) );

    handleJoins( "cogroupinnerouter", new LeftJoin(), results, 7, false );
    handleJoins( "cogroupinnerouter-resultgroup", new LeftJoin(), results, 7, true );
    }

  /**
   * 1 a1
   * 1 a2
   * 1 a3
   * 2 b1
   * 3 c1
   * 4 d1
   * 4 d2
   * 4 d3
   * 5 e1
   * 5 e2
   * 5 e3
   * 7 g1
   * 7 g2
   * 7 g3
   * 7 g4
   * 7 g5
   * <p/>
   * 1 A1
   * 1 A2
   * 1 A3
   * 2 B1
   * 2 B2
   * 2 B3
   * 4 D1
   * 6 F1
   * 6 F2
   * <p/>
   * 1	a1	1	A1
   * 1	a1	1	A2
   * 1	a1	1	A3
   * 1	a2	1	A1
   * 1	a2	1	A2
   * 1	a2	1	A3
   * 1	a3	1	A1
   * 1	a3	1	A2
   * 1	a3	1	A3
   * 2	b1	2	B1
   * 2	b1	2	B2
   * 2	b1	2	B3
   * 4	d1	4	D1
   * 4	d2	4	D1
   * 4	d3	4	D1
   * null	null	6	F1
   * null	null	6	F2
   *
   * @throws Exception
   */
  @Test
  public void testCoGroupOuterInner() throws Exception
    {
    Set<Tuple> results = new HashSet<Tuple>();

    results.add( new Tuple( "1\ta1\t1\tA1" ) );
    results.add( new Tuple( "1\ta1\t1\tA2" ) );
    results.add( new Tuple( "1\ta1\t1\tA3" ) );
    results.add( new Tuple( "1\ta2\t1\tA1" ) );
    results.add( new Tuple( "1\ta2\t1\tA2" ) );
    results.add( new Tuple( "1\ta2\t1\tA3" ) );
    results.add( new Tuple( "1\ta3\t1\tA1" ) );
    results.add( new Tuple( "1\ta3\t1\tA2" ) );
    results.add( new Tuple( "1\ta3\t1\tA3" ) );
    results.add( new Tuple( "2\tb1\t2\tB1" ) );
    results.add( new Tuple( "2\tb1\t2\tB2" ) );
    results.add( new Tuple( "2\tb1\t2\tB3" ) );
    results.add( new Tuple( "4\td1\t4\tD1" ) );
    results.add( new Tuple( "4\td2\t4\tD1" ) );
    results.add( new Tuple( "4\td3\t4\tD1" ) );
    results.add( new Tuple( "null\tnull\t6\tF1" ) );
    results.add( new Tuple( "null\tnull\t6\tF2" ) );

    handleJoins( "cogroupouterinner", new RightJoin(), results, 7, false );
    handleJoins( "cogroupouterinner-resultgroup", new RightJoin(), results, 7, true );
    }

  private void handleJoins( String path, Joiner joiner, Set<Tuple> results, int numGroups, boolean useResultGroupFields ) throws Exception
    {
    results = new HashSet<Tuple>( results );

    getPlatform().copyFromLocal( inputFileLhsSparse );
    getPlatform().copyFromLocal( inputFileRhsSparse );

    Tap sourceLower = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLhsSparse );
    Tap sourceUpper = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileRhsSparse );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( path ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );

    Fields declaredFields = new Fields( "num", "char", "num2", "char2" );

    Pipe splice;
    if( useResultGroupFields )
      splice = new CoGroup( pipeLower, new Fields( "num" ), pipeUpper, new Fields( "num" ), declaredFields, new Fields( "num", "num2" ), joiner );
    else
      splice = new CoGroup( pipeLower, new Fields( "num" ), pipeUpper, new Fields( "num" ), declaredFields, joiner );

    splice = new Every( splice, Fields.ALL, new TestIdentityBuffer( new Fields( "num", "num2" ), numGroups ), Fields.RESULTS );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, splice );

    flow.complete();

    validateLength( flow, results.size() );

    List<Tuple> actual = getSinkAsList( flow );

    results.removeAll( actual );

    assertEquals( 0, results.size() );
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
  @Test
  public void testCoGroupMixed() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLowerOffset );
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    Tap sourceLowerOffset = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLowerOffset );
    Tap sourceUpper = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileUpper );
    Tap sourceLower = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLower );

    Map sources = new HashMap();

    sources.put( "loweroffset", sourceLowerOffset );
    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "cogroupmixed" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe pipeLowerOffset = new Each( new Pipe( "loweroffset" ), new Fields( "line" ), splitter );
    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );


    Pipe[] pipes = Pipe.pipes( pipeLowerOffset, pipeUpper, pipeLower );
    Fields[] fields = Fields.fields( new Fields( "num" ), new Fields( "num" ), new Fields( "num" ) );

    MixedJoin join = new MixedJoin( new boolean[]{MixedJoin.OUTER, MixedJoin.INNER, MixedJoin.OUTER} );
    Pipe splice = new CoGroup( pipes, fields, Fields.size( 6 ), join );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, splice );

    flow.complete();

    validateLength( flow, 6 );


    Set<Tuple> results = new HashSet<Tuple>();

    results.add( new Tuple( "1\ta\t1\tA\t1\ta" ) );
    results.add( new Tuple( "null\tnull\t2\tB\t2\tb" ) );
    results.add( new Tuple( "null\tnull\t3\tC\t3\tc" ) );
    results.add( new Tuple( "null\tnull\t4\tD\t4\td" ) );
    results.add( new Tuple( "5\tb\t5\tE\t5\te" ) );
    results.add( new Tuple( "5\te\t5\tE\t5\te" ) );

    List<Tuple> actual = getSinkAsList( flow );

    results.removeAll( actual );

    assertEquals( 0, results.size() );
    }

  @Test
  public void testCoGroupDiffFields() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    Tap sourceLower = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLower );
    Tap sourceUpper = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileUpper );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "difffields" ), SinkMode.REPLACE );

    Function splitterLower = new RegexSplitter( new Fields( "numA", "lower" ), " " );
    Function splitterUpper = new RegexSplitter( new Fields( "numB", "upper" ), " " );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitterLower );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitterUpper );

    Pipe cogroup = new CoGroup( pipeLower, new Fields( "numA" ), pipeUpper, new Fields( "numB" ) );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, cogroup );

    flow.complete();

    validateLength( flow, 5 );

    List<Tuple> actual = getSinkAsList( flow );

    assertTrue( actual.contains( new Tuple( "1\ta\t1\tA" ) ) );
    assertTrue( actual.contains( new Tuple( "2\tb\t2\tB" ) ) );
    }

  @Test
  public void testCoGroupGroupBy() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    Tap sourceLower = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLower );
    Tap sourceUpper = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileUpper );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "cogroupgroupby" ), SinkMode.REPLACE );

    Function splitterLower = new RegexSplitter( new Fields( "numA", "lower" ), " " );
    Function splitterUpper = new RegexSplitter( new Fields( "numB", "upper" ), " " );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitterLower );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitterUpper );

    Pipe cogroup = new CoGroup( pipeLower, new Fields( "numA" ), pipeUpper, new Fields( "numB" ) );

    Pipe groupby = new GroupBy( cogroup, new Fields( "numA" ) );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, groupby );

    flow.complete();

    validateLength( flow, 5, null );

    List<Tuple> actual = getSinkAsList( flow );

    assertTrue( actual.contains( new Tuple( "1\ta\t1\tA" ) ) );
    assertTrue( actual.contains( new Tuple( "2\tb\t2\tB" ) ) );
    }

  @Test
  public void testCoGroupSamePipe() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );

    Tap source = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLower );

    Map sources = new HashMap();

    sources.put( "lower", source );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "samepipe" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );

    Pipe cogroup = new CoGroup( pipeLower, new Fields( "num" ), 1, new Fields( "num1", "char1", "num2", "char2" ) );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, cogroup );

    flow.complete();

    validateLength( flow, 5, null );

    List<Tuple> actual = getSinkAsList( flow );

    assertTrue( actual.contains( new Tuple( "1\ta\t1\ta" ) ) );
    assertTrue( actual.contains( new Tuple( "2\tb\t2\tb" ) ) );
    }

  @Test
  public void testCoGroupSamePipe2() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );

    Tap source = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLower );

    Map sources = new HashMap();

    sources.put( "lower", source );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "samepipe2" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );

    Pipe cogroup = new CoGroup( pipeLower, new Fields( "num" ), pipeLower, new Fields( "num" ), new Fields( "num1", "char1", "num2", "char2" ) );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, cogroup );

    flow.complete();

    validateLength( flow, 5, null );

    List<Tuple> actual = getSinkAsList( flow );

    assertTrue( actual.contains( new Tuple( "1\ta\t1\ta" ) ) );
    assertTrue( actual.contains( new Tuple( "2\tb\t2\tb" ) ) );
    }

  @Test
  public void testCoGroupSamePipe3() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );

    Tap source = getPlatform().getDelimitedFile( new Fields( "num", "char" ), " ", inputFileLower );

    Map sources = new HashMap();

    sources.put( "lower", source );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "samepipe3" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "lower" );

    Pipe lhs = new Pipe( "lhs", pipe );
    Pipe rhs = new Pipe( "rhs", pipe );

    Pipe cogroup = new CoGroup( lhs, new Fields( "num" ), rhs, new Fields( "num" ), new Fields( "num1", "char1", "num2", "char2" ) );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, cogroup );

    flow.complete();

    validateLength( flow, 5, null );

    List<Tuple> actual = getSinkAsList( flow );

    assertTrue( actual.contains( new Tuple( "1\ta\t1\ta" ) ) );
    assertTrue( actual.contains( new Tuple( "2\tb\t2\tb" ) ) );
    }

  @Test
  public void testCoGroupAroundCoGroup() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    Tap sourceLower = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLower );
    Tap sourceUpper = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileUpper );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper1", sourceUpper );
    sources.put( "upper2", sourceUpper );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "cogroupacogroup" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper1 = new Each( new Pipe( "upper1" ), new Fields( "line" ), splitter );
    Pipe pipeUpper2 = new Each( new Pipe( "upper2" ), new Fields( "line" ), splitter );

    Pipe splice1 = new CoGroup( pipeLower, new Fields( "num" ), pipeUpper1, new Fields( "num" ), new Fields( "num1", "char1", "num2", "char2" ) );

    splice1 = new Each( splice1, new Identity() );

    Pipe splice2 = new CoGroup( splice1, new Fields( "num1" ), pipeUpper2, new Fields( "num" ), new Fields( "num1", "char1", "num2", "char2", "num3", "char3" ) );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, splice2 );

    flow.complete();

    validateLength( flow, 5, null );

    List<Tuple> actual = getSinkAsList( flow );

    assertTrue( actual.contains( new Tuple( "1\ta\t1\tA\t1\tA" ) ) );
    assertTrue( actual.contains( new Tuple( "2\tb\t2\tB\t2\tB" ) ) );
    }

  @Test
  public void testCoGroupAroundCoGroupWithout() throws Exception
    {
    runCoGroupAroundCoGroup( null, "cogroupacogroupopt1" );
    }

  @Test
  public void testCoGroupAroundCoGroupWith() throws Exception
    {
    // hack to get classname
    runCoGroupAroundCoGroup( getPlatform().getDelimitedFile( new Fields( "num" ), "\t", inputFileNums10 ).getScheme().getClass(), "cogroupacogroupopt2" );
    }

  private void runCoGroupAroundCoGroup( Class schemeClass, String stringPath ) throws IOException
    {
    getPlatform().copyFromLocal( inputFileNums20 );
    getPlatform().copyFromLocal( inputFileNums10 );

    Tap source10 = getPlatform().getDelimitedFile( new Fields( "num" ), "\t", inputFileNums10 );
    Tap source20 = getPlatform().getDelimitedFile( new Fields( "num" ), "\t", inputFileNums20 );

    Map sources = new HashMap();

    sources.put( "source20", source20 );
    sources.put( "source101", source10 );
    sources.put( "source102", source10 );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( stringPath ), SinkMode.REPLACE );

    Pipe pipeNum20 = new Pipe( "source20" );
    Pipe pipeNum101 = new Pipe( "source101" );
    Pipe pipeNum102 = new Pipe( "source102" );

    Pipe splice1 = new CoGroup( pipeNum20, new Fields( "num" ), pipeNum101, new Fields( "num" ), new Fields( "num1", "num2" ) );

    Pipe splice2 = new CoGroup( splice1, new Fields( "num1" ), pipeNum102, new Fields( "num" ), new Fields( "num1", "num2", "num3" ) );

    splice2 = new Each( splice2, new Identity() );

    Map<Object, Object> properties = getPlatform().getProperties();

    if( getPlatform().isMapReduce() )
      FlowConnectorProps.setIntermediateSchemeClass( properties, schemeClass );

    Flow flow = getPlatform().getFlowConnector( properties ).connect( "cogroupopt", sources, sink, splice2 );

    if( getPlatform().isMapReduce() )
      assertEquals( "wrong number of steps", 2, flow.getFlowSteps().size() );

    flow.complete();

    validateLength( flow, 10 );

    List<Tuple> actual = getSinkAsList( flow );

    assertTrue( actual.contains( new Tuple( "1\t1\t1" ) ) );
    assertTrue( actual.contains( new Tuple( "10\t10\t10" ) ) );
    }

  @Test
  public void testCoGroupDiffFieldsSameFile() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    Tap sourceOffsetLower = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLower );
    Tap sourceLower = getPlatform().getTextFile( new Fields( "line" ), inputFileLower );

    Map sources = new HashMap();

    sources.put( "offsetLower", sourceOffsetLower );
    sources.put( "lower", sourceLower );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "samefiledifffields" ), SinkMode.REPLACE );

    Function splitterLower = new RegexSplitter( new Fields( "numA", "left" ), " " );
    Function splitterUpper = new RegexSplitter( new Fields( "numB", "right" ), " " );

    Pipe offsetLower = new Pipe( "offsetLower" );
    offsetLower = new Discard( offsetLower, new Fields( "offset" ) );
    offsetLower = new Each( offsetLower, new Fields( "line" ), splitterLower );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitterUpper );

    Pipe cogroup = new CoGroup( offsetLower, new Fields( "numA" ), pipeLower, new Fields( "numB" ) );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, cogroup );

    flow.complete();

    validateLength( flow, 5 );

    List<Tuple> actual = getSinkAsList( flow );

    assertTrue( actual.contains( new Tuple( "1\ta\t1\ta" ) ) );
    assertTrue( actual.contains( new Tuple( "2\tb\t2\tb" ) ) );
    }

  @Test
  public void testJoinNone() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    Tap sourceLower = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLower );
    Tap sourceUpper = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileUpper );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "joinnone" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );

    Pipe splice = new CoGroup( pipeLower, Fields.NONE, pipeUpper, Fields.NONE, Fields.size( 4 ) );

    Map<Object, Object> properties = getProperties();

    Flow flow = getPlatform().getFlowConnector( properties ).connect( sources, sink, splice );

    flow.complete();

    validateLength( flow, 25 );

    List<Tuple> values = getSinkAsList( flow );

    assertTrue( values.contains( new Tuple( "1\ta\t1\tA" ) ) );
    assertTrue( values.contains( new Tuple( "1\ta\t2\tB" ) ) );
    assertTrue( values.contains( new Tuple( "2\tb\t2\tB" ) ) );
    }
  }