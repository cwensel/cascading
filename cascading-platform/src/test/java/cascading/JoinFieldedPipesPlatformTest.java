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

import java.io.Serializable;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cascading.flow.Flow;
import cascading.operation.Function;
import cascading.operation.Identity;
import cascading.operation.aggregator.First;
import cascading.operation.expression.ExpressionFunction;
import cascading.operation.regex.RegexFilter;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.HashJoin;
import cascading.pipe.Pipe;
import cascading.pipe.joiner.InnerJoin;
import cascading.pipe.joiner.Joiner;
import cascading.pipe.joiner.LeftJoin;
import cascading.pipe.joiner.MixedJoin;
import cascading.pipe.joiner.OuterJoin;
import cascading.pipe.joiner.RightJoin;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Hasher;
import cascading.tuple.Tuple;
import org.junit.Test;

import static data.InputData.*;


public class JoinFieldedPipesPlatformTest extends PlatformTestCase
  {
  public JoinFieldedPipesPlatformTest()
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

    Pipe cross = new HashJoin( pipeLower, new Fields( "numLHS" ), pipeUpper, new Fields( "numRHS" ), new InnerJoin() );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, cross );

    flow.complete();

    validateLength( flow, 37, null );

    List<Tuple> values = getSinkAsList( flow );

    assertTrue( values.contains( new Tuple( "1\ta\t1\tA" ) ) );
    assertTrue( values.contains( new Tuple( "1\ta\t1\tB" ) ) );
    }

  @Test
  public void testJoin() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    Tap sourceLower = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLower );
    Tap sourceUpper = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileUpper );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "join" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );

    Pipe splice = new HashJoin( pipeLower, new Fields( "num" ), pipeUpper, new Fields( "num" ), Fields.size( 4 ) );

    Map<Object, Object> properties = getProperties();

    Flow flow = getPlatform().getFlowConnector( properties ).connect( sources, sink, splice );

    flow.complete();

    validateLength( flow, 5 );

    List<Tuple> values = getSinkAsList( flow );

    assertTrue( values.contains( new Tuple( "1\ta\t1\tA" ) ) );
    assertTrue( values.contains( new Tuple( "2\tb\t2\tB" ) ) );
    }

  @Test
  public void testJoinSamePipeName() throws Exception
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

    Pipe splice = new HashJoin( pipeLower, new Fields( "num" ), pipeUpper, new Fields( "num" ), Fields.size( 4 ) );

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
  public void testJoinWithUnknowns() throws Exception
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

    Pipe splice = new HashJoin( pipeLower, new Fields( 0 ), pipeUpper, new Fields( 0 ), Fields.size( 4 ) );

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
  public void testJoinFilteredBranch() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    Tap sourceLower = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLower );
    Tap sourceUpper = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileUpper );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "joinfilteredbranch" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );
    pipeUpper = new Each( pipeUpper, new Fields( "num" ), new RegexFilter( "^fobar" ) ); // intentionally filtering all
    pipeUpper = new GroupBy( pipeUpper, new Fields( "num" ) );

    Pipe splice = new HashJoin( pipeLower, new Fields( "num" ), pipeUpper, new Fields( "num" ), Fields.size( 4 ), new OuterJoin() );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, splice );

    flow.complete();

    validateLength( flow, 5 );

    List<Tuple> values = getSinkAsList( flow );

    assertTrue( values.contains( new Tuple( "1\ta\tnull\tnull" ) ) );
    assertTrue( values.contains( new Tuple( "2\tb\tnull\tnull" ) ) );
    }

  @Test
  public void testJoinSelf() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );

    Tap sourceLower = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLower );
    Tap sourceUpper = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLower );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "joinself" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );

    Pipe splice = new HashJoin( pipeLower, new Fields( "num" ), pipeUpper, new Fields( "num" ), Fields.size( 4 ) );

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
  public void testJoinAfterEvery() throws Exception
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

    Pipe splice = new HashJoin( pipeLower, new Fields( "num" ), pipeUpper, new Fields( "num" ), Fields.size( 4 ) );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, splice );

    flow.complete();

    validateLength( flow, 5, null );

    List<Tuple> values = getSinkAsList( flow );

    assertTrue( values.contains( new Tuple( "1\ta\t1\tA" ) ) );
    assertTrue( values.contains( new Tuple( "2\tb\t2\tB" ) ) );
    }

  @Test
  public void testJoinInnerSingleField() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLowerOffset );
    getPlatform().copyFromLocal( inputFileUpper );

    Tap sourceLower = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLowerOffset );
    Tap sourceUpper = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileUpper );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "joininnersingle" ), SinkMode.REPLACE );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), new RegexSplitter( new Fields( "num1", "char" ), " " ), new Fields( "num1" ) );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), new RegexSplitter( new Fields( "num2", "char" ), " " ), new Fields( "num2" ) );

    Pipe join = new HashJoin( pipeLower, new Fields( "num1" ), pipeUpper, new Fields( "num2" ) );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, join );

    flow.complete();

    validateLength( flow, 3, null );

    Set<Tuple> results = new HashSet<Tuple>();

    results.add( new Tuple( "1\t1" ) );
    results.add( new Tuple( "5\t5" ) );

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
   * null h1
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
   * null H1
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
   * null h1  null  H1
   *
   * @throws Exception
   */
  @Test
  public void testJoinInner() throws Exception
    {
    HashSet<Tuple> results = new HashSet<Tuple>();

    results.add( new Tuple( "1", "a1", "1", "A1" ) );
    results.add( new Tuple( "1", "a1", "1", "A2" ) );
    results.add( new Tuple( "1", "a1", "1", "A3" ) );
    results.add( new Tuple( "1", "a2", "1", "A1" ) );
    results.add( new Tuple( "1", "a2", "1", "A2" ) );
    results.add( new Tuple( "1", "a2", "1", "A3" ) );
    results.add( new Tuple( "1", "a3", "1", "A1" ) );
    results.add( new Tuple( "1", "a3", "1", "A2" ) );
    results.add( new Tuple( "1", "a3", "1", "A3" ) );
    results.add( new Tuple( "2", "b1", "2", "B1" ) );
    results.add( new Tuple( "2", "b1", "2", "B2" ) );
    results.add( new Tuple( "2", "b1", "2", "B3" ) );
    results.add( new Tuple( "4", "d1", "4", "D1" ) );
    results.add( new Tuple( "4", "d2", "4", "D1" ) );
    results.add( new Tuple( "4", "d3", "4", "D1" ) );
    results.add( new Tuple( null, "h1", null, "H1" ) );

    handleJoins( "joininner", new InnerJoin(), results );
    }

  /**
   * /**
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
   * null h1
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
   * null H1
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
   * null h1  null  H1
   *
   * @throws Exception
   */
  @Test
  public void testJoinOuter() throws Exception
    {
    // skip if hadoop cluster mode, outer joins don't behave the same
    if( getPlatform().isMapReduce() && getPlatform().isUseCluster() )
      return;

    Set<Tuple> results = new HashSet<Tuple>();

    results.add( new Tuple( "1", "a1", "1", "A1" ) );
    results.add( new Tuple( "1", "a1", "1", "A2" ) );
    results.add( new Tuple( "1", "a1", "1", "A3" ) );
    results.add( new Tuple( "1", "a2", "1", "A1" ) );
    results.add( new Tuple( "1", "a2", "1", "A2" ) );
    results.add( new Tuple( "1", "a2", "1", "A3" ) );
    results.add( new Tuple( "1", "a3", "1", "A1" ) );
    results.add( new Tuple( "1", "a3", "1", "A2" ) );
    results.add( new Tuple( "1", "a3", "1", "A3" ) );
    results.add( new Tuple( "2", "b1", "2", "B1" ) );
    results.add( new Tuple( "2", "b1", "2", "B2" ) );
    results.add( new Tuple( "2", "b1", "2", "B3" ) );
    results.add( new Tuple( "3", "c1", null, null ) );
    results.add( new Tuple( "4", "d1", "4", "D1" ) );
    results.add( new Tuple( "4", "d2", "4", "D1" ) );
    results.add( new Tuple( "4", "d3", "4", "D1" ) );
    results.add( new Tuple( "5", "e1", null, null ) );
    results.add( new Tuple( "5", "e2", null, null ) );
    results.add( new Tuple( "5", "e3", null, null ) );
    results.add( new Tuple( null, null, "6", "F1" ) );
    results.add( new Tuple( null, null, "6", "F2" ) );
    results.add( new Tuple( "7", "g1", null, null ) );
    results.add( new Tuple( "7", "g2", null, null ) );
    results.add( new Tuple( "7", "g3", null, null ) );
    results.add( new Tuple( "7", "g4", null, null ) );
    results.add( new Tuple( "7", "g5", null, null ) );
    results.add( new Tuple( null, "h1", null, "H1" ) );

    handleJoins( "joinouter", new OuterJoin(), results );
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
   * null h1
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
   * null H1
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
   * null h1	null	H1
   *
   * @throws Exception
   */
  @Test
  public void testJoinInnerOuter() throws Exception
    {
    Set<Tuple> results = new HashSet<Tuple>();

    results.add( new Tuple( "1", "a1", "1", "A1" ) );
    results.add( new Tuple( "1", "a1", "1", "A2" ) );
    results.add( new Tuple( "1", "a1", "1", "A3" ) );
    results.add( new Tuple( "1", "a2", "1", "A1" ) );
    results.add( new Tuple( "1", "a2", "1", "A2" ) );
    results.add( new Tuple( "1", "a2", "1", "A3" ) );
    results.add( new Tuple( "1", "a3", "1", "A1" ) );
    results.add( new Tuple( "1", "a3", "1", "A2" ) );
    results.add( new Tuple( "1", "a3", "1", "A3" ) );
    results.add( new Tuple( "2", "b1", "2", "B1" ) );
    results.add( new Tuple( "2", "b1", "2", "B2" ) );
    results.add( new Tuple( "2", "b1", "2", "B3" ) );
    results.add( new Tuple( "3", "c1", null, null ) );
    results.add( new Tuple( "4", "d1", "4", "D1" ) );
    results.add( new Tuple( "4", "d2", "4", "D1" ) );
    results.add( new Tuple( "4", "d3", "4", "D1" ) );
    results.add( new Tuple( "5", "e1", null, null ) );
    results.add( new Tuple( "5", "e2", null, null ) );
    results.add( new Tuple( "5", "e3", null, null ) );
    results.add( new Tuple( "7", "g1", null, null ) );
    results.add( new Tuple( "7", "g2", null, null ) );
    results.add( new Tuple( "7", "g3", null, null ) );
    results.add( new Tuple( "7", "g4", null, null ) );
    results.add( new Tuple( "7", "g5", null, null ) );
    results.add( new Tuple( null, "h1", null, "H1" ) );

    handleJoins( "joininnerouter", new LeftJoin(), results );
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
   * null h1
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
   * null H1
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
   * null h1	null	H1
   *
   * @throws Exception
   */
  @Test
  public void testJoinOuterInner() throws Exception
    {
    // skip if hadoop cluster mode, outer joins don't behave the same
    if( getPlatform().isMapReduce() && getPlatform().isUseCluster() )
      return;

    Set<Tuple> results = new HashSet<Tuple>();

    results.add( new Tuple( "1", "a1", "1", "A1" ) );
    results.add( new Tuple( "1", "a1", "1", "A2" ) );
    results.add( new Tuple( "1", "a1", "1", "A3" ) );
    results.add( new Tuple( "1", "a2", "1", "A1" ) );
    results.add( new Tuple( "1", "a2", "1", "A2" ) );
    results.add( new Tuple( "1", "a2", "1", "A3" ) );
    results.add( new Tuple( "1", "a3", "1", "A1" ) );
    results.add( new Tuple( "1", "a3", "1", "A2" ) );
    results.add( new Tuple( "1", "a3", "1", "A3" ) );
    results.add( new Tuple( "2", "b1", "2", "B1" ) );
    results.add( new Tuple( "2", "b1", "2", "B2" ) );
    results.add( new Tuple( "2", "b1", "2", "B3" ) );
    results.add( new Tuple( "4", "d1", "4", "D1" ) );
    results.add( new Tuple( "4", "d2", "4", "D1" ) );
    results.add( new Tuple( "4", "d3", "4", "D1" ) );
    results.add( new Tuple( null, null, "6", "F1" ) );
    results.add( new Tuple( null, null, "6", "F2" ) );
    results.add( new Tuple( null, "h1", null, "H1" ) );

    handleJoins( "joinouterinner", new RightJoin(), results );
    }

  private void handleJoins( String path, Joiner joiner, Set<Tuple> results ) throws Exception
    {
    getPlatform().copyFromLocal( inputFileLhsSparse );
    getPlatform().copyFromLocal( inputFileRhsSparse );

    Fields fields = new Fields( "num", "char" ).applyTypes( Integer.class, String.class );
    Tap sourceLower = getPlatform().getDelimitedFile( fields, " ", inputFileLhsSparse );
    Tap sourceUpper = getPlatform().getDelimitedFile( fields, " ", inputFileRhsSparse );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Tap sink = getPlatform().getDelimitedFile( Fields.size( 4, String.class ), "\t", getOutputPath( path ), SinkMode.REPLACE );

    Pipe pipeLower = new Pipe( "lower" );
    Pipe pipeUpper = new Pipe( "upper" );

    Fields declaredFields = new Fields( "num", "char", "num2", "char2" );
    Fields groupingFields = new Fields( "num" );

    Pipe splice = new HashJoin( pipeLower, groupingFields, pipeUpper, groupingFields, declaredFields, joiner );

    splice = new Each( splice, Fields.ALL, new Identity(), Fields.RESULTS );

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
  public void testJoinMixed() throws Exception
    {
    // skip if hadoop cluster mode, outer joins don't behave the same
    if( getPlatform().isMapReduce() && getPlatform().isUseCluster() )
      return;

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

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "joinmixed" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe pipeLowerOffset = new Each( new Pipe( "loweroffset" ), new Fields( "line" ), splitter );
    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );

    Pipe[] pipes = Pipe.pipes( pipeLowerOffset, pipeUpper, pipeLower );
    Fields[] fields = Fields.fields( new Fields( "num" ), new Fields( "num" ), new Fields( "num" ) );

    MixedJoin join = new MixedJoin( new boolean[]{MixedJoin.OUTER, MixedJoin.INNER, MixedJoin.OUTER} );
    Pipe splice = new HashJoin( pipes, fields, Fields.size( 6 ), join );

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
  public void testJoinDiffFields() throws Exception
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

    Pipe pipe = new HashJoin( pipeLower, new Fields( "numA" ), pipeUpper, new Fields( "numB" ) );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, pipe );

    flow.complete();

    validateLength( flow, 5 );

    List<Tuple> actual = getSinkAsList( flow );

    assertTrue( actual.contains( new Tuple( "1\ta\t1\tA" ) ) );
    assertTrue( actual.contains( new Tuple( "2\tb\t2\tB" ) ) );
    }

  @Test
  public void testJoinGroupBy() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    Tap sourceLower = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLower );
    Tap sourceUpper = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileUpper );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "joingroupby" ), SinkMode.REPLACE );

    Function splitterLower = new RegexSplitter( new Fields( "numA", "lower" ), " " );
    Function splitterUpper = new RegexSplitter( new Fields( "numB", "upper" ), " " );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitterLower );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitterUpper );

    Pipe pipe = new HashJoin( pipeLower, new Fields( "numA" ), pipeUpper, new Fields( "numB" ) );

    Pipe groupby = new GroupBy( pipe, new Fields( "numA" ) );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, groupby );

    flow.complete();

    validateLength( flow, 5, null );

    List<Tuple> actual = getSinkAsList( flow );

    assertTrue( actual.contains( new Tuple( "1\ta\t1\tA" ) ) );
    assertTrue( actual.contains( new Tuple( "2\tb\t2\tB" ) ) );
    }

  @Test
  public void testJoinSamePipe() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );

    Tap source = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLower );

    Map sources = new HashMap();

    sources.put( "lower", source );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "samepipe" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );

    Pipe pipe = new HashJoin( pipeLower, new Fields( "num" ), 1, new Fields( "num1", "char1", "num2", "char2" ) );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, pipe );

    flow.complete();

    validateLength( flow, 5, null );

    List<Tuple> actual = getSinkAsList( flow );

    assertTrue( actual.contains( new Tuple( "1\ta\t1\ta" ) ) );
    assertTrue( actual.contains( new Tuple( "2\tb\t2\tb" ) ) );
    }

  @Test
  public void testJoinSamePipe2() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );

    Tap source = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLower );

    Map sources = new HashMap();

    sources.put( "lower", source );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "samepipe2" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );

    Pipe join = new HashJoin( pipeLower, new Fields( "num" ), pipeLower, new Fields( "num" ), new Fields( "num1", "char1", "num2", "char2" ) );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, join );

    flow.complete();

    validateLength( flow, 5, null );

    List<Tuple> actual = getSinkAsList( flow );

    assertTrue( actual.contains( new Tuple( "1\ta\t1\ta" ) ) );
    assertTrue( actual.contains( new Tuple( "2\tb\t2\tb" ) ) );
    }

  @Test
  public void testJoinSamePipe3() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );

    Tap source = getPlatform().getDelimitedFile( new Fields( "num", "char" ), " ", inputFileLower );

    Map sources = new HashMap();

    sources.put( "lower", source );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "samepipe3" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "lower" );

    Pipe lhs = new Pipe( "lhs", pipe );
    Pipe rhs = new Pipe( "rhs", pipe );

    Pipe join = new HashJoin( lhs, new Fields( "num" ), rhs, new Fields( "num" ), new Fields( "num1", "char1", "num2", "char2" ) );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, join );

    flow.complete();

    validateLength( flow, 5, null );

    List<Tuple> actual = getSinkAsList( flow );

    assertTrue( actual.contains( new Tuple( "1\ta\t1\ta" ) ) );
    assertTrue( actual.contains( new Tuple( "2\tb\t2\tb" ) ) );
    }

  /**
   * Same source as rightmost
   * <p/>
   * should be a single job as the same file accumulates into the joins
   *
   * @throws Exception
   */
  @Test
  public void testJoinAroundJoinRightMost() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    Tap sourceLower = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLower );
    Tap sourceUpper = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileUpper );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper1", sourceUpper );
    sources.put( "upper2", sourceUpper );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "joinaroundjoinrightmost" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper1 = new Each( new Pipe( "upper1" ), new Fields( "line" ), splitter );
    Pipe pipeUpper2 = new Each( new Pipe( "upper2" ), new Fields( "line" ), splitter );

    Pipe splice1 = new HashJoin( pipeLower, new Fields( "num" ), pipeUpper1, new Fields( "num" ), new Fields( "num1", "char1", "num2", "char2" ) );

    splice1 = new Each( splice1, new Identity() );

    Pipe splice2 = new HashJoin( splice1, new Fields( "num1" ), pipeUpper2, new Fields( "num" ), new Fields( "num1", "char1", "num2", "char2", "num3", "char3" ) );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, splice2 );

//    flow.writeDOT( "joinaroundrightmost.dot" );

    if( getPlatform().isMapReduce() )
      assertEquals( "wrong number of steps", 1, flow.getFlowSteps().size() );

    flow.complete();

    validateLength( flow, 5, null );

    List<Tuple> actual = getSinkAsList( flow );

    assertTrue( actual.contains( new Tuple( "1\ta\t1\tA\t1\tA" ) ) );
    assertTrue( actual.contains( new Tuple( "2\tb\t2\tB\t2\tB" ) ) );
    }

  /**
   * Same source as leftmost
   *
   * @throws Exception
   */
  @Test
  public void testJoinAroundJoinLeftMost() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    Tap sourceLower = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLower );
    Tap sourceUpper = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileUpper );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper1", sourceUpper );
    sources.put( "upper2", sourceUpper );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "joinaroundjoinleftmost" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper1 = new Each( new Pipe( "upper1" ), new Fields( "line" ), splitter );
    Pipe pipeUpper2 = new Each( new Pipe( "upper2" ), new Fields( "line" ), splitter );

    Pipe splice1 = new HashJoin( pipeUpper1, new Fields( "num" ), pipeUpper2, new Fields( "num" ), new Fields( "num1", "char1", "num2", "char2" ) );

    splice1 = new Each( splice1, new Identity() );

    Pipe splice2 = new HashJoin( splice1, new Fields( "num1" ), pipeLower, new Fields( "num" ), new Fields( "num1", "char1", "num2", "char2", "num3", "char3" ) );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, splice2 );

//    flow.writeDOT( "joinaroundleftmost.dot" );

    if( getPlatform().isMapReduce() )
      assertEquals( "wrong number of steps", 2, flow.getFlowSteps().size() );

    flow.complete();

    validateLength( flow, 5, null );

    List<Tuple> actual = getSinkAsList( flow );

    assertTrue( actual.contains( new Tuple( "1\tA\t1\tA\t1\ta" ) ) );
    assertTrue( actual.contains( new Tuple( "2\tB\t2\tB\t2\tb" ) ) );
    }

  /**
   * Upper as leftmost and rightmost forcing two jobs
   *
   * @throws Exception
   */
  @Test
  public void testJoinAroundJoinRightMostSwapped() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    Tap sourceLower = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLower );
    Tap sourceUpper = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileUpper );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper1", sourceUpper );
    sources.put( "upper2", sourceUpper );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "joinaroundjoinswapped" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper1 = new Each( new Pipe( "upper1" ), new Fields( "line" ), splitter );
    Pipe pipeUpper2 = new Each( new Pipe( "upper2" ), new Fields( "line" ), splitter );

    Pipe splice1 = new HashJoin( pipeLower, new Fields( "num" ), pipeUpper1, new Fields( "num" ), new Fields( "num1", "char1", "num2", "char2" ) );

    splice1 = new Each( splice1, new Identity() );

    // upper2 becomes leftmost, forcing a tap between the joins
    Pipe splice2 = new HashJoin( pipeUpper2, new Fields( "num" ), splice1, new Fields( "num1" ), new Fields( "num1", "char1", "num2", "char2", "num3", "char3" ) );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, splice2 );

    if( getPlatform().isMapReduce() )
      assertEquals( "wrong number of steps", 2, flow.getFlowSteps().size() );

    flow.complete();

    validateLength( flow, 5, null );

    List<Tuple> actual = getSinkAsList( flow );

    assertTrue( actual.contains( new Tuple( "1\tA\t1\ta\t1\tA" ) ) );
    assertTrue( actual.contains( new Tuple( "2\tB\t2\tb\t2\tB" ) ) );
    }

  @Test
  public void testJoinGroupByJoin() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );
    getPlatform().copyFromLocal( inputFileJoined );

    Tap sourceLower = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLower );
    Tap sourceUpper = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileUpper );
    Tap sourceJoined = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileJoined );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );
    sources.put( "joined", sourceJoined );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "joingroupbyjoin" ), SinkMode.REPLACE );

    Function splitterLower = new RegexSplitter( new Fields( "numA", "lower" ), " " );
    Function splitterUpper = new RegexSplitter( new Fields( "numB", "upper" ), " " );
    Function splitterJoined = new RegexSplitter( new Fields( "numC", "lowerC", "upperC" ), "\t" );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitterLower );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitterUpper );
    Pipe pipeJoined = new Each( new Pipe( "joined" ), new Fields( "line" ), splitterJoined );

    Pipe pipe = new HashJoin( pipeLower, new Fields( "numA" ), pipeUpper, new Fields( "numB" ) );

    pipe = new GroupBy( pipe, new Fields( "numA" ) );

    pipe = new HashJoin( pipe, new Fields( "numA" ), pipeJoined, new Fields( "numC" ) );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, pipe );

    if( getPlatform().isMapReduce() )
      assertEquals( "wrong number of steps", 2, flow.getFlowSteps().size() );

    flow.complete();

    validateLength( flow, 5, null );

    List<Tuple> actual = getSinkAsList( flow );

    assertTrue( actual.contains( new Tuple( "1\ta\t1\tA\t1\ta\tA" ) ) );
    assertTrue( actual.contains( new Tuple( "2\tb\t2\tB\t2\tb\tB" ) ) );
    }

  /**
   * here the same file is fed into the same HashJoin.
   * <p/>
   * This is three jobs.
   * <p/>
   * a temp tap is inserted before the accumulated branch for two reasons on the common HashJoin
   * <p/>
   * it is assumed the accumulated side is filtered down, so pushing to disk will preserve io
   * if accumulated side was streamed instead via a fork, only part of the file will accumulate into the HashJoin
   * <p/>
   * /-T-\ <-- accumulated
   * T      HJ
   * \---/ <-- streamed
   *
   * @throws Exception
   */
  @Test
  public void testJoinSameSourceIntoJoin() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    Tap sourceLower = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLower );
    Tap sourceUpper = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileUpper );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper1", sourceUpper );
    sources.put( "upper2", sourceUpper );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "joinsamesourceintojoin" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper1 = new Each( new Pipe( "upper1" ), new Fields( "line" ), splitter );
    Pipe pipeUpper2 = new Each( new Pipe( "upper2" ), new Fields( "line" ), splitter );

    Pipe splice1 = new HashJoin( pipeUpper1, new Fields( "num" ), pipeUpper2, new Fields( "num" ), new Fields( "num1", "char1", "num2", "char2" ) );

    splice1 = new Each( splice1, new Identity() );

    Pipe splice2 = new HashJoin( pipeLower, new Fields( "num" ), splice1, new Fields( "num1" ), new Fields( "num1", "char1", "num2", "char2", "num3", "char3" ) );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, splice2 );

//    flow.writeDOT( "joinsamesourceintojoin.dot" );

    if( getPlatform().isMapReduce() )
      assertEquals( "wrong number of steps", 3, flow.getFlowSteps().size() );

    flow.complete();

    validateLength( flow, 5, null );

    List<Tuple> actual = getSinkAsList( flow );

    assertTrue( actual.contains( new Tuple( "1\ta\t1\tA\t1\tA" ) ) );
    assertTrue( actual.contains( new Tuple( "2\tb\t2\tB\t2\tB" ) ) );
    }

  /**
   * Tests that two independent streamed sources with loadable tributaries properly plan into a GroupBy
   * without loading unused sources
   *
   * @throws Exception
   */
  @Test
  public void testJoinsIntoGroupBy() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    getPlatform().copyFromLocal( inputFileLhs );
    getPlatform().copyFromLocal( inputFileRhs );

    Tap sourceLower = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLower );
    Tap sourceUpper = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileUpper );

    Tap sourceLhs = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLhs );
    Tap sourceRhs = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileRhs );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );
    sources.put( "lhs", sourceLhs );
    sources.put( "rhs", sourceRhs );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "joinsintogroupby" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );

    Pipe pipeLhs = new Each( new Pipe( "lhs" ), new Fields( "line" ), splitter );
    Pipe pipeRhs = new Each( new Pipe( "rhs" ), new Fields( "line" ), splitter );

    Pipe upperLower = new HashJoin( pipeLower, new Fields( "num" ), pipeUpper, new Fields( "num" ), new Fields( "num1", "char1", "num2", "char2" ) );

    upperLower = new Each( upperLower, new Identity() );

    Pipe lhsRhs = new HashJoin( pipeLhs, new Fields( "num" ), pipeRhs, new Fields( "num" ), new Fields( "num1", "char1", "num2", "char2" ) );

    lhsRhs = new Each( lhsRhs, new Identity() );

    Pipe grouped = new GroupBy( "merging", Pipe.pipes( upperLower, lhsRhs ), new Fields( "num1" ) );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, grouped );

    if( getPlatform().isMapReduce() )
      assertEquals( "wrong number of steps", 1, flow.getFlowSteps().size() );

    flow.complete();

    validateLength( flow, 42, null );

    List<Tuple> actual = getSinkAsList( flow );

    assertTrue( actual.contains( new Tuple( "1\ta\t1\tA" ) ) );
    assertTrue( actual.contains( new Tuple( "5\te\t5\tE" ) ) );
    }

  @Test
  public void testJoinSamePipeAroundGroupBy() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );

    Tap source = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLower );
    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "samepipearoundgroupby" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );

    Pipe lhsPipe = new Each( new Pipe( "lhs", pipeLower ), new Identity() );

    Pipe rhsPipe = new Each( new Pipe( "rhs", pipeLower ), new Identity() );

    rhsPipe = new GroupBy( rhsPipe, new Fields( "num" ) );

    rhsPipe = new Each( rhsPipe, new Identity() );

    Pipe pipe = new HashJoin( lhsPipe, new Fields( "num" ), rhsPipe, new Fields( "num" ), new Fields( "num1", "char1", "num2", "char2" ) );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 5, null );

    List<Tuple> actual = getSinkAsList( flow );

    assertTrue( actual.contains( new Tuple( "1\ta\t1\ta" ) ) );
    assertTrue( actual.contains( new Tuple( "2\tb\t2\tb" ) ) );
    }

  /**
   * This test results in two MR jobs because one join feeds into the accumulated side of the second. A mapper
   * can only stream on branch at a time forcing a temp file between the mappers. see next test for swapped join
   *
   * @throws Exception
   */
  @Test
  public void testJoinsIntoCoGroupLhs() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    getPlatform().copyFromLocal( inputFileLhs );
    getPlatform().copyFromLocal( inputFileRhs );

    Tap sourceLower = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLower );
    Tap sourceUpper = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileUpper );

    Tap sourceLhs = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLhs );
    Tap sourceRhs = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileRhs );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );
    sources.put( "lhs", sourceLhs );
    sources.put( "rhs", sourceRhs );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "joinsintocogrouplhs" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );

    Pipe pipeLhs = new Each( new Pipe( "lhs" ), new Fields( "line" ), splitter );
    Pipe pipeRhs = new Each( new Pipe( "rhs" ), new Fields( "line" ), splitter );

    Pipe upperLower = new HashJoin( pipeLower, new Fields( "num" ), pipeUpper, new Fields( "num" ), new Fields( "numUpperLower", "charUpperLower", "num2UpperLower", "char2UpperLower" ) );

    upperLower = new Each( upperLower, new Identity() );

    Pipe lhsUpperLower = new HashJoin( pipeLhs, new Fields( "num" ), upperLower, new Fields( "numUpperLower" ), new Fields( "numLhs", "charLhs", "numUpperLower", "charUpperLower", "num2UpperLower", "char2UpperLower" ) );

    lhsUpperLower = new Each( lhsUpperLower, new Identity() );

    Pipe grouped = new CoGroup( "cogrouping", lhsUpperLower, new Fields( "numLhs" ), pipeRhs, new Fields( "num" ) );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, grouped );

    if( getPlatform().isMapReduce() )
      assertEquals( "wrong number of steps", 2, flow.getFlowSteps().size() );

    flow.complete();

    validateLength( flow, 37, null );

    List<Tuple> actual = getSinkAsList( flow );

    assertTrue( actual.contains( new Tuple( "1\ta\t1\ta\t1\tA\t1\tA" ) ) );
    assertTrue( actual.contains( new Tuple( "5\ta\t5\te\t5\tE\t5\tA" ) ) );
    }

  /**
   * This test results in one MR jobs because one join feeds into the streamed side of the second.
   *
   * @throws Exception
   */
  @Test
  public void testJoinsIntoCoGroupLhsSwappedJoin() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    getPlatform().copyFromLocal( inputFileLhs );
    getPlatform().copyFromLocal( inputFileRhs );

    Tap sourceLower = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLower );
    Tap sourceUpper = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileUpper );

    Tap sourceLhs = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLhs );
    Tap sourceRhs = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileRhs );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );
    sources.put( "lhs", sourceLhs );
    sources.put( "rhs", sourceRhs );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "joinsintocogrouplhsswappedjoin" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );

    Pipe pipeLhs = new Each( new Pipe( "lhs" ), new Fields( "line" ), splitter );
    Pipe pipeRhs = new Each( new Pipe( "rhs" ), new Fields( "line" ), splitter );

    Pipe upperLower = new HashJoin( pipeLower, new Fields( "num" ), pipeUpper, new Fields( "num" ), new Fields( "numUpperLower", "charUpperLower", "num2UpperLower", "char2UpperLower" ) );

    upperLower = new Each( upperLower, new Identity() );

    Pipe lhsUpperLower = new HashJoin( upperLower, new Fields( "numUpperLower" ), pipeLhs, new Fields( "num" ), new Fields( "numUpperLower", "charUpperLower", "num2UpperLower", "char2UpperLower", "numLhs", "charLhs" ) );

    lhsUpperLower = new Each( lhsUpperLower, new Identity() );

    Pipe grouped = new CoGroup( "cogrouping", lhsUpperLower, new Fields( "numLhs" ), pipeRhs, new Fields( "num" ) );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, grouped );

    if( getPlatform().isMapReduce() )
      assertEquals( "wrong number of steps", 1, flow.getFlowSteps().size() );

    flow.complete();

    validateLength( flow, 37, null );

    List<Tuple> actual = getSinkAsList( flow );

    assertTrue( actual.contains( new Tuple( "1\ta\t1\tA\t1\ta\t1\tA" ) ) );
    assertTrue( actual.contains( new Tuple( "5\te\t5\tE\t5\te\t5\tE" ) ) );
    }

  @Test
  public void testJoinsIntoCoGroupRhs() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    getPlatform().copyFromLocal( inputFileLhs );
    getPlatform().copyFromLocal( inputFileRhs );

    Tap sourceLower = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLower );
    Tap sourceUpper = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileUpper );

    Tap sourceLhs = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLhs );
    Tap sourceRhs = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileRhs );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );
    sources.put( "lhs", sourceLhs );
    sources.put( "rhs", sourceRhs );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "joinsintocogrouprhs" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );

    Pipe pipeLhs = new Each( new Pipe( "lhs" ), new Fields( "line" ), splitter );
    Pipe pipeRhs = new Each( new Pipe( "rhs" ), new Fields( "line" ), splitter );

    Pipe upperLower = new HashJoin( pipeLower, new Fields( "num" ), pipeUpper, new Fields( "num" ), new Fields( "numUpperLower", "charUpperLower", "num2UpperLower", "char2UpperLower" ) );

    upperLower = new Each( upperLower, new Identity() );

    Pipe lhsUpperLower = new HashJoin( pipeLhs, new Fields( "num" ), upperLower, new Fields( "numUpperLower" ), new Fields( "numLhs", "charLhs", "numUpperLower", "charUpperLower", "num2UpperLower", "char2UpperLower" ) );

    lhsUpperLower = new Each( lhsUpperLower, new Identity() );

    Pipe grouped = new CoGroup( "cogrouping", pipeRhs, new Fields( "num" ), lhsUpperLower, new Fields( "numLhs" ) );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, grouped );

    if( getPlatform().isMapReduce() )
      assertEquals( "wrong number of steps", 2, flow.getFlowSteps().size() );

    flow.complete();

    validateLength( flow, 37, null );

    List<Tuple> actual = getSinkAsList( flow );

    assertTrue( actual.contains( new Tuple( "1\tA\t1\ta\t1\ta\t1\tA" ) ) );
    assertTrue( actual.contains( new Tuple( "5\tE\t5\te\t5\te\t5\tE" ) ) );
    }

  @Test
  public void testJoinsIntoCoGroup() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    getPlatform().copyFromLocal( inputFileLhs );
    getPlatform().copyFromLocal( inputFileRhs );

    Tap sourceLower = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLower );
    Tap sourceUpper = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileUpper );

    Tap sourceLhs = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLhs );
    Tap sourceRhs = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileRhs );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );
    sources.put( "lhs", sourceLhs );
    sources.put( "rhs", sourceRhs );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "joinsintocogroup" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );

    Pipe pipeLhs = new Each( new Pipe( "lhs" ), new Fields( "line" ), splitter );
    Pipe pipeRhs = new Each( new Pipe( "rhs" ), new Fields( "line" ), splitter );

    Pipe upperLower = new HashJoin( pipeLower, new Fields( "num" ), pipeUpper, new Fields( "num" ), new Fields( "numUpperLower1", "charUpperLower1", "numUpperLower2", "charUpperLower2" ) );

    upperLower = new Each( upperLower, new Identity() );

    Pipe lhsRhs = new HashJoin( pipeLhs, new Fields( "num" ), pipeRhs, new Fields( "num" ), new Fields( "numLhsRhs1", "charLhsRhs1", "numLhsRhs2", "charLhsRhs2" ) );

    lhsRhs = new Each( lhsRhs, new Identity() );

    Pipe grouped = new CoGroup( "cogrouping", upperLower, new Fields( "numUpperLower1" ), lhsRhs, new Fields( "numLhsRhs1" ) );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, grouped );

    if( getPlatform().isMapReduce() )
      assertEquals( "wrong number of steps", 1, flow.getFlowSteps().size() );

    flow.complete();

    validateLength( flow, 37, null );

    List<Tuple> actual = getSinkAsList( flow );

    assertTrue( actual.contains( new Tuple( "1\ta\t1\tA\t1\ta\t1\tA" ) ) );
    assertTrue( actual.contains( new Tuple( "5\te\t5\tE\t5\te\t5\tE" ) ) );
    }

  public static class AllComparator implements Comparator<Comparable>, Hasher<Comparable>, Serializable
    {

    @Override
    public int compare( Comparable lhs, Comparable rhs )
      {
      return lhs.toString().compareTo( rhs.toString() );
      }

    @Override
    public int hashCode( Comparable value )
      {
      if( value == null )
        return 0;

      return value.toString().hashCode();
      }
    }

  /**
   * Tests Hasher being honored even if default comparator is null.
   *
   * @throws Exception
   */
  @Test
  public void testJoinWithHasher() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    Tap sourceLower = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLower );
    Tap sourceUpper = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileUpper );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "joinhasher" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );

    pipeLower = new Each( pipeLower, new Fields( "num" ), new ExpressionFunction( Fields.ARGS, "Integer.parseInt( num )", String.class ), Fields.REPLACE );

    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );

    Fields num = new Fields( "num" );
    num.setComparator( "num", new AllComparator() );

    Pipe splice = new HashJoin( pipeLower, num, pipeUpper, new Fields( "num" ), Fields.size( 4 ) );

    Map<Object, Object> properties = getProperties();

    Flow flow = getPlatform().getFlowConnector( properties ).connect( sources, sink, splice );

    flow.complete();

    validateLength( flow, 5 );

    List<Tuple> values = getSinkAsList( flow );

    assertTrue( values.contains( new Tuple( "1\ta\t1\tA" ) ) );
    assertTrue( values.contains( new Tuple( "2\tb\t2\tB" ) ) );
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

    Pipe splice = new HashJoin( pipeLower, Fields.NONE, pipeUpper, Fields.NONE, Fields.size( 4 ) );

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