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

package cascading.flow.hadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import cascading.CascadingTestCase;
import cascading.TestBuffer;
import cascading.TestFunction;
import cascading.flow.Flow;
import cascading.flow.FlowConnectorProps;
import cascading.flow.FlowElement;
import cascading.flow.FlowException;
import cascading.flow.FlowStep;
import cascading.flow.planner.BaseFlowStep;
import cascading.flow.planner.PlannerException;
import cascading.flow.planner.Scope;
import cascading.operation.AssertionLevel;
import cascading.operation.Function;
import cascading.operation.Identity;
import cascading.operation.aggregator.Count;
import cascading.operation.aggregator.First;
import cascading.operation.aggregator.MaxValue;
import cascading.operation.assertion.AssertNotNull;
import cascading.operation.assertion.AssertNull;
import cascading.operation.expression.ExpressionFilter;
import cascading.operation.regex.RegexFilter;
import cascading.operation.regex.RegexParser;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.HashJoin;
import cascading.pipe.Pipe;
import cascading.pipe.joiner.InnerJoin;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.SequenceFile;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.hadoop.util.TempHfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import org.jgrapht.alg.DijkstraShortestPath;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.junit.Test;

public class BuildJobsHadoopTest extends CascadingTestCase
  {
  public BuildJobsHadoopTest()
    {
    }

  /**
   * Test a single piece Pipe, should not fail, inserts Identity pipe
   *
   * @throws IOException
   */
  @Test
  public void testIdentity() throws Exception
    {
    Tap source = new Hfs( new TextLine(), "input/path" );
    Tap sink = new Hfs( new TextLine(), "output/path", true );

    Pipe pipe = new Pipe( "test" );

    Flow flow = new HadoopFlowConnector().connect( source, sink, pipe );

    List<FlowStep> steps = flow.getFlowSteps();

    assertEquals( "wrong size", 1, steps.size() );

    HadoopFlowStep step = (HadoopFlowStep) steps.get( 0 );

    assertEquals( "not equal: step.sources.size()", 1, step.getSources().size() );
    assertNull( "not null: step.groupBy", step.getGroup() );
    assertNotNull( "null: step.sink", step.getSink() );
    }

  @Test
  public void testName()
    {
    Pipe count = new Pipe( "count" );
    Pipe pipe = new GroupBy( count, new Fields( 1 ) );
    pipe = new Every( pipe, new Fields( 1 ), new Count(), new Fields( 0, 1 ) );

    assertEquals( "not equal: count.getName()", "count", count.getName() );
    assertEquals( "not equal: pipe.getName()", "count", pipe.getName() );

    pipe = new Each( count, new Fields( 1 ), new RegexSplitter( Fields.size( 2 ) ) );
    assertEquals( "not equal: pipe.getName()", "count", pipe.getName() );
    }

  @Test
  public void testOneJob() throws IOException
    {
    Map sources = new HashMap();
    Map sinks = new HashMap();

    sources.put( "count", new Hfs( new Fields( "first", "second" ), "input/path" ) );
    sinks.put( "count", new Hfs( new Fields( 0, 1 ), "output/path" ) );

    Pipe pipe = new Pipe( "count" );
    pipe = new GroupBy( pipe, new Fields( 1 ) );
    pipe = new Every( pipe, new Fields( 1 ), new Count(), new Fields( 0, 1 ) );

    List steps = new HadoopFlowConnector().connect( sources, sinks, pipe ).getFlowSteps();

    assertEquals( "wrong size", 1, steps.size() );

    BaseFlowStep step = (BaseFlowStep) steps.get( 0 );

    assertEquals( "not equal: step.sources.size()", 1, step.getSources().size() );
    assertNotNull( "null: step.groupBy", step.getGroup() );
    assertNotNull( "null: step.sink", step.getSink() );

    int mapDist = countDistance( step.getGraph(), (FlowElement) step.getSources().iterator().next(), step.getGroup() );
    assertEquals( "not equal: mapDist", 0, mapDist );

    int reduceDist = countDistance( step.getGraph(), step.getGroup(), step.getSink() );
    assertEquals( "not equal: reduceDist", 1, reduceDist );
    }

  @Test
  public void testOneJob2() throws IOException
    {
    Map sources = new HashMap();
    Map sinks = new HashMap();

    sources.put( "count", new Hfs( new Fields( "first", "second" ), "input/path" ) );
    sinks.put( "count", new Hfs( new Fields( 0, 1 ), "output/path" ) );

    Pipe pipe = new Pipe( "count" );
    pipe = new Each( pipe, new Fields( 1 ), new Identity(), new Fields( 2 ) ); // in:second out:all
    pipe = new Each( pipe, new Fields( 0 ), new Identity( new Fields( "_all" ) ), new Fields( 1 ) ); // in:all out:_all
    pipe = new GroupBy( pipe, new Fields( 0 ) ); // in:_all out:_all
    pipe = new Every( pipe, new Fields( 0 ), new Count(), new Fields( 0, 1 ) ); // in:_all out:_all,count

    List steps = new HadoopFlowConnector().connect( sources, sinks, pipe ).getFlowSteps();

    assertEquals( "wrong size", 1, steps.size() );

    BaseFlowStep step = (BaseFlowStep) steps.get( 0 );

    assertEquals( "not equal: step.sources.size()", 1, step.getSources().size() );
    assertNotNull( "null: step.groupBy", step.getGroup() );
    assertNotNull( "null: step.sink", step.getSink() );

    int mapDist = countDistance( step.getGraph(), (FlowElement) step.getSources().iterator().next(), step.getGroup() );
    assertEquals( "not equal: mapDist", 2, mapDist );

    int reduceDist = countDistance( step.getGraph(), step.getGroup(), step.getSink() );
    assertEquals( "not equal: reduceDist", 1, reduceDist );
    }

  @Test
  public void testOneJob3() throws IOException
    {
    Map sources = new HashMap();
    Map sinks = new HashMap();

    sources.put( "a", new Hfs( new Fields( "first", "second" ), "input/path/a" ) );
    sources.put( "b", new Hfs( new Fields( "third", "fourth" ), "input/path/b" ) );

    Pipe pipeA = new Pipe( "a" );
    Pipe pipeB = new Pipe( "b" );

    Pipe splice = new CoGroup( pipeA, new Fields( 1 ), pipeB, new Fields( 1 ) );

    sinks.put( splice.getName(), new Hfs( new Fields( 0, 1 ), "output/path" ) );

    List steps = new HadoopFlowConnector().connect( sources, sinks, splice ).getFlowSteps();

    assertEquals( "wrong size", 1, steps.size() );

    BaseFlowStep step = (BaseFlowStep) steps.get( 0 );

    assertEquals( "not equal: step.sources.size()", 2, step.getSources().size() );
    assertNotNull( "null: step.groupBy", step.getGroup() );
    assertNotNull( "null: step.sink", step.getSink() );

    Iterator<Tap> iterator = step.getSources().iterator();
    int mapDist = countDistance( step.getGraph(), iterator.next(), step.getGroup() );
    assertEquals( "not equal: mapDist", 0, mapDist );
    mapDist = countDistance( step.getGraph(), iterator.next(), step.getGroup() );
    assertEquals( "not equal: mapDist", 0, mapDist );

    int reduceDist = countDistance( step.getGraph(), step.getGroup(), step.getSink() );
    assertEquals( "not equal: reduceDist", 0, reduceDist );
    }

  @Test
  public void testOneJob4() throws IOException
    {
    Map sources = new HashMap();
    Map sinks = new HashMap();

    sources.put( "a", new Hfs( new Fields( "first", "second" ), "input/path/a" ) );
    sources.put( "b", new Hfs( new Fields( "third", "fourth" ), "input/path/b" ) );

    Pipe pipeA = new Pipe( "a" );
    Pipe pipeB = new Pipe( "b" );

    Pipe cogroup = new CoGroup( pipeA, new Fields( 1 ), pipeB, new Fields( 1 ) );

    cogroup = new Each( cogroup, new Identity() );

    sinks.put( cogroup.getName(), new Hfs( new Fields( 0, 1 ), "output/path" ) );

    List steps = new HadoopFlowConnector().connect( sources, sinks, cogroup ).getFlowSteps();

    assertEquals( "wrong size", 1, steps.size() );

    BaseFlowStep step = (BaseFlowStep) steps.get( 0 );

    assertEquals( "not equal: step.sources.size()", 2, step.getSources().size() );
    assertNotNull( "null: step.groupBy", step.getGroup() );
    assertNotNull( "null: step.sink", step.getSink() );

    int mapDist = countDistance( step.getGraph(), (FlowElement) step.getSources().iterator().next(), step.getGroup() );
    assertEquals( "not equal: mapDist", 0, mapDist );

    int reduceDist = countDistance( step.getGraph(), step.getGroup(), step.getSink() );
    assertEquals( "not equal: reduceDist", 1, reduceDist );
    }

  @Test
  public void testOneJob5() throws IOException
    {
    Map sources = new HashMap();
    Map sinks = new HashMap();

    sources.put( "a", new Hfs( new Fields( "first", "second" ), "input/path/a" ) );
    sources.put( "b", new Hfs( new Fields( "third", "fourth" ), "input/path/b" ) );

    Pipe pipeA = new Pipe( "a" );
    Pipe pipeB = new Pipe( "b" );

    Pipe splice = new CoGroup( pipeA, pipeB );

    splice = new Each( splice, new Identity() );

    sinks.put( splice.getName(), new Hfs( new TextLine(), "output/path" ) );

    List steps = new HadoopFlowConnector().connect( sources, sinks, splice ).getFlowSteps();

    assertEquals( "wrong size", 1, steps.size() );

    BaseFlowStep step = (BaseFlowStep) steps.get( 0 );

    assertEquals( "not equal: step.sources.size()", 2, step.getSources().size() );
    assertNotNull( "null: step.groupBy", step.getGroup() );
    assertNotNull( "null: step.sink", step.getSink() );

    int mapDist = countDistance( step.getGraph(), (FlowElement) step.getSources().iterator().next(), step.getGroup() );
    assertEquals( "not equal: mapDist", 0, mapDist );

    int reduceDist = countDistance( step.getGraph(), step.getGroup(), step.getSink() );
    assertEquals( "not equal: reduceDist", 1, reduceDist );
    }

  @Test
  public void testNoGroup() throws IOException
    {
    Map sources = new HashMap();
    Map sinks = new HashMap();

    sources.put( "count", new Hfs( new Fields( "first", "second" ), "input/path" ) );
    sinks.put( "count", new Hfs( new Fields( 0, 1 ), "output/path" ) );

    Pipe pipe = new Pipe( "count" );
    pipe = new Each( pipe, new Identity() );
    pipe = new Every( pipe, new Fields( 1 ), new Count(), new Fields( 0, 1 ) );

    try
      {
      Flow flow = new HadoopFlowConnector().connect( sources, sinks, pipe );
      fail( "did not throw flow exception" );
      }
    catch( Exception exception )
      {
      // ignore
//      exception.printStackTrace();
      }
    }

  /** This should result in only two steps, one for each side */
  @Test
  public void testSplit()
    {
    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), "foo" );
    Tap sink1 = new Hfs( new TextLine(), "foo/split1", true );
    Tap sink2 = new Hfs( new TextLine(), "foo/split2", true );

    Pipe pipe = new Pipe( "split" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexFilter( "^68.*" ) );

    Pipe left = new Each( new Pipe( "left", pipe ), new Fields( "line" ), new RegexFilter( ".*46.*" ) );
    Pipe right = new Each( new Pipe( "right", pipe ), new Fields( "line" ), new RegexFilter( ".*192.*" ) );

    Map sources = new HashMap();
    sources.put( "split", source );

    Map sinks = new HashMap();
    sinks.put( "left", sink1 );
    sinks.put( "right", sink2 );

    Flow flow = new HadoopFlowConnector().connect( sources, sinks, left, right );

//    flow.writeDOT( "safesplit.dot" );

    List<FlowStep> steps = flow.getFlowSteps();

    assertEquals( "not equal: steps.size()", 2, steps.size() );
    }

  /** this test verifies that the planner recognizes there are fewer tails than sinks. */
  @Test
  public void testSplitHangingTails()
    {
    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), "foo" );
    Tap sink1 = new Hfs( new TextLine(), "foo/split1", true );
    Tap sink2 = new Hfs( new TextLine(), "foo/split2", true );

    Pipe pipe = new Pipe( "split" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexFilter( "^68.*" ) );


    // this is ok
    Pipe left = new Each( new Pipe( "left", pipe ), new Fields( "line" ), new RegexFilter( ".*46.*" ) );
    Pipe right = new Each( new Pipe( "right", pipe ), new Fields( "line" ), new RegexFilter( ".*192.*" ) );

    Map sources = new HashMap();
    sources.put( "split", source );

    Map sinks = new HashMap();
    sinks.put( "left", sink1 );
    sinks.put( "right", sink2 );

    try
      {
      new HadoopFlowConnector().connect( sources, sinks, pipe );
      fail( "did not catch missing tails" );
      }
    catch( Exception exception )
      {
      System.out.println( "exception.getMessage() = " + exception.getMessage() );
      assertTrue( exception.getMessage().contains( "'left', 'right'" ) );
      }
    }

  @Test
  public void testSplitOnNonSafeOperations()
    {
    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), "foo" );
    Tap sink1 = new Hfs( new TextLine(), "foo/split1", true );
    Tap sink2 = new Hfs( new TextLine(), "foo/split2", true );

    Pipe pipe = new Pipe( "split" );

    // this operation is not safe
    pipe = new Each( pipe, new Fields( "line" ), new TestFunction( new Fields( "ignore" ), new Tuple( 1 ), false ), new Fields( "line" ) );

    pipe = new Each( pipe, new Fields( "line" ), new RegexFilter( "^68.*" ) );

    Pipe left = new Each( new Pipe( "left", pipe ), new Fields( "line" ), new RegexFilter( ".*46.*" ) );
    Pipe right = new Each( new Pipe( "right", pipe ), new Fields( "line" ), new RegexFilter( ".*192.*" ) );

    Map sources = new HashMap();
    sources.put( "split", source );

    Map sinks = new HashMap();
    sinks.put( "left", sink1 );
    sinks.put( "right", sink2 );

    Flow flow = new HadoopFlowConnector().connect( sources, sinks, left, right );

    List<FlowStep> steps = flow.getFlowSteps();

    assertEquals( "not equal: steps.size()", 3, steps.size() );

    FlowStep step = steps.get( 0 );

    assertEquals( "wrong number of operations", 2, ( (BaseFlowStep) step ).getAllOperations().size() );
    }

  // verify unsafe splits happen when splitting on a pipe
  @Test
  public void testSplitOnNonSafeOperations2()
    {
    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), "foo" );
    Tap sink1 = new Hfs( new TextLine(), "foo/split1", true );
    Tap sink2 = new Hfs( new TextLine(), "foo/split2", true );
    Tap sink3 = new Hfs( new TextLine(), "foo/split3", true );

    Pipe pipe = new Pipe( "split" );

    // this operation is not safe
    pipe = new Each( pipe, new Fields( "line" ), new TestFunction( new Fields( "ignore" ), new Tuple( 1 ), false ), new Fields( "line" ) );

    pipe = new Each( pipe, new Fields( "line" ), new RegexFilter( "^68.*" ) );

    pipe = new Pipe( "middle", pipe );

    Pipe left = new Each( new Pipe( "left", pipe ), new Fields( "line" ), new RegexFilter( ".*46.*" ) );
    Pipe right = new Each( new Pipe( "right", pipe ), new Fields( "line" ), new RegexFilter( ".*192.*" ) );

    Map sources = new HashMap();
    sources.put( "split", source );

    Map sinks = new HashMap();
    sinks.put( "left", sink1 );
    sinks.put( "right", sink2 );
    sinks.put( "middle", sink3 );

    Flow flow = new HadoopFlowConnector().connect( sources, sinks, left, right );

    List<FlowStep> steps = flow.getFlowSteps();

    assertEquals( "not equal: steps.size()", 4, steps.size() );

    FlowStep step = steps.get( 0 );

    assertEquals( "wrong number of operations", 2, ( (BaseFlowStep) step ).getAllOperations().size() );
    }

  /**
   * This should result in a Temp Tap after the Each split.
   * <p/>
   * We previously would push the each to the next step, but if there is already data being written, save the cpu.
   */
  @Test
  public void testSplitComplex()
    {
    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), "foo" );
    Tap sink1 = new Hfs( new TextLine(), "foo/split1", true );
    Tap sink2 = new Hfs( new TextLine(), "foo/split2", true );

    Pipe pipe = new Pipe( "split" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    pipe = new GroupBy( pipe, new Fields( "ip" ) );

    pipe = new Every( pipe, new Fields( "ip" ), new Count(), new Fields( "ip", "count" ) );

    pipe = new Each( pipe, new Fields( "ip" ), new RegexFilter( "^68.*" ) );

    Pipe left = new Each( new Pipe( "left", pipe ), new Fields( "ip" ), new RegexFilter( ".*46.*" ) );

    Pipe right = new Each( new Pipe( "right", pipe ), new Fields( "ip" ), new RegexFilter( ".*192.*" ) );

    Map sources = new HashMap();
    sources.put( "split", source );

    Map sinks = new HashMap();
    sinks.put( "left", sink1 );
    sinks.put( "right", sink2 );

    Flow flow = new HadoopFlowConnector().connect( sources, sinks, left, right );

//    flow.writeDOT( "splitcomplex.dot" );

    List<FlowStep> steps = flow.getFlowSteps();

    assertEquals( "not equal: steps.size()", 3, steps.size() );

    BaseFlowStep step = (BaseFlowStep) steps.get( 0 );

    Scope nextScope = step.getNextScope( step.getGroup() );
    FlowElement operator = step.getNextFlowElement( nextScope );

    assertTrue( "not an Every", operator instanceof Every );

    nextScope = step.getNextScope( operator );
    operator = step.getNextFlowElement( nextScope );

    assertTrue( "not a Each", operator instanceof Each );

    nextScope = step.getNextScope( operator );
    operator = step.getNextFlowElement( nextScope );

    assertTrue( "not a TempHfs", operator instanceof TempHfs );
    }

  /** same as splitComplex, except pipe/branch naming is after the Each, not before */
  @Test
  public void testSplitComplex2()
    {
    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), "foo" );
    Tap sink1 = new Hfs( new TextLine(), "foo/split1", true );
    Tap sink2 = new Hfs( new TextLine(), "foo/split2", true );

    Pipe pipe = new Pipe( "split" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    pipe = new GroupBy( pipe, new Fields( "ip" ) );

    pipe = new Every( pipe, new Fields( "ip" ), new Count(), new Fields( "ip", "count" ) );

    pipe = new Each( pipe, new Fields( "ip" ), new RegexFilter( "^68.*" ) );

    Pipe left = new Each( pipe, new Fields( "ip" ), new RegexFilter( ".*46.*" ) );

    left = new Pipe( "left", left );

    Pipe right = new Each( pipe, new Fields( "ip" ), new RegexFilter( ".*192.*" ) );

    right = new Pipe( "right", right );

    Map sources = new HashMap();
    sources.put( "split", source );

    Map sinks = new HashMap();
    sinks.put( "left", sink1 );
    sinks.put( "right", sink2 );

    Flow flow = new HadoopFlowConnector().connect( sources, sinks, left, right );

    List<FlowStep> steps = flow.getFlowSteps();

    assertEquals( "not equal: steps.size()", 3, steps.size() );

    BaseFlowStep step = (BaseFlowStep) steps.get( 0 );

    Scope nextScope = step.getNextScope( step.getGroup() );
    FlowElement operator = step.getNextFlowElement( nextScope );

    assertTrue( "not an Every", operator instanceof Every );

    nextScope = step.getNextScope( operator );
    operator = step.getNextFlowElement( nextScope );

    assertTrue( "not a Each", operator instanceof Each );

    nextScope = step.getNextScope( operator );
    operator = step.getNextFlowElement( nextScope );

    assertTrue( "not a TempHfs", operator instanceof TempHfs );
    }

  @Test
  public void testMerge()
    {
    Tap source1 = new Hfs( new TextLine( new Fields( "offset", "line" ) ), "foo/merge1" );
    Tap source2 = new Hfs( new TextLine( new Fields( "offset", "line" ) ), "foo/merge2" );

    Tap sink = new Hfs( new TextLine(), "foo" );

    Pipe left = new Each( new Pipe( "left" ), new Fields( "line" ), new RegexFilter( ".*46.*" ) );
    Pipe right = new Each( new Pipe( "right" ), new Fields( "line" ), new RegexFilter( ".*192.*" ) );

    Pipe merge = new GroupBy( "merge", Pipe.pipes( left, right ), new Fields( "offset" ) );

    Map sources = new HashMap();
    sources.put( "left", source1 );
    sources.put( "right", source2 );

    Map sinks = new HashMap();
    sinks.put( "merge", sink );

    Flow flow = new HadoopFlowConnector().connect( sources, sinks, merge );

    List<FlowStep> steps = flow.getFlowSteps();

    assertEquals( "not equal: steps.size()", 1, steps.size() );
    }

  @Test
  public void testDupeSource()
    {
    Tap source1 = new Hfs( new TextLine( new Fields( "offset", "line" ) ), "foo/merge" );
    Tap source2 = new Hfs( new TextLine( new Fields( "offset", "line" ) ), "foo/merge" );

    Tap sink = new Hfs( new TextLine(), "foo" );

    Pipe left = new Each( new Pipe( "left" ), new Fields( "line" ), new RegexFilter( ".*46.*" ) );
    Pipe right = new Each( new Pipe( "right" ), new Fields( "line" ), new RegexFilter( ".*192.*" ) );
    right = new Each( right, new Fields( "line" ), new RegexFilter( ".*192.*" ) );
    right = new Each( right, new Fields( "line" ), new RegexFilter( ".*192.*" ) );
    right = new Each( right, new Fields( "line" ), new RegexFilter( ".*192.*" ) );

    Pipe merge = new GroupBy( "merge", Pipe.pipes( left, right ), new Fields( "offset" ) );

    Map sources = new HashMap();
    sources.put( "left", source1 );
    sources.put( "right", source2 );

    Map sinks = new HashMap();
    sinks.put( "merge", sink );

    Flow flow = new HadoopFlowConnector().connect( sources, sinks, merge );

    List<FlowStep> steps = flow.getFlowSteps();

    assertEquals( "not equal: steps.size()", 1, steps.size() );
    }

  @Test
  public void testDupeSourceRepeat()
    {
    Tap source1 = new Hfs( new TextLine( new Fields( "offset", "line" ) ), "foo/merge" );

    Tap sink = new Hfs( new TextLine(), "foo" );

    Pipe pipe = new Pipe( "pipe" );

    Pipe merge = new CoGroup( "cogroup", pipe, new Fields( "offset" ), 1, Fields.size( 4 ) );

    Map sources = new HashMap();
    sources.put( "pipe", source1 );

    Map sinks = new HashMap();
    sinks.put( "cogroup", sink );

    Flow flow = new HadoopFlowConnector().connect( sources, sinks, merge );

    List<FlowStep> steps = flow.getFlowSteps();

    assertEquals( "not equal: steps.size()", 1, steps.size() );
    }

  @Test
  public void testDupeSource2()
    {
    Tap source1 = new Hfs( new TextLine( new Fields( "offset", "line" ) ), "foo/merge" );

    Tap sink = new Hfs( new TextLine(), "foo" );

    Pipe left = new Pipe( "left" );
    Pipe right = new Pipe( "right" );

    Pipe merge = new CoGroup( "cogroup", left, new Fields( "offset" ), right, new Fields( "offset" ), Fields.size( 4 ) );

    Map sources = new HashMap();
    sources.put( "left", source1 );
    sources.put( "right", source1 );

    Map sinks = new HashMap();
    sinks.put( "cogroup", sink );

    try
      {
      Flow flow = new HadoopFlowConnector().connect( sources, sinks, merge );
//    flow.writeDOT( "dupesource.dot" );
      fail( "did not throw planner exception" );
      }
    catch( Exception exception )
      {

      }
    }

  @Test
  public void testDupeSource3()
    {
    Tap source1 = new Hfs( new TextLine( new Fields( "offset", "line" ) ), "foo/merge" );
    Tap source2 = new Hfs( new TextLine( new Fields( "offset", "line" ) ), "bar/merge" );

    Tap sink = new Hfs( new TextLine(), "foo" );

    Pipe left = new Pipe( "left" );
    Pipe middle = new Pipe( "middle" );
    Pipe right = new Pipe( "right" );

    Pipe[] pipes = Pipe.pipes( left, middle, right );
    Fields[] fields = Fields.fields( new Fields( "offset" ), new Fields( "offset" ), new Fields( "offset" ) );

    Pipe merge = new CoGroup( "cogroup", pipes, fields, Fields.size( 6 ) );

    Map sources = new HashMap();
    sources.put( "left", source1 );
    sources.put( "middle", source2 );
    sources.put( "right", source1 );

    Map sinks = new HashMap();
    sinks.put( "cogroup", sink );

    try
      {
      Flow flow = new HadoopFlowConnector().connect( sources, sinks, merge );
      fail( "did not throw planner exception" );
      }
    catch( PlannerException exception )
      {
//      exception.printStackTrace();
      }
    }

//  public void testEquivalentPaths()
//    {
//    Tap source1 = new Hfs( new TextLine( new Fields( "offset", "line" ) ), "foo/merge" );
//    Tap source2 = new Hfs( new TextLine( new Fields( "offset", "line" ) ), "foo/merge" );
//
//    Tap sink = new Hfs( new TextLine(), "foo" );
//
//    Pipe left = new Each( new Pipe( "left" ), new Fields( "line" ), new RegexFilter( ".*46.*" ) );
//    Pipe right = new Each( new Pipe( "right" ), new Fields( "line" ), new RegexFilter( ".*46.*" ) );
//
//    Pipe join = new CoGroup( "cogroup", left, new Fields( "offset" ), right, new Fields( "offset" ), Fields.size( 4 ) );
//
//    Map sources = new HashMap();
//    sources.put( "left", source1 );
//    sources.put( "right", source2 );
//
//    Map sinks = new HashMap();
//    sinks.put( "cogroup", sink );
//
//    Flow flow = new FlowConnector().connect( sources, sinks, join );
//    flow.writeDOT( "identicalpaths.dot" );
//
//    List<FlowStep> steps = flow.getSteps();
//
//    assertEquals( "not equal: steps.size()", 1, steps.size() );
//
//    FlowStep step = steps.get( 0 );
//    System.out.println( "size: " + step.sources.size() );
//
//    System.out.println( "size: " + step.getNextScopes( step.sources.keySet().iterator().next()).size() );
//    }

  @Test
  public void testMerge2()
    {
    Tap source1 = new Hfs( new TextLine( new Fields( "offset", "line" ) ), "foo/merge1" );
    Tap source2 = new Hfs( new SequenceFile( new Fields( "offset", "line" ) ), "foo/merge2" );

    Tap sink = new Hfs( new TextLine(), "foo" );

    Pipe left = new Each( new Pipe( "left" ), new Fields( "line" ), new RegexFilter( ".*46.*" ) );
    Pipe right = new Each( new Pipe( "right" ), new Fields( "line" ), new RegexFilter( ".*192.*" ) );

    Pipe merge = new GroupBy( "merge", Pipe.pipes( left, right ), new Fields( "offset" ) );

    Map sources = new HashMap();
    sources.put( "left", source1 );
    sources.put( "right", source2 );

    Map sinks = new HashMap();
    sinks.put( "merge", sink );

    Flow flow = new HadoopFlowConnector().connect( sources, sinks, merge );

    List<FlowStep> steps = flow.getFlowSteps();

    assertEquals( "not equal: steps.size()", 1, steps.size() );
    }

  /**
   * Tests the case where the same source is split, then re-merged
   * <p/>
   * todo: planner should keep as a single mr job
   */
  @Test
  public void testMergeSameSourceSplit()
    {
    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), "foo/merge1" );

    Tap sink = new Hfs( new TextLine(), "foo" );

    Pipe head = new Pipe( "source" );

    head = new Each( head, new Fields( "line" ), new ExpressionFilter( "line.length() != 0", String.class ) );

    Pipe left = new Each( new Pipe( "left", head ), new Fields( "line" ), new RegexFilter( ".*46.*" ) );
    Pipe right = new Each( new Pipe( "right", head ), new Fields( "line" ), new RegexFilter( ".*192.*" ) );

    Pipe merge = new GroupBy( "merge", Pipe.pipes( left, right ), new Fields( "offset" ) );

    Flow flow = new HadoopFlowConnector().connect( source, sink, merge );

//    flow.writeDOT( "splitmerge.dot" );

    List<FlowStep> steps = flow.getFlowSteps();

    assertEquals( "not equal: steps.size()", 2, steps.size() ); // todo: should be a single job
    }

  @Test
  public void testCoGroupAroundCoGroup() throws Exception
    {
    Tap source10 = new Hfs( new TextLine( new Fields( "num" ) ), "foo" );
    Tap source20 = new Hfs( new TextLine( new Fields( "num" ) ), "bar" );

    Map sources = new HashMap();

    sources.put( "source20", source20 );
    sources.put( "source101", source10 );
    sources.put( "source102", source10 );

    // using null pos so all fields are written
    Tap sink = new Hfs( new TextLine(), "baz", true );

    Pipe pipeNum20 = new Pipe( "source20" );
    Pipe pipeNum101 = new Pipe( "source101" );
    Pipe pipeNum102 = new Pipe( "source102" );

    Pipe splice1 = new CoGroup( pipeNum20, new Fields( "num" ), pipeNum101, new Fields( "num" ), new Fields( "num1", "num2" ) );

    Pipe splice2 = new CoGroup( splice1, new Fields( "num1" ), pipeNum102, new Fields( "num" ), new Fields( "num1", "num2", "num3" ) );

    Flow flow = new HadoopFlowConnector().connect( sources, sink, splice2 );

    assertEquals( "not equal: steps.size()", 2, flow.getFlowSteps().size() );
    }

  @Test
  public void testCoGroupAroundCoGroupOptimized() throws Exception
    {
    Tap source10 = new Hfs( new TextLine( new Fields( "num" ) ), "foo" );
    Tap source20 = new Hfs( new TextLine( new Fields( "num" ) ), "bar" );

    Map sources = new HashMap();

    sources.put( "source20", source20 );
    sources.put( "source101", source10 );
    sources.put( "source102", source10 );

    // using null pos so all fields are written
    Tap sink = new Hfs( new TextLine(), "baz", true );

    Pipe pipeNum20 = new Pipe( "source20" );
    Pipe pipeNum101 = new Pipe( "source101" );
    Pipe pipeNum102 = new Pipe( "source102" );

    Pipe splice1 = new CoGroup( pipeNum20, new Fields( "num" ), pipeNum101, new Fields( "num" ), new Fields( "num1", "num2" ) );

    Pipe splice2 = new CoGroup( splice1, new Fields( "num1" ), pipeNum102, new Fields( "num" ), new Fields( "num1", "num2", "num3" ) );

    Properties properties = new Properties();
    FlowConnectorProps.setIntermediateSchemeClass( properties, TextLine.class );

    HadoopFlowConnector flowConnector = new HadoopFlowConnector( properties );

    Flow flow = flowConnector.connect( sources, sink, splice2 );

    assertEquals( "not equal: steps.size()", 2, flow.getFlowSteps().size() );
    }

  @Test
  public void testCoGroupAroundCoGroupAroundCoGroup() throws Exception
    {
    Tap sourceLower = new Hfs( new TextLine( new Fields( "offset", "line" ) ), "foo" );
    Tap sourceUpper = new Hfs( new TextLine( new Fields( "offset", "line" ) ), "bar" );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper1", sourceUpper );
    sources.put( "upper2", sourceUpper );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    // using null pos so all fields are written
    Tap sink = new Hfs( new TextLine(), "output", true );

    Pipe pipeLower = new Each( "lower", new Fields( "line" ), splitter );
    Pipe pipeUpper1 = new Each( "upper1", new Fields( "line" ), splitter );
    Pipe pipeUpper2 = new Each( "upper2", new Fields( "line" ), splitter );

    Pipe splice1 = new CoGroup( pipeLower, new Fields( "num" ), pipeUpper1, new Fields( "num" ), new Fields( "num1", "char1", "num2", "char2" ) );

    splice1 = new Each( splice1, new Identity() );

    splice1 = new GroupBy( splice1, new Fields( 0 ) );

    Pipe splice2 = new CoGroup( splice1, new Fields( "num1" ), pipeUpper2, new Fields( "num" ), new Fields( "num1", "char1", "num2", "char2", "num3", "char3" ) );

    splice2 = new Each( splice2, new Identity() );

    splice2 = new GroupBy( splice2, new Fields( 0 ) );

    splice2 = new CoGroup( splice2, new Fields( "num1" ), splice1, new Fields( "num1" ), new Fields( "num1", "char1", "num2", "char2", "num3", "char3", "num4", "char4", "num5", "char5" ) );

    Flow flow = null;
    try
      {
      flow = new HadoopFlowConnector().connect( sources, sink, splice2 );
      }
    catch( FlowException exception )
      {
//      exception.writeDOT( "cogroupcogroup.dot" );
      throw exception;
      }

    assertEquals( "not equal: steps.size()", 5, flow.getFlowSteps().size() );
    }

  @Test
  public void testCoGroupWithResultGroupFieldsDefault() throws Exception
    {
    Tap sourceLower = new Hfs( new TextLine( new Fields( "offset", "line" ) ), "foo" );
    Tap sourceUpper = new Hfs( new TextLine( new Fields( "offset", "line" ) ), "bar" );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    // using null pos so all fields are written
    Tap sink = new Hfs( new TextLine(), "/complex/cogroup/", true );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );

    Pipe splice = new CoGroup( pipeLower, new Fields( "num" ), pipeUpper, new Fields( "num" ), new Fields( "num1", "lhs", "num2", "rhs" ) );

    splice = new Every( splice, new First( new Fields( "value" ) ), new Fields( "num1", "value" ) );

    Flow countFlow = new HadoopFlowConnector().connect( sources, sink, splice );
    }

  @Test
  public void testCoGroupWithResultGroupFields() throws Exception
    {
    Tap sourceLower = new Hfs( new TextLine( new Fields( "offset", "line" ) ), "foo" );
    Tap sourceUpper = new Hfs( new TextLine( new Fields( "offset", "line" ) ), "bar" );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    // using null pos so all fields are written
    Tap sink = new Hfs( new TextLine(), "/complex/cogroup/", true );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );

    Pipe splice = new CoGroup( pipeLower, new Fields( "num" ), pipeUpper, new Fields( "num" ), new Fields( "num1", "lhs", "num2", "rhs" ), new Fields( "somenum", "somenum2" ) );

    splice = new Every( splice, new First( new Fields( "value" ) ), new Fields( "somenum", "value" ) );

    Flow countFlow = new HadoopFlowConnector().connect( sources, sink, splice );
    }

  @Test
  public void testDirectCoGroup() throws Exception
    {
    Tap sourceLower = new Hfs( new TextLine( new Fields( "num", "char" ) ), "foo" );
    Tap sourceUpper = new Hfs( new TextLine( new Fields( "num", "char" ) ), "bar" );

    Map sources = new HashMap();

    sources.put( "lower1", sourceLower );
    sources.put( "lower2", sourceLower );
    sources.put( "upper1", sourceUpper );
    sources.put( "upper2", sourceUpper );

    // using null pos so all fields are written
    Tap sink1 = new Hfs( new TextLine(), "output1", true );
    Tap sink2 = new Hfs( new TextLine(), "output2", true );

    Map sinks = new HashMap();

    sinks.put( "output1", sink1 );
    sinks.put( "output2", sink2 );

    Pipe pipeLower1 = new Pipe( "lower1" );
    Pipe pipeLower2 = new Pipe( "lower2" );
    Pipe pipeUpper1 = new Pipe( "upper1" );
    Pipe pipeUpper2 = new Pipe( "upper2" );

    Pipe splice1 = new CoGroup( pipeLower1, new Fields( "num" ), pipeUpper1, new Fields( "num" ), new Fields( "num1", "char1", "num2", "char2" ) );

    Pipe splice2 = new CoGroup( splice1, new Fields( "num1" ), pipeUpper2, new Fields( "num" ), new Fields( "num1", "char1", "num2", "char2", "num3", "char3" ) );

    splice2 = new CoGroup( "output1", splice2, new Fields( "num1" ), splice1, new Fields( "num1" ), new Fields( "num1", "char1", "num2", "char2", "num3", "char3", "num4", "char4", "num5", "char5" ) );

    Pipe splice3 = new CoGroup( "output2", pipeLower2, new Fields( "num" ), splice2, new Fields( "num1" ), new Fields( "num1", "char1", "num2", "char2", "num3", "char3", "num4", "char4", "num5", "char5", "num6", "char6" ) );

    Flow flow = null;
    try
      {
      flow = new HadoopFlowConnector().connect( sources, sinks, splice3 );
      }
    catch( FlowException exception )
      {
//      exception.writeDOT( "directcogroup.dot" );
      throw exception;
      }

    assertEquals( "not equal: steps.size()", 5, flow.getFlowSteps().size() );
    }

  /**
   * verify case where same source is fed to multiple chained cogroups
   *
   * @throws Exception
   */
  @Test
  public void testMultipleCoGroupSimilarSources() throws Exception
    {
    Tap sourceLower = new Hfs( new TextLine( new Fields( "num", "char" ) ), "foo" );
    Tap sourceUpper = new Hfs( new TextLine( new Fields( "num", "char" ) ), "bar" );

    Map sources = new HashMap();

    sources.put( "lower1", sourceLower );
    sources.put( "upper1", sourceUpper );

    // using null pos so all fields are written
    Tap sink1 = new Hfs( new TextLine(), "output1", true );
    Tap sink2 = new Hfs( new TextLine(), "output2", true );

    Map sinks = new HashMap();

    sinks.put( "output1", sink1 );
    sinks.put( "output2", sink2 );

    Pipe pipeLower1 = new Pipe( "lower1" );
    Pipe pipeUpper1 = new Pipe( "upper1" );

    Pipe splice1 = new CoGroup( pipeLower1, new Fields( "num" ), pipeUpper1, new Fields( "num" ), new Fields( "num1", "char1", "num2", "char2" ) );

    Pipe splice2 = new CoGroup( splice1, new Fields( "num1" ), pipeUpper1, new Fields( "num" ), new Fields( "num1", "char1", "num2", "char2", "num3", "char3" ) );

    splice2 = new CoGroup( "output1", splice2, new Fields( "num1" ), splice1, new Fields( "num1" ), new Fields( "num1", "char1", "num2", "char2", "num3", "char3", "num4", "char4", "num5", "char5" ) );

    Pipe splice3 = new CoGroup( "output2", pipeUpper1, new Fields( "num" ), splice2, new Fields( "num1" ), new Fields( "num1", "char1", "num2", "char2", "num3", "char3", "num4", "char4", "num5", "char5", "num6", "char6" ) );

    Flow flow = null;
    try
      {
      flow = new HadoopFlowConnector().connect( sources, sinks, splice3 );
      }
    catch( FlowException exception )
      {
//      exception.writeDOT( "chainedcogroup.dot" );
      throw exception;
      }

    assertEquals( "not equal: steps.size()", 5, flow.getFlowSteps().size() );
    }

  /**
   * tests to make sure splits on a pipe before a cogroup and after result in proper normalization
   *
   * @throws Exception
   */
  @Test
  public void testMultipleCoGroupSplitSources() throws Exception
    {
    Tap sourceLower = new Hfs( new TextLine( new Fields( "num", "char" ) ), "foo" );
    Tap sourceUpper = new Hfs( new TextLine( new Fields( "num", "char" ) ), "bar" );

    Map sources = new HashMap();

    sources.put( "lower1", sourceLower );
    sources.put( "upper1", sourceUpper );

    // using null pos so all fields are written
    Tap sink1 = new Hfs( new TextLine(), "output1", true );
    Tap sink2 = new Hfs( new TextLine(), "output2", true );

    Map sinks = new HashMap();

    sinks.put( "output1", sink1 );
    sinks.put( "output2", sink2 );

    Pipe pipeLower1 = new Pipe( "lower1" );
    Pipe pipeUpper1 = new Pipe( "upper1" );

    Pipe pipeLower2 = new Each( pipeLower1, new Identity() );
    pipeLower2 = new GroupBy( pipeLower2, new Fields( "num", "char" ) );
    pipeLower2 = new Every( pipeLower2, new Fields( "num", "char" ), new Count(), new Fields( "num", "char" ) );

    pipeLower1 = new Each( pipeLower1, new Identity() );
    pipeLower1 = new Each( pipeLower1, new Identity() );
    pipeLower1 = new Each( pipeLower1, new Identity() );
    pipeLower1 = new Pipe( "lower2", pipeLower1 );

    pipeUpper1 = new Each( pipeUpper1, new Identity() );
    pipeUpper1 = new Each( pipeUpper1, new Identity() );
    pipeUpper1 = new Each( pipeUpper1, new Identity() );

    Pipe splice1 = new CoGroup( "group", Pipe.pipes( pipeLower1, pipeLower2, pipeUpper1 ), Fields.fields( new Fields( "num" ), new Fields( "num" ), new Fields( "num" ) ), new Fields( "num1", "char1", "num2", "char2", "num3", "char3" ), new InnerJoin() );

    Pipe output1 = new Each( splice1, AssertionLevel.VALID, new AssertNotNull() );
    output1 = new Each( output1, new Identity() );
    output1 = new Pipe( "output1", output1 );

    Pipe output2 = new Each( splice1, AssertionLevel.VALID, new AssertNull() );
    output2 = new Each( output2, new Identity() );
    output2 = new Pipe( "output2", output2 );

    Flow flow = null;
    try
      {
      flow = new HadoopFlowConnector().connect( sources, sinks, output1, output2 );
      }
    catch( FlowException exception )
      {
//      exception.writeDOT( "chainedcogroup.dot" );
      throw exception;
      }

    assertEquals( "not equal: steps.size()", 4, flow.getFlowSteps().size() );
    }

  /**
   * verify split is homogeneous
   *
   * @throws Exception
   */
  @Test
  public void testSplitEachOnGroup() throws Exception
    {
    Tap sourceLower = new Hfs( new TextLine( new Fields( "num", "char" ) ), "foo" );

    Map sources = new HashMap();

    sources.put( "lower1", sourceLower );

    // using null pos so all fields are written
    Tap sink1 = new Hfs( new TextLine(), "output1", true );
    Tap sink2 = new Hfs( new TextLine(), "output2", true );

    Map sinks = new HashMap();

    sinks.put( "output1", sink1 );
    sinks.put( "output2", sink2 );

    Pipe pipeLower1 = new Pipe( "lower1" );

    Pipe pipe = new GroupBy( pipeLower1, new Fields( 0 ) );
    Pipe left = new Each( new Pipe( "output1", pipe ), new Identity() );
    Pipe right = new Each( new Pipe( "output2", pipe ), new Identity() );

    Flow flow = null;
    try
      {
      flow = new HadoopFlowConnector().connect( sources, sinks, Pipe.pipes( left, right ) );
      }
    catch( PlannerException exception )
      {
//      exception.writeDOT( "splitout.dot" );
      throw exception;
      }

    List<FlowStep> steps = flow.getFlowSteps();

    assertEquals( "not equal: steps.size()", 3, steps.size() );
    }

  @Test
  public void testSplitEveryOnGroup() throws Exception
    {
    Tap sourceLower = new Hfs( new TextLine( new Fields( "num", "char" ) ), "foo" );

    Map sources = new HashMap();

    sources.put( "lower1", sourceLower );

    // using null pos so all fields are written
    Tap sink1 = new Hfs( new TextLine(), "output1", true );
    Tap sink2 = new Hfs( new TextLine(), "output2", true );

    Map sinks = new HashMap();

    sinks.put( "output1", sink1 );
    sinks.put( "output2", sink2 );

    Pipe pipeLower1 = new Pipe( "lower1" );

    Pipe pipe = new GroupBy( pipeLower1, new Fields( 0 ) );
    Pipe left = new Every( new Pipe( "output1", pipe ), new TestBuffer( new Fields( "left" ), true ) );
    Pipe right = new Every( new Pipe( "output2", pipe ), new TestBuffer( new Fields( "right" ), true ) );

    Flow flow = null;
    try
      {
      flow = new HadoopFlowConnector().connect( sources, sinks, Pipe.pipes( left, right ) );
      fail( "did not throw planner exception" );
      }
    catch( PlannerException exception )
      {
//      exception.printStackTrace(  );
      }
    }

  @Test
  public void testSplitOutput() throws Exception
    {
    Tap sourceLower = new Hfs( new TextLine( new Fields( "num", "char" ) ), "foo" );

    Map sources = new HashMap();

    sources.put( "lower1", sourceLower );

    // using null pos so all fields are written
    Tap sink1 = new Hfs( new TextLine(), "output1", true );
    Tap sink2 = new Hfs( new TextLine(), "output2", true );

    Map sinks = new HashMap();

    sinks.put( "output1", sink1 );
    sinks.put( "output2", sink2 );

    Pipe pipeLower1 = new Pipe( "lower1" );

    Pipe left = new GroupBy( "output1", pipeLower1, new Fields( 0 ) );
    Pipe right = new GroupBy( "output2", left, new Fields( 0 ) );

    Flow flow = null;
    try
      {
      flow = new HadoopFlowConnector().connect( sources, sinks, Pipe.pipes( left, right ) );
      }
    catch( FlowException exception )
      {
//      exception.writeDOT( "splitout.dot" );
      throw exception;
      }

    List<FlowStep> steps = flow.getFlowSteps();

    assertEquals( "not equal: steps.size()", 3, steps.size() );

//    for( FlowStep step : steps )
//      {
//      if( step.group != null )
//        continue;
//
//      Scope nextScope = step.getNextScope( step.sources.keySet().iterator().next() );
//      FlowElement operator = step.getNextFlowElement( nextScope );
//
//      assertTrue( "should be Pipe", operator instanceof Pipe );
//      }
    }

  /**
   * DISABLED
   * found having pipes with same names was too error prone. the workaround is to bind the tap to both names.
   * if the process logically must use the same tap for each branch, then the branch should be split
   *
   * This tests if two pipes can have the same name, and thus logically the same input source.
   * <p/>
   * Further, a GroupBy with two inputs would fail if the source was directly associated. but there is a Group
   * function between the source and the merge, so it passes.
   *
   *
   * @throws java.io.IOException
   */
//  public void testSameHeadName() throws IOException
//    {
//    Map sources = new HashMap();
//    Map sinks = new HashMap();
//
//    sources.put( "a", new Hfs( new Fields( "first", "second" ), "input/path/a" ) );
//
//    Pipe pipeA = new Pipe( "a" );
//    Pipe pipeB = new Pipe( "a" );
//
//    Pipe group1 = new GroupBy( "a1", pipeA, Fields.FIRST );
//    Pipe group2 = new GroupBy( "a2", pipeB, Fields.FIRST );
//
//    Pipe merge = new GroupBy( "tail", Pipe.pipes( group1, group2 ), new Fields( "first", "second" ) );
//
//    sinks.put( merge.getName(), new Hfs( new TextLine(), "output/path" ) );
//
//    Flow flow = new FlowConnector().connect( sources, sinks, merge );
//
//    assertEquals( "not equal: steps.size()", 3, flow.getSteps().size() );
//    }

  /**
   * This is an alternative to having two pipes with the same name, but uses one pipe that is split
   * across two branches.
   *
   * @throws IOException
   */
  @Test
  public void testSameSourceForBranch() throws IOException
    {
    Map sources = new HashMap();
    Map sinks = new HashMap();

    sources.put( "a", new Hfs( new Fields( "first", "second" ), "input/path/a" ) );

    Pipe pipeA = new Pipe( "a" );

    Pipe group1 = new GroupBy( "a1", pipeA, Fields.FIRST );
    Pipe group2 = new GroupBy( "a2", pipeA, Fields.FIRST );

    Pipe merge = new GroupBy( "tail", Pipe.pipes( group1, group2 ), new Fields( "first", "second" ) );

    sinks.put( merge.getName(), new Hfs( new TextLine(), "output/path" ) );

    Flow flow = new HadoopFlowConnector().connect( sources, sinks, merge );

    assertEquals( "not equal: steps.size()", 3, flow.getFlowSteps().size() );
    }

  /**
   * Verifies the same tap instance can be shared between two logically different pipes.
   *
   * @throws IOException
   */
  @Test
  public void testSameTaps() throws IOException
    {
    Map sources = new HashMap();
    Map sinks = new HashMap();

    Hfs tap = new Hfs( new Fields( "first", "second" ), "input/path/a" );
    sources.put( "a", tap );
    sources.put( "b", tap );

    Pipe pipeA = new Pipe( "a" );
    Pipe pipeB = new Pipe( "b" );

    Pipe group1 = new GroupBy( pipeA );
    Pipe group2 = new GroupBy( pipeB );

    Pipe merge = new GroupBy( "tail", Pipe.pipes( group1, group2 ), new Fields( "first", "second" ) );

    sinks.put( merge.getName(), new Hfs( new TextLine(), "output/path" ) );

    Flow flow = new HadoopFlowConnector().connect( sources, sinks, merge );

    assertEquals( "not equal: steps.size()", 3, flow.getFlowSteps().size() );
    }

  @Test
  public void testDanglingHead() throws IOException
    {
    Map sources = new HashMap();
    Map sinks = new HashMap();

    Hfs source = new Hfs( new Fields( "first", "second" ), "input/path/a" );
    sources.put( "a", source );

    Pipe pipeA = new Pipe( "a" );
    Pipe pipeB = new Pipe( "b" );

    Pipe group1 = new GroupBy( pipeA );
    Pipe group2 = new GroupBy( pipeB );

    Pipe merge = new GroupBy( "tail", Pipe.pipes( group1, group2 ), new Fields( "first", "second" ) );

    sinks.put( merge.getName(), new Hfs( new TextLine(), "output/path" ) );

    try
      {
      Flow flow = new HadoopFlowConnector().connect( sources, sinks, merge );
      fail( "did not catch missing source tap" );
      }
    catch( PlannerException exception )
      {
      // do nothing
      }
    catch( Exception exception )
      {
      fail( "threw wrong exception" );
      }
    }

  @Test
  public void testDanglingTail() throws IOException
    {
    Map sources = new HashMap();
    Map sinks = new HashMap();

    Hfs tap = new Hfs( new Fields( "first", "second" ), "input/path/a" );
    sources.put( "a", tap );
    sources.put( "b", tap );

    Pipe pipeA = new Pipe( "a" );
    Pipe pipeB = new Pipe( "b" );

    Pipe group1 = new GroupBy( pipeA );
    Pipe group2 = new GroupBy( pipeB );

    Pipe merge = new GroupBy( "tail", Pipe.pipes( group1, group2 ), new Fields( "first", "second" ) );

//    sinks.put( merge.getName(), new Hfs( new TextLine(), "output/path" ) );

    try
      {
      Flow flow = new HadoopFlowConnector().connect( sources, sinks, merge );
      fail( "did not catch missing sink tap" );
      }
    catch( PlannerException exception )
      {
      // do nothing
      }
    catch( Exception exception )
      {
      fail( "threw wrong exception" );
      }
    }

  @Test
  public void testExtraSource() throws IOException
    {
    Map sources = new HashMap();
    Map sinks = new HashMap();

    Hfs tap = new Hfs( new Fields( "first", "second" ), "input/path/a" );
    sources.put( "a", tap );
    sources.put( "b", tap );
    sources.put( "c", tap );

    Pipe pipeA = new Pipe( "a" );
    Pipe pipeB = new Pipe( "b" );

    Pipe group1 = new GroupBy( pipeA );
    Pipe group2 = new GroupBy( pipeB );

    Pipe merge = new GroupBy( "tail", Pipe.pipes( group1, group2 ), new Fields( "first", "second" ) );

    sinks.put( merge.getName(), new Hfs( new TextLine(), "output/path" ) );

    try
      {
      Flow flow = new HadoopFlowConnector().connect( sources, sinks, merge );
      fail( "did not catch extra source tap" );
      }
    catch( PlannerException exception )
      {
//      exception.printStackTrace();
      assertTrue( exception.getMessage().contains( "['c']" ) );
      }
    catch( Exception exception )
      {
      fail( "threw wrong exception" );
      }
    }

  @Test
  public void testExtraSink() throws IOException
    {
    Map sources = new HashMap();
    Map sinks = new HashMap();

    Hfs tap = new Hfs( new Fields( "first", "second" ), "input/path/a" );
    sources.put( "a", tap );
    sources.put( "b", tap );

    Pipe pipeA = new Pipe( "a" );
    Pipe pipeB = new Pipe( "b" );

    Pipe group1 = new GroupBy( pipeA );
    Pipe group2 = new GroupBy( pipeB );

    Pipe merge = new GroupBy( "tail", Pipe.pipes( group1, group2 ), new Fields( "first", "second" ) );

    sinks.put( merge.getName(), new Hfs( new TextLine(), "output/path" ) );
    sinks.put( "c", new Hfs( new TextLine(), "output/path" ) );

    try
      {
      Flow flow = new HadoopFlowConnector().connect( sources, sinks, merge );
      fail( "did not catch extra sink tap" );
      }
    catch( PlannerException exception )
      {
//      exception.printStackTrace();
      assertTrue( exception.getMessage().contains( "['c']" ) );
      }
    catch( Exception exception )
      {
      fail( "threw wrong exception" );
      }
    }

  @Test
  public void testBuffer() throws IOException
    {
    Map sources = new HashMap();
    Map sinks = new HashMap();

    sources.put( "count", new Hfs( new Fields( "first", "second" ), "input/path" ) );
    sinks.put( "count", new Hfs( new Fields( 0, 1 ), "output/path" ) );

    Pipe pipe = new Pipe( "count" );
    pipe = new GroupBy( pipe, new Fields( 1 ) );
    pipe = new Every( pipe, new Fields( 1 ), new TestBuffer( new Fields( "fourth" ), "value" ), new Fields( 0, 1 ) );

    List steps = new HadoopFlowConnector().connect( sources, sinks, pipe ).getFlowSteps();

    assertEquals( "wrong size", 1, steps.size() );

    BaseFlowStep step = (BaseFlowStep) steps.get( 0 );

    assertEquals( "not equal: step.sources.size()", 1, step.getSources().size() );
    assertNotNull( "null: step.groupBy", step.getGroup() );
    assertNotNull( "null: step.sink", step.getSink() );

    int mapDist = countDistance( step.getGraph(), (FlowElement) step.getSources().iterator().next(), step.getGroup() );
    assertEquals( "not equal: mapDist", 0, mapDist );

    int reduceDist = countDistance( step.getGraph(), step.getGroup(), step.getSink() );
    assertEquals( "not equal: reduceDist", 1, reduceDist );
    }

  @Test
  public void testBufferFail() throws IOException
    {
    Map sources = new HashMap();
    Map sinks = new HashMap();

    sources.put( "count", new Hfs( new Fields( "first", "second" ), "input/path" ) );
    sinks.put( "count", new Hfs( new Fields( 0, 1 ), "output/path" ) );

    Pipe pipe = new Pipe( "count" );
    pipe = new GroupBy( pipe, new Fields( 1 ) );
    pipe = new Every( pipe, new Fields( 1 ), new TestBuffer( new Fields( "fourth" ), "value" ), new Fields( 0, 1 ) );
    pipe = new Every( pipe, new Fields( 1 ), new Count(), new Fields( 0, 1 ) );

    try
      {
      new HadoopFlowConnector().connect( sources, sinks, pipe );
      fail( "did not throw planner exception" );
      }
    catch( Exception exception )
      {
      // ignore
//      exception.printStackTrace();
      }
    }

  @Test
  public void testBufferFail2() throws IOException
    {
    Map sources = new HashMap();
    Map sinks = new HashMap();

    sources.put( "count", new Hfs( new Fields( "first", "second" ), "input/path" ) );
    sinks.put( "count", new Hfs( new Fields( 0, 1 ), "output/path" ) );

    Pipe pipe = new Pipe( "count" );
    pipe = new GroupBy( pipe, new Fields( 1 ) );
    pipe = new Every( pipe, new Fields( 1 ), new Count(), new Fields( 0, 1 ) );
    pipe = new Every( pipe, new Fields( 1 ), new TestBuffer( new Fields( "fourth" ), "value" ), new Fields( 0, 1 ) );

    try
      {
      new HadoopFlowConnector().connect( sources, sinks, pipe );
      fail( "did not throw planner exception" );
      }
    catch( Exception exception )
      {
      // ignore
//      exception.printStackTrace();
      }
    }

  @Test
  public void testErrorMessages() throws Exception
    {
    Tap source10 = new Hfs( new TextLine( new Fields( "num" ) ), "foo" );
    Tap source20 = new Hfs( new TextLine( new Fields( "num" ) ), "bar" );

    Map sources = new HashMap();

    sources.put( "source20", source20 );
    sources.put( "source101", source10 );
    sources.put( "source102", source10 );

    // using null pos so all fields are written
    Tap sink = new Hfs( new TextLine(), "baz", true );

    Pipe pipeNum20 = new Pipe( "source20" );
    Pipe pipeNum101 = new Pipe( "source101" );
    Pipe pipeNum102 = new Pipe( "source102" );

    Pipe splice1 = new CoGroup( pipeNum20, new Fields( "num" ), pipeNum101, new Fields( "num" ), new Fields( "num1", "num2" ) );

    Pipe splice2 = new CoGroup( splice1, new Fields( "num9" ), pipeNum102, new Fields( "num" ), new Fields( "num1", "num2", "num3" ) );

    HadoopFlowConnector flowConnector = new HadoopFlowConnector();

    try
      {
      Flow flow = flowConnector.connect( sources, sink, splice2 );
      fail( "did not fail on bad field" );
      }
    catch( Exception exception )
      {
      // ignore
      assertTrue( "missing message", exception.getMessage().contains( "BuildJobsHadoopTest.testErrorMessages" ) );
      }
    }

  /**
   * This test verifies splits on Pipe instances are recognized
   * <p/>
   * This flow intentionally splits to a Each and a Tap from a Each
   * <pre>
   *
   *  .... E1 - T1 - E2 - T2
   *
   * </pre>
   * <p/>
   * this test also verifed T1 feeds E2, instead of a new copy job being created
   *
   * @throws Exception
   */
  @Test
  public void testSplitInMiddleBeforePipeOptimized() throws Exception
    {
    splitMiddle( true, true );
    }

  @Test
  public void testSplitInMiddleBeforePipe() throws Exception
    {
    splitMiddle( true, false );
    }

  @Test
  public void testSplitInMiddleAfterPipe() throws Exception
    {
    splitMiddle( false, false );
    }

  @Test
  private void splitMiddle( boolean before, boolean testTempReplaced )
    {
    Tap sourceLower = new Hfs( new TextLine( new Fields( "offset", "line" ) ), "lower" );
    Tap sourceUpper = new Hfs( new TextLine( new Fields( "offset", "line" ) ), "upper" );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Scheme leftScheme = testTempReplaced ? new SequenceFile( new Fields( "num", "lower", "num2", "upper" ) ) : new TextLine( new Fields( "offset", "line" ), new Fields( "lower" ) );
    Tap sinkLeft = new Hfs( leftScheme, "/splitmiddle/left", SinkMode.REPLACE );

    Scheme rightScheme = testTempReplaced ? new SequenceFile( new Fields( "lower" ) ) : new TextLine( new Fields( "offset", "line" ), new Fields( "lower" ) );
    Tap sinkRight = new Hfs( rightScheme, "/splitmiddle/right", SinkMode.REPLACE );

    Map sinks = new HashMap();

    sinks.put( "left", sinkLeft );
    sinks.put( "right", sinkRight );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );

    Pipe splice = new CoGroup( "both", pipeLower, new Fields( "num" ), pipeUpper, new Fields( "num" ), new Fields( "num", "lower", "num2", "upper" ) );

    splice = new Each( splice, new Fields( "num" ), new RegexFilter( ".*" ) );

    Pipe left = splice;

    if( before )
      left = new Pipe( "left", left );

    left = new Each( left, new Fields( "num" ), new RegexFilter( ".*" ) );

    if( !before )
      left = new Pipe( "left", left );

    Pipe right = left;

    if( before )
      right = new Pipe( "right", right );

    right = new Each( right, new Fields( "num" ), new RegexFilter( ".*" ) );

    if( !before )
      right = new Pipe( "right", right );

    Flow flow = new HadoopFlowConnector().connect( "splitmiddle", sources, sinks, left, right );

    List<FlowStep> steps = flow.getFlowSteps();

    assertEquals( "not equal: steps.size()", testTempReplaced ? 2 : 3, steps.size() );

    BaseFlowStep step = (BaseFlowStep) steps.get( 0 );

    Scope nextScope = step.getNextScope( step.getGroup() );
    FlowElement operator = step.getNextFlowElement( nextScope );

    assertTrue( "not an Each", operator instanceof Each );

    nextScope = step.getNextScope( operator );
    operator = step.getNextFlowElement( nextScope );

    assertTrue( "not a Each", operator instanceof Each );

    nextScope = step.getNextScope( operator );
    operator = step.getNextFlowElement( nextScope );

    if( testTempReplaced )
      {
      assertEquals( "not proper sink", sinkLeft, operator );
      }
    else
      {
      assertTrue( "not a TempHfs", operator instanceof TempHfs );
      }
    }

  @Test
  public void testSourceIsSink()
    {
    Tap tap = new Hfs( new TextLine( new Fields( "offset", "line" ) ), "foo/merge" );

    Pipe pipe = new Pipe( "left" );

    try
      {
      Flow flow = new HadoopFlowConnector().connect( tap, tap, pipe );
      fail( "did not throw planner exception" );
      }
    catch( Exception exception )
      {
//      exception.printStackTrace();
      }
    }

  @Test
  public void testReplaceFail() throws Exception
    {
    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), "foo" );
    Tap sink = new Hfs( new TextLine( new Fields( "offset", "line" ), new Fields( "offset", "line2" ) ), "bar", true );

    Pipe pipe = new Pipe( "test" );

    Function parser = new RegexParser( new Fields( 0 ), "^[^ ]*" );
    pipe = new Each( pipe, new Fields( "line" ), parser, Fields.REPLACE );
    pipe = new Each( pipe, new Fields( "line" ), new Identity( Fields.ARGS ), Fields.REPLACE );
    pipe = new Each( pipe, new Fields( "line" ), new Identity( new Fields( "line2" ) ), Fields.REPLACE );

    try
      {
      Flow flow = new HadoopFlowConnector().connect( source, sink, pipe );
      fail( "did not fail" );
      }
    catch( Exception exception )
      {
      }
    }

  private int countDistance( SimpleDirectedGraph<FlowElement, Scope> graph, FlowElement lhs, FlowElement rhs )
    {
    return DijkstraShortestPath.findPathBetween( graph, lhs, rhs ).size() - 1;
    }

  @Test
  public void testNestedProperties() throws IOException
    {
    Tap source = new Hfs( new TextLine( new Fields( "line" ) ), "/input" );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new RegexSplitter( new Fields( "first", "second", "third" ), "\\s" ), Fields.ALL );

    Tap sink = new Hfs( new TextLine(), "output", true );

    Properties defaultProperties = new Properties();

    defaultProperties.setProperty( "test.key", "test.value" );

    HadoopFlow flow = (HadoopFlow) new HadoopFlowConnector( new Properties( defaultProperties ) ).connect( source, sink, pipe );

    assertEquals( "test flow", "test.value", flow.getProperty( "test.key" ) );
    assertEquals( "test step", "test.value", ( (HadoopFlowStep) flow.getFlowSteps().get( 0 ) ).getInitializedConfig( flow.getFlowProcess(), flow.getConfig() ).get( "test.key" ) );
    }

  @Test
  public void testEveryAfterJoin()
    {
    Tap source1 = new Hfs( new TextLine( new Fields( "offset", "line" ) ), "foo/merge1" );
    Tap source2 = new Hfs( new TextLine( new Fields( "offset", "line" ) ), "foo/merge2" );

    Tap sink = new Hfs( new TextLine(), "foo" );

    Pipe left = new Each( new Pipe( "left" ), new Fields( "line" ), new RegexFilter( ".*46.*" ) );
    Pipe right = new Each( new Pipe( "right" ), new Fields( "line" ), new RegexFilter( ".*192.*" ) );

    Fields[] fields = Fields.fields( new Fields( "offset" ), new Fields( "offset" ) );
    Pipe merge = new HashJoin( "join", Pipe.pipes( left, right ), fields, Fields.size( 4 ), new InnerJoin() );

    merge = new Every( merge, new MaxValue() );

    Map sources = new HashMap();
    sources.put( "left", source1 );
    sources.put( "right", source2 );

    Map sinks = new HashMap();
    sinks.put( "join", sink );

    try
      {
      Flow flow = new HadoopFlowConnector().connect( sources, sinks, merge );
      fail();
      }
    catch( Exception exception )
      {
      // do nothing
//      exception.printStackTrace();
      }
    }
  }
