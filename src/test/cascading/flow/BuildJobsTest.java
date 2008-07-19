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

package cascading.flow;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import cascading.CascadingTestCase;
import cascading.operation.Function;
import cascading.operation.Identity;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexFilter;
import cascading.operation.regex.RegexParser;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.Group;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.PipeAssembly;
import cascading.scheme.SequenceFile;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tap.TempHfs;
import cascading.tuple.Fields;
import org.jgrapht.alg.DijkstraShortestPath;
import org.jgrapht.graph.SimpleDirectedGraph;

/** @version $Id: //depot/calku/cascading/src/test/cascading/flow/BuildJobsTest.java#2 $ */
public class BuildJobsTest extends CascadingTestCase
  {
  public BuildJobsTest()
    {
    super( "build jobs" );
    }

  /**
   * Test a single piece Pipe, should not fail, inserts Identity pipe
   *
   * @throws IOException
   */
  public void testIdentity() throws Exception
    {
    Tap source = new Hfs( new TextLine(), "input/path" );
    Tap sink = new Hfs( new TextLine(), "output/path", true );

    Pipe pipe = new Pipe( "test" );

    Flow flow = new FlowConnector().connect( source, sink, pipe );

    List<FlowStep> steps = flow.getSteps();

    assertEquals( "wrong size", 1, steps.size() );

    FlowStep step = (FlowStep) steps.get( 0 );

    step.getJobConf(); // called init the step

    assertEquals( "not equal: step.sources.size()", 1, step.sources.size() );
    assertNull( "not null: step.groupBy", step.group );
    assertNotNull( "null: step.sink", step.sink );

    }

  public void testName()
    {
    Pipe count = new Pipe( "count" );
    Pipe pipe = new Group( count, new Fields( 1 ) );
    pipe = new Every( pipe, new Fields( 1 ), new Count(), new Fields( 0, 1 ) );

    assertEquals( "not equal: count.getName()", "count", count.getName() );
    assertEquals( "not equal: pipe.getName()", "count", pipe.getName() );

    pipe = new Each( count, new Fields( 1 ), new RegexSplitter( Fields.size( 2 ) ) );
    assertEquals( "not equal: pipe.getName()", "count", pipe.getName() );
    }

  public void testOneJob() throws IOException
    {
    Map sources = new HashMap();
    Map sinks = new HashMap();

    sources.put( "count", new Hfs( new Fields( "first", "second" ), "input/path" ) );
    sinks.put( "count", new Hfs( new Fields( 0, 1 ), "output/path" ) );

    Pipe pipe = new Pipe( "count" );
    pipe = new Group( pipe, new Fields( 1 ) );
    pipe = new Every( pipe, new Fields( 1 ), new Count(), new Fields( 0, 1 ) );

    List steps = new FlowConnector().connect( sources, sinks, pipe ).getSteps();

    assertEquals( "wrong size", 1, steps.size() );

    FlowStep step = (FlowStep) steps.get( 0 );

    step.getJobConf(); // called init the step

    assertEquals( "not equal: step.sources.size()", 1, step.sources.size() );
    assertNotNull( "null: step.groupBy", step.group );
    assertNotNull( "null: step.sink", step.sink );

    int mapDist = countDistance( step.graph, step.sources.keySet().iterator().next(), step.group );
    assertEquals( "not equal: mapDist", 0, mapDist );

    int reduceDist = countDistance( step.graph, step.group, step.sink );
    assertEquals( "not equal: reduceDist", 1, reduceDist );
    }

  public void testOneJob2() throws IOException
    {
    Map sources = new HashMap();
    Map sinks = new HashMap();

    sources.put( "count", new Hfs( new Fields( "first", "second" ), "input/path" ) );
    sinks.put( "count", new Hfs( new Fields( 0, 1 ), "output/path" ) );

    Pipe pipe = new Pipe( "count" );
    pipe = new Each( pipe, new Fields( 1 ), new Identity(), new Fields( 2 ) ); // in:second out:all
    pipe = new Each( pipe, new Fields( 0 ), new Identity( new Fields( "_all" ) ), new Fields( 1 ) ); // in:all out:_all
    pipe = new Group( pipe, new Fields( 0 ) ); // in:_all out:_all
    pipe = new Every( pipe, new Fields( 0 ), new Count(), new Fields( 0, 1 ) ); // in:_all out:_all,count

    List steps = new FlowConnector().connect( sources, sinks, pipe ).getSteps();

    assertEquals( "wrong size", 1, steps.size() );

    FlowStep step = (FlowStep) steps.get( 0 );

    step.getJobConf(); // called init the step

    assertEquals( "not equal: step.sources.size()", 1, step.sources.size() );
    assertNotNull( "null: step.groupBy", step.group );
    assertNotNull( "null: step.sink", step.sink );

    int mapDist = countDistance( step.graph, step.sources.keySet().iterator().next(), step.group );
    assertEquals( "not equal: mapDist", 2, mapDist );

    int reduceDist = countDistance( step.graph, step.group, step.sink );
    assertEquals( "not equal: reduceDist", 1, reduceDist );
    }

  public void testOneJob3() throws IOException
    {
    Map sources = new HashMap();
    Map sinks = new HashMap();

    sources.put( "a", new Hfs( new Fields( "first", "second" ), "input/path/a" ) );
    sources.put( "b", new Hfs( new Fields( "third", "fourth" ), "input/path/b" ) );

    Pipe pipeA = new Pipe( "a" );
    Pipe pipeB = new Pipe( "b" );

    Pipe splice = new Group( pipeA, new Fields( 1 ), pipeB, new Fields( 1 ) );

    sinks.put( splice.getName(), new Hfs( new Fields( 0, 1 ), "output/path" ) );

    List steps = new FlowConnector().connect( sources, sinks, splice ).getSteps();

    assertEquals( "wrong size", 1, steps.size() );

    FlowStep step = (FlowStep) steps.get( 0 );

    step.getJobConf(); // called init the step

    assertEquals( "not equal: step.sources.size()", 2, step.sources.size() );
    assertNotNull( "null: step.groupBy", step.group );
    assertNotNull( "null: step.sink", step.sink );

    Iterator<Tap> iterator = step.sources.keySet().iterator();
    int mapDist = countDistance( step.graph, iterator.next(), step.group );
    assertEquals( "not equal: mapDist", 0, mapDist );
    mapDist = countDistance( step.graph, iterator.next(), step.group );
    assertEquals( "not equal: mapDist", 0, mapDist );

    int reduceDist = countDistance( step.graph, step.group, step.sink );
    assertEquals( "not equal: reduceDist", 0, reduceDist );
    }

  public void testOneJob4() throws IOException
    {
    Map sources = new HashMap();
    Map sinks = new HashMap();

    sources.put( "a", new Hfs( new Fields( "first", "second" ), "input/path/a" ) );
    sources.put( "b", new Hfs( new Fields( "third", "fourth" ), "input/path/b" ) );

    Pipe pipeA = new Pipe( "a" );
    Pipe pipeB = new Pipe( "b" );

    Pipe cogroup = new Group( pipeA, new Fields( 1 ), pipeB, new Fields( 1 ) );

    cogroup = new Each( cogroup, new Identity() );

    sinks.put( cogroup.getName(), new Hfs( new Fields( 0, 1 ), "output/path" ) );

    List steps = new FlowConnector().connect( sources, sinks, cogroup ).getSteps();

    assertEquals( "wrong size", 1, steps.size() );

    FlowStep step = (FlowStep) steps.get( 0 );

    step.getJobConf(); // called init the step

    assertEquals( "not equal: step.sources.size()", 2, step.sources.size() );
    assertNotNull( "null: step.groupBy", step.group );
    assertNotNull( "null: step.sink", step.sink );

    int mapDist = countDistance( step.graph, step.sources.keySet().iterator().next(), step.group );
    assertEquals( "not equal: mapDist", 0, mapDist );

    int reduceDist = countDistance( step.graph, step.group, step.sink );
    assertEquals( "not equal: reduceDist", 1, reduceDist );
    }

  public void testOneJob5() throws IOException
    {
    Map sources = new HashMap();
    Map sinks = new HashMap();

    sources.put( "a", new Hfs( new Fields( "first", "second" ), "input/path/a" ) );
    sources.put( "b", new Hfs( new Fields( "third", "fourth" ), "input/path/b" ) );

    Pipe pipeA = new Pipe( "a" );
    Pipe pipeB = new Pipe( "b" );

    Pipe splice = new Group( pipeA, pipeB );

    splice = new Each( splice, new Identity() );

    sinks.put( splice.getName(), new Hfs( new TextLine(), "output/path" ) );

    List steps = new FlowConnector().connect( sources, sinks, splice ).getSteps();

    assertEquals( "wrong size", 1, steps.size() );

    FlowStep step = (FlowStep) steps.get( 0 );

    step.getJobConf(); // called init the step

    assertEquals( "not equal: step.sources.size()", 2, step.sources.size() );
    assertNotNull( "null: step.groupBy", step.group );
    assertNotNull( "null: step.sink", step.sink );

    int mapDist = countDistance( step.graph, step.sources.keySet().iterator().next(), step.group );
    assertEquals( "not equal: mapDist", 0, mapDist );

    int reduceDist = countDistance( step.graph, step.group, step.sink );
    assertEquals( "not equal: reduceDist", 1, reduceDist );
    }

  /** This should result in only two steps, one for each side */
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

    List<FlowStep> steps = new FlowConnector().connect( sources, sinks, left, right ).getSteps();

    assertEquals( "not equal: steps.size()", 2, steps.size() );
    }

  /**
   * This should result in a Temp Tap after the Each split.
   * <p/>
   * We previously would push the each to the next step, but if there is already data being written, save the cpu.
   */
  public void testSplitComplex()
    {
    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), "foo" );
    Tap sink1 = new Hfs( new TextLine(), "foo/split1", true );
    Tap sink2 = new Hfs( new TextLine(), "foo/split2", true );

    Pipe pipe = new Pipe( "split" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    pipe = new Group( pipe, new Fields( "ip" ) );

    pipe = new Every( pipe, new Fields( "ip" ), new Count(), new Fields( "ip", "count" ) );

    pipe = new Each( pipe, new Fields( "ip" ), new RegexFilter( "^68.*" ) );

    Pipe left = new Each( new Pipe( "left", pipe ), new Fields( "ip" ), new RegexFilter( ".*46.*" ) );

    Pipe right = new Each( new Pipe( "right", pipe ), new Fields( "ip" ), new RegexFilter( ".*192.*" ) );

    Map sources = new HashMap();
    sources.put( "split", source );

    Map sinks = new HashMap();
    sinks.put( "left", sink1 );
    sinks.put( "right", sink2 );

    Flow flow = new FlowConnector().connect( sources, sinks, left, right );

//    flow.writeDOT( "splitcomplex.dot" );

    List<FlowStep> steps = flow.getSteps();

    assertEquals( "not equal: steps.size()", 3, steps.size() );

    FlowStep step = steps.get( 0 );

    Scope nextScope = step.getNextScope( step.group );
    FlowElement operator = step.getNextFlowElement( nextScope );

    assertTrue( "not an Every", operator instanceof Every );

    nextScope = step.getNextScope( operator );
    operator = step.getNextFlowElement( nextScope );

    assertTrue( "not a Each", operator instanceof Each );

    nextScope = step.getNextScope( operator );
    operator = step.getNextFlowElement( nextScope );

    assertTrue( "not a TempHfs", operator instanceof TempHfs );
    }

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

    Flow flow = new FlowConnector().connect( sources, sinks, merge );

//    flow.writeDOT( "merged.dot" );

    List<FlowStep> steps = flow.getSteps();

    assertEquals( "not equal: steps.size()", 1, steps.size() );
    }

  public void testDupeSourceFail()
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

    Flow flow = null;
    try
      {
      flow = new FlowConnector().connect( sources, sinks, merge );
      flow.writeDOT( "dupesource.dot" );
      fail( "did not fail on dupe source" );
      }
    catch( Exception exception )
      {

      }
    }

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

    Flow flow = new FlowConnector().connect( sources, sinks, merge );

//    flow.writeDOT( "merged.dot" );

    List<FlowStep> steps = flow.getSteps();

    assertEquals( "not equal: steps.size()", 3, steps.size() );
    }


  private static class TestAssembly extends PipeAssembly
    {
    public TestAssembly( String name )
      {
      Pipe pipe = new Pipe( name );

      pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

      setTails( pipe );
      }
    }

  /** Tests that proper pipe graph is assembled without throwing an internal error */
  public void testPipeAssembly()
    {
    Pipe pipe = new TestAssembly( "test" );
    pipe = new GroupBy( pipe, new Fields( "ip" ) );

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), "foo" );
    Tap sink = new Hfs( new TextLine(), "foo/split1", true );

    List<FlowStep> steps = new FlowConnector().connect( source, sink, pipe ).getSteps();

    assertEquals( "not equal: steps.size()", 1, steps.size() );
    }

  public void testPipeAssemblySplit()
    {
    Pipe pipe = new TestAssembly( "test" );
    Pipe pipe1 = new GroupBy( "left", pipe, new Fields( "ip" ) );
    Pipe pipe2 = new GroupBy( "right", pipe, new Fields( "ip" ) );

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), "foo" );
    Tap sink1 = new Hfs( new TextLine(), "foo/split1", true );
    Tap sink2 = new Hfs( new TextLine(), "foo/split2", true );

    Map sources = new HashMap();
    sources.put( "test", source );

    Map sinks = new HashMap();
    sinks.put( "left", sink1 );
    sinks.put( "right", sink2 );

    List<FlowStep> steps = new FlowConnector().connect( sources, sinks, pipe1, pipe2 ).getSteps();

    assertEquals( "not equal: steps.size()", 2, steps.size() );
    }

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

    Flow flow = new FlowConnector().connect( sources, sink, splice2 );

//    flow.writeDOT( "cogroupcogroupopt.dot" );

    assertEquals( "not equal: steps.size()", 3, flow.getSteps().size() );
    }

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
    FlowConnector.setIntermediateSchemeClass( properties, TextLine.class );

    FlowConnector flowConnector = new FlowConnector( properties );

    Flow flow = flowConnector.connect( sources, sink, splice2 );

//    flow.writeDOT( "cogroupcogroupopt.dot" );

    assertEquals( "not equal: steps.size()", 2, flow.getSteps().size() );
    }

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

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper1 = new Each( new Pipe( "upper1" ), new Fields( "line" ), splitter );
    Pipe pipeUpper2 = new Each( new Pipe( "upper2" ), new Fields( "line" ), splitter );

    Pipe splice1 = new CoGroup( pipeLower, new Fields( "num" ), pipeUpper1, new Fields( "num" ), new Fields( "num1", "char1", "num2", "char2" ) );

    splice1 = new Each( splice1, new Identity() );

    Pipe splice2 = new CoGroup( splice1, new Fields( "num1" ), pipeUpper2, new Fields( "num" ), new Fields( "num1", "char1", "num2", "char2", "num3", "char3" ) );

    splice2 = new Each( splice2, new Identity() );

    splice2 = new CoGroup( splice2, new Fields( "num1" ), splice1, new Fields( "num1" ), new Fields( "num1", "char1", "num2", "char2", "num3", "char3", "num4", "char4", "num5", "char5" ) );

    Flow flow = null;
    try
      {
      flow = new FlowConnector().connect( sources, sink, splice2 );
      }
    catch( FlowException exception )
      {
//      exception.writeDOT( "cogroupcogroup.dot" );
      throw exception;
      }

//    flow.writeDOT( "cogroupcogroup.dot" );

    assertEquals( "not equal: steps.size()", 4, flow.getSteps().size() );
    }

  /**
   * This tests if two pipes can have the same name, and thus logically the same input source.
   * <p/>
   * Further, a GroupBy with two inputs would fail if the source was directly associated. but there is a Group
   * function between the source and the merge, so it passes.
   *
   * @throws IOException
   */
  public void testSameHeadName() throws IOException
    {
    Map sources = new HashMap();
    Map sinks = new HashMap();

    sources.put( "a", new Hfs( new Fields( "first", "second" ), "input/path/a" ) );

    Pipe pipeA = new Pipe( "a" );
    Pipe pipeB = new Pipe( "a" );

    Pipe group1 = new Group( "a1", pipeA );
    Pipe group2 = new Group( "a2", pipeB );

    Pipe merge = new GroupBy( "tail", Pipe.pipes( group1, group2 ), new Fields( "first", "second" ) );

    sinks.put( merge.getName(), new Hfs( new TextLine(), "output/path" ) );

    Flow flow = new FlowConnector().connect( sources, sinks, merge );

    assertEquals( "not equal: steps.size()", 3, flow.getSteps().size() );
    }

  /**
   * Verifies the same tap instance can be shared between two logically different pipes.
   *
   * @throws IOException
   */
  public void testSameTaps() throws IOException
    {
    Map sources = new HashMap();
    Map sinks = new HashMap();

    Hfs tap = new Hfs( new Fields( "first", "second" ), "input/path/a" );
    sources.put( "a", tap );
    sources.put( "b", tap );

    Pipe pipeA = new Pipe( "a" );
    Pipe pipeB = new Pipe( "b" );

    Pipe group1 = new Group( pipeA );
    Pipe group2 = new Group( pipeB );

    Pipe merge = new GroupBy( "tail", Pipe.pipes( group1, group2 ), new Fields( "first", "second" ) );

    sinks.put( merge.getName(), new Hfs( new TextLine(), "output/path" ) );

    Flow flow = new FlowConnector().connect( sources, sinks, merge );

//    flow.writeDOT( "sametaps.dot" );

    assertEquals( "not equal: steps.size()", 3, flow.getSteps().size() );
    }

  public void testDanglingHead() throws IOException
    {
    Map sources = new HashMap();
    Map sinks = new HashMap();

    Hfs tap = new Hfs( new Fields( "first", "second" ), "input/path/a" );
    sources.put( "a", tap );
//    sources.put( "b", tap );

    Pipe pipeA = new Pipe( "a" );
    Pipe pipeB = new Pipe( "b" );

    Pipe group1 = new Group( pipeA );
    Pipe group2 = new Group( pipeB );

    Pipe merge = new GroupBy( "tail", Pipe.pipes( group1, group2 ), new Fields( "first", "second" ) );

    sinks.put( merge.getName(), new Hfs( new TextLine(), "output/path" ) );

    try
      {
      Flow flow = new FlowConnector().connect( sources, sinks, merge );
      fail( "did not catch missing sink tap" );
      }
    catch( Exception exception )
      {
      // do nothing
      }
    }

  public void testDanglingTail() throws IOException
    {
    Map sources = new HashMap();
    Map sinks = new HashMap();

    Hfs tap = new Hfs( new Fields( "first", "second" ), "input/path/a" );
    sources.put( "a", tap );
    sources.put( "b", tap );

    Pipe pipeA = new Pipe( "a" );
    Pipe pipeB = new Pipe( "b" );

    Pipe group1 = new Group( pipeA );
    Pipe group2 = new Group( pipeB );

    Pipe merge = new GroupBy( "tail", Pipe.pipes( group1, group2 ), new Fields( "first", "second" ) );

//    sinks.put( merge.getName(), new Hfs( new TextLine(), "output/path" ) );

    try
      {
      Flow flow = new FlowConnector().connect( sources, sinks, merge );
      fail( "did not catch missing source tap" );
      }
    catch( Exception exception )
      {
      // do nothing
      }
    }

  private int countDistance( SimpleDirectedGraph<FlowElement, Scope> graph, FlowElement lhs, FlowElement rhs )
    {
    return DijkstraShortestPath.findPathBetween( graph, lhs, rhs ).size() - 1;
    }
  }
