/*
 * Copyright (c) 2007-2008 Vinculum Technologies, Inc. All Rights Reserved.
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

import cascading.CascadingTestCase;
import cascading.operation.Identity;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexFilter;
import cascading.operation.regex.RegexParser;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.Group;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.PipeAssembly;
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
   * Test a single piece Pipe, should fail on connect()
   *
   * @throws IOException
   */
  public void testIdentity() throws Exception
    {
    Tap source = new Hfs( new TextLine(), "input/path" );
    Tap sink = new Hfs( new TextLine(), "output/path", true );

    Pipe pipe = new Pipe( "test" );

    try
      {
      new FlowConnector().connect( source, sink, pipe );
      fail( "did not throw failure on attept to connect source to sink" );
      }
    catch( FlowException exception )
      {
      }
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

  /** This should result in a Temp Tap after the Every. Pushing the next Each to be run inside the next two parallel steps */
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

    List<FlowStep> steps = new FlowConnector().connect( sources, sinks, left, right ).getSteps();

    assertEquals( "not equal: steps.size()", 3, steps.size() );

    FlowStep step = steps.get( 0 );

    Scope nextScope = step.getNextScope( step.group );
    FlowElement operator = step.getNextFlowElement( nextScope );

    assertTrue( "not an Every", operator instanceof Every );

    nextScope = step.getNextScope( operator );
    operator = step.getNextFlowElement( nextScope );

    assertTrue( "not a TempHfs", operator instanceof TempHfs );
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

  private int countDistance( SimpleDirectedGraph<FlowElement, Scope> graph, FlowElement lhs, FlowElement rhs )
    {
    return DijkstraShortestPath.findPathBetween( graph, lhs, rhs ).size() - 1;
    }
  }
