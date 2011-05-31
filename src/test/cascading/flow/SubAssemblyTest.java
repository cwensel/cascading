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

package cascading.flow;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cascading.CascadingTestCase;
import cascading.operation.Identity;
import cascading.operation.aggregator.First;
import cascading.operation.regex.RegexParser;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.AggregateBy;
import cascading.pipe.assembly.CountBy;
import cascading.pipe.assembly.SumBy;
import cascading.scheme.Scheme;
import cascading.scheme.TextDelimited;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/** A planner test only, does not execute */
public class SubAssemblyTest extends CascadingTestCase
  {
  public SubAssemblyTest()
    {
    super( "pipe assembly tests" );
    }

  private static class TestAssembly extends SubAssembly
    {
    public TestAssembly( String name )
      {
      this( name, false );
      }

    public TestAssembly( String name, boolean bad )
      {
      Pipe pipe = new Pipe( name );

      pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

      if( !bad )
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

  public void testBadSubAssembly()
    {
    Pipe pipe = new TestAssembly( "test", true );

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), "foo" );
    Tap sink = new Hfs( new TextLine(), "foo/split1", true );

    try
      {
      new FlowConnector().connect( source, sink, pipe );
      fail( "did not throw exception" );
      }
    catch( Exception exception )
      {
      // ignore
      }
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

  private static class FirstAssembly extends SubAssembly
    {
    public FirstAssembly( Pipe previous )
      {
      Pipe pipe = new Pipe( "first", previous );

      pipe = new Each( pipe, new Identity() );

      pipe = new GroupBy( pipe, Fields.ALL );

      pipe = new Every( pipe, new First(), Fields.RESULTS );

      setTails( pipe );
      }
    }

  private static class SecondAssembly extends SubAssembly
    {
    public SecondAssembly( Pipe previous )
      {
      Pipe pipe = new Pipe( "second", previous );

      pipe = new Each( pipe, new Identity() );

      pipe = new FirstAssembly( pipe );

      setTails( pipe );
      }
    }

  public void testNestedAssembliesAccessors() throws IOException
    {
    Pipe pipe = new Pipe( "test" );

    pipe = new SecondAssembly( pipe );

    Pipe[] allPrevious = pipe.getPrevious();

    assertEquals( "wrong number of previous", 1, allPrevious.length );

//    for( Pipe previous : allPrevious )
//      assertFalse( previous instanceof PipeAssembly );

    Pipe[] heads = pipe.getHeads();

    assertEquals( "wrong number of heads", 1, heads.length );

    for( Pipe head : heads )
      assertFalse( head instanceof SubAssembly );

    }

  public void testNestedAssemblies() throws IOException
    {
    Tap source = new Hfs( new TextLine(), "input/path" );
    Tap sink = new Hfs( new TextLine(), "output/path", true );

    Pipe pipe = new Pipe( "test" );

    pipe = new SecondAssembly( pipe );

    pipe = new GroupBy( pipe, Fields.size( 1 ) );

    try
      {
      Flow flow = new FlowConnector().connect( source, sink, pipe );

//      flow.writeDOT( "nestedassembly.dot" );

      List<FlowStep> steps = flow.getSteps();

      assertEquals( "wrong size", 2, steps.size() );
      }
    catch( PlannerException exception )
      {
//      exception.writeDOT( "nestedassembly.dot" );

      throw exception;
      }
    }

  public void testAssemblyPlanFailure()
    {
    Scheme sourceScheme = new TextDelimited( new Fields( "date", "size" ), "\t" );
    Tap source = new Hfs( sourceScheme, "input" );

    Tap sink = new Hfs( new TextLine(), "outputPath", SinkMode.REPLACE );
    Tap sink2 = new Hfs( new TextLine(), "outputPath2", SinkMode.REPLACE );

    Pipe assembly = new Pipe( "assembly" );

    Pipe assembly2 = new Pipe( "assembly2", assembly );

    Fields groupingFields = new Fields( "date" );

    assembly = new AggregateBy(
      assembly,
      groupingFields,
      new SumBy( new Fields( "size" ), new Fields( "size" ), double.class ),
      new SumBy( new Fields( "size" ), new Fields( "size2" ), double.class ),
      new CountBy( new Fields( "sizes" ) ), new CountBy( new Fields(
      "sizes2" ) )

    );

    assembly2 = new AggregateBy(
      assembly2,
      groupingFields,
      new SumBy( new Fields( "size" ), new Fields( "size" ), double.class ),
      new SumBy( new Fields( "size" ), new Fields( "size2" ), double.class ),
      new CountBy( new Fields( "sizes" ) ), new CountBy( new Fields(
      "sizes2" ) )

    );

    Map<String, Tap> sinks = new HashMap<String, Tap>();
    sinks.put( "assembly", sink );
    sinks.put( "assembly2", sink2 );

    FlowConnector flowConnector = new FlowConnector();
    // if you reverse assembly and assembly2 it works:
    //Flow flow = flowConnector.connect("test", source, sinks, assembly, assembly2);
    try
      {
      Flow flow = flowConnector.connect( "test", source, sinks, assembly2, assembly );
      fail();
      }
    catch( Exception exception )
      {
      // do nothing - test passes
      }
    }
  }
