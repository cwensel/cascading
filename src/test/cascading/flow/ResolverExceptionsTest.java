/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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

import cascading.PlatformTestCase;
import cascading.flow.planner.PlannerException;
import cascading.operation.Identity;
import cascading.operation.aggregator.Count;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.test.PlatformTest;
import cascading.tuple.Fields;
import cascading.tuple.FieldsResolverException;

/**
 * This test helps maintain consistent error messages across resolver failures.
 * <p/>
 * add new resolver usecases to the test suite.
 */
@PlatformTest(platforms = {"local", "hadoop"})
public class ResolverExceptionsTest extends PlatformTestCase
  {
  public ResolverExceptionsTest()
    {
    }

  private void verify( Tap source, Tap sink, Pipe pipe )
    {
    try
      {
      getPlatform().getFlowConnector().connect( source, sink, pipe );
      fail( "no exception thrown" );
      }
    catch( Exception exception )
      {
      assertTrue( exception instanceof PlannerException );
      assertTrue( exception.getCause().getCause() instanceof FieldsResolverException );
      }
    }

  public void testSchemeResolver() throws Exception
    {
    Fields sourceFields = new Fields( "first", "second" );
    Tap source = getPlatform().getDelimitedFile( sourceFields, "input/path", SinkMode.KEEP );

    Fields sinkFields = new Fields( "third", "fourth" );
    Tap sink = getPlatform().getDelimitedFile( sinkFields, "output/path", SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );

    verify( source, sink, pipe );
    }

  public void testEachArgResolver() throws Exception
    {
    Fields sourceFields = new Fields( "first", "second" );
    Tap source = getPlatform().getDelimitedFile( sourceFields, "input/path", SinkMode.KEEP );

    Fields sinkFields = new Fields( "third", "fourth" );
    Tap sink = getPlatform().getDelimitedFile( sinkFields, "output/path", SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );
    pipe = new Each( pipe, new Fields( "third" ), new Identity() );

    verify( source, sink, pipe );
    }

  public void testEachOutResolver() throws Exception
    {
    Fields sourceFields = new Fields( "first", "second" );
    Tap source = getPlatform().getDelimitedFile( sourceFields, "input/path", SinkMode.KEEP );

    Fields sinkFields = new Fields( "third", "fourth" );
    Tap sink = getPlatform().getDelimitedFile( sinkFields, "output/path", SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );
    pipe = new Each( pipe, new Fields( "first" ), new Identity( new Fields( "none" ) ), new Fields( "third" ) );

    verify( source, sink, pipe );
    }

  public void testGroupByResolver() throws Exception
    {
    Fields sourceFields = new Fields( "first", "second" );
    Tap source = getPlatform().getDelimitedFile( sourceFields, "input/path", SinkMode.KEEP );

    Fields sinkFields = new Fields( "third", "fourth" );
    Tap sink = getPlatform().getDelimitedFile( sinkFields, "output/path", SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );
    pipe = new GroupBy( pipe, new Fields( "third" ) );

    verify( source, sink, pipe );
    }

  public void testGroupBySortResolver() throws Exception
    {
    Fields sourceFields = new Fields( "first", "second" );
    Tap source = getPlatform().getDelimitedFile( sourceFields, "input/path", SinkMode.KEEP );

    Fields sinkFields = new Fields( "third", "fourth" );
    Tap sink = getPlatform().getDelimitedFile( sinkFields, "output/path", SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );
    pipe = new GroupBy( pipe, new Fields( "first" ), new Fields( "third" ) );

    verify( source, sink, pipe );
    }

  public void testEveryArgResolver() throws Exception
    {
    Fields sourceFields = new Fields( "first", "second" );
    Tap source = getPlatform().getDelimitedFile( sourceFields, "input/path", SinkMode.KEEP );

    Fields sinkFields = new Fields( "third", "fourth" );
    Tap sink = getPlatform().getDelimitedFile( sinkFields, "output/path", SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );
    pipe = new GroupBy( pipe, new Fields( "first" ) );

    pipe = new Every( pipe, new Fields( "third" ), new Count() );

    verify( source, sink, pipe );
    }

  public void testEveryOutResolver() throws Exception
    {
    Fields sourceFields = new Fields( "first", "second" );
    Tap source = getPlatform().getDelimitedFile( sourceFields, "input/path", SinkMode.KEEP );

    Fields sinkFields = new Fields( "third", "fourth" );
    Tap sink = getPlatform().getDelimitedFile( sinkFields, "output/path", SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );
    pipe = new GroupBy( pipe, new Fields( "first" ) );

    pipe = new Every( pipe, new Fields( "second" ), new Count(), new Fields( "third" ) );

    verify( source, sink, pipe );
    }

  }
