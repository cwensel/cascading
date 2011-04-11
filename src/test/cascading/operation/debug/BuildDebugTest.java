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

package cascading.operation.debug;

import java.util.Collection;
import java.util.Map;

import cascading.PlatformTestCase;
import cascading.TestConstants;
import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.planner.FlowStep;
import cascading.operation.AssertionLevel;
import cascading.operation.Debug;
import cascading.operation.DebugLevel;
import cascading.operation.Operation;
import cascading.operation.assertion.AssertMatches;
import cascading.operation.assertion.AssertNotNull;
import cascading.operation.regex.RegexFilter;
import cascading.operation.regex.RegexParser;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.test.PlatformTest;
import cascading.tuple.Fields;

/**
 *
 */
@PlatformTest(platforms = {"local", "hadoop"})
public class BuildDebugTest extends PlatformTestCase
  {
  public BuildDebugTest()
    {
    }

  /**
   * verify lone group assertion fails
   *
   * @throws Exception
   */
  public void testDebugLevels() throws Exception
    {
    Tap source = getPlatform().getTextFile( "input" );
    Tap sink = getPlatform().getTextFile( "output" );

    Pipe pipe = new Pipe( "test" );

    String regex = TestConstants.APACHE_COMMON_REGEX;
    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip", "time", "method", "event", "status", "size" ), regex, new int[]{
      1, 2, 3, 4, 5, 6} ) );

    pipe = new Each( pipe, AssertionLevel.STRICT, new AssertNotNull() );

    pipe = new Each( pipe, DebugLevel.DEFAULT, new Debug() );

    pipe = new Each( pipe, DebugLevel.VERBOSE, new Debug() );

    pipe = new Each( pipe, new Fields( "method" ), new RegexFilter( "^POST" ) );

    pipe = new Each( pipe, new Fields( "method" ), AssertionLevel.STRICT, new AssertMatches( "^POST" ) );

    pipe = new GroupBy( pipe, new Fields( "method" ) );

    Map<Object, Object> properties = getProperties();

    // test default config case
    assertEquals( getDebugCount( getPlatform().getFlowConnector( properties ).connect( source, sink, pipe ) ), 1 );

    HadoopFlowConnector.setDebugLevel( properties, DebugLevel.DEFAULT );
    assertEquals( getDebugCount( getPlatform().getFlowConnector( properties ).connect( source, sink, pipe ) ), 1 );

    HadoopFlowConnector.setDebugLevel( properties, DebugLevel.VERBOSE );
    assertEquals( getDebugCount( getPlatform().getFlowConnector( properties ).connect( source, sink, pipe ) ), 2 );

    HadoopFlowConnector.setDebugLevel( properties, DebugLevel.NONE );
    assertEquals( getDebugCount( getPlatform().getFlowConnector( properties ).connect( source, sink, pipe ) ), 0 );
    }

  private int getDebugCount( Flow flow )
    {
    FlowStep step = (FlowStep) flow.getSteps().get( 0 );

    Collection<Operation> operations = step.getAllOperations();
    int count = 0;

    for( Operation operation : operations )
      {
      if( operation instanceof Debug )
        count++;
      }

    return count;
    }
  }