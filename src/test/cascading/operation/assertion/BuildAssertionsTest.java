/*
 * Copyright (c) 2007-20010 Concurrent, Inc. All Rights Reserved.
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

package cascading.operation.assertion;

import cascading.CascadingTestCase;
import cascading.TestConstants;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.AssertionLevel;
import cascading.operation.regex.RegexFilter;
import cascading.operation.regex.RegexParser;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/**
 *
 */
public class BuildAssertionsTest extends CascadingTestCase
  {
  public BuildAssertionsTest()
    {
    super( "build assertions" );
    }

  /**
   * verify lone group assertion fails
   *
   * @throws Exception
   */
  public void testLoneGroupAssertion() throws Exception
    {
    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), "input" );
    Tap sink = new Hfs( new TextLine(), "output", true );

    Pipe pipe = new Pipe( "test" );

    String regex = TestConstants.APACHE_COMMON_REGEX;
    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip", "time", "method", "event", "status", "size" ), regex, new int[]{
      1, 2, 3, 4, 5, 6} ) );

    pipe = new Each( pipe, AssertionLevel.STRICT, new AssertNotNull() );

    pipe = new Each( pipe, new Fields( "method" ), new RegexFilter( "^POST" ) );

    pipe = new Each( pipe, new Fields( "method" ), AssertionLevel.STRICT, new AssertMatches( "^POST" ) );

    pipe = new GroupBy( pipe, new Fields( "method" ) );

    pipe = new Every( pipe, AssertionLevel.STRICT, new AssertGroupSizeEquals( 7L ) );

    try
      {
      Flow flow = new FlowConnector().connect( source, sink, pipe );
      fail( "did not throw lone group assertion error" );
      }
    catch( Exception exception )
      {
      // ignore
      }
    }
  }