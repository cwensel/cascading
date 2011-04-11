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

package cascading.pipe;

import java.util.HashMap;
import java.util.Map;

import cascading.PlatformTestCase;
import cascading.flow.Flow;
import cascading.operation.Function;
import cascading.operation.regex.RegexSplitter;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.test.PlatformTest;
import cascading.tuple.Fields;

/**
 *
 */
@PlatformTest(platforms = {"local", "hadoop"})
public class MergeGroupTest extends PlatformTestCase
  {
  public MergeGroupTest()
    {
    }

  public void testBuildMerge()
    {
    Tap sourceLower = getPlatform().getTextFile( "file1" );
    Tap sourceUpper = getPlatform().getTextFile( "file2" );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Tap sink = getPlatform().getTextFile( "outpath", SinkMode.REPLACE );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );

    Pipe splice = new Group( "merge", Pipe.pipes( pipeLower, pipeUpper ), new Fields( "num" ), null, false );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, splice );
    }

  public void testBuildMergeFail()
    {
    Tap sourceLower = getPlatform().getTextFile( "file1" );
    Tap sourceUpper = getPlatform().getTextFile( "file2" );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Function splitter1 = new RegexSplitter( new Fields( "num", "foo" ), " " );
    Function splitter2 = new RegexSplitter( new Fields( "num", "bar" ), " " );

    Tap sink = getPlatform().getTextFile( "outpath", SinkMode.REPLACE );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter1 );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter2 );

    Pipe splice = new Group( "merge", Pipe.pipes( pipeLower, pipeUpper ), new Fields( "num" ), null, false );

    try
      {
      Flow flow = getPlatform().getFlowConnector().connect( sources, sink, splice );
      fail( "did not fail on mismatched field names" );
      }
    catch( Exception exception )
      {
      // test passes
      }
    }
  }
