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

package cascading.pipe;

import java.util.HashMap;
import java.util.Map;

import cascading.CascadingTestCase;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.Function;
import cascading.operation.regex.RegexSplitter;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/**
 *
 */
public class MergeGroupTest extends CascadingTestCase
  {
  public MergeGroupTest()
    {
    super( "test merge in group" );
    }

  public void testBuildMerge()
    {
    Tap sourceLower = new Hfs( new TextLine( new Fields( "offset", "line" ) ), "file1" );
    Tap sourceUpper = new Hfs( new TextLine( new Fields( "offset", "line" ) ), "file2" );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    // using null pos so all fields are written
    Tap sink = new Hfs( new TextLine(), "outpath", true );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );

    Pipe splice = new Group( "merge", Pipe.pipes( pipeLower, pipeUpper ), new Fields( "num" ), null, false );

    Flow flow = new FlowConnector().connect( sources, sink, splice );

    }

  public void testBuildMergeFail()
    {
    Tap sourceLower = new Hfs( new TextLine( new Fields( "offset", "line" ) ), "file1" );
    Tap sourceUpper = new Hfs( new TextLine( new Fields( "offset", "line" ) ), "file2" );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Function splitter1 = new RegexSplitter( new Fields( "num", "foo" ), " " );
    Function splitter2 = new RegexSplitter( new Fields( "num", "bar" ), " " );

    // using null pos so all fields are written
    Tap sink = new Hfs( new TextLine(), "outpath", true );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter1 );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter2 );

    Pipe splice = new Group( "merge", Pipe.pipes( pipeLower, pipeUpper ), new Fields( "num" ), null, false );

    try
      {
      Flow flow = new FlowConnector().connect( sources, sink, splice );
      fail( "did not fail on mismatched field names" );
      }
    catch( Exception exception )
      {

      }

    }
  }
