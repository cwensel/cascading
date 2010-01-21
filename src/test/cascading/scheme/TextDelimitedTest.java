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

package cascading.scheme;

import java.io.IOException;
import java.util.Properties;
import java.util.regex.Pattern;

import cascading.CascadingTestCase;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.pipe.Pipe;
import cascading.tap.Hfs;
import cascading.tap.SinkMode;
import cascading.tuple.Fields;

/**
 *
 */
public class TextDelimitedTest extends CascadingTestCase
  {
  String testData = "build/test/data/delimited.txt";

  String outputPath = "build/test/output/delim";


  public TextDelimitedTest()
    {
    super( "delimite text tests" );
    }

  public void testQuotedText() throws IOException
    {
    Properties properties = new Properties();

    Class[] types = new Class[]{String.class, String.class, String.class, long.class};
    Fields fields = new Fields( "first", "second", "third", "fourth" );
    Hfs text = new Hfs( new TextDelimited( fields, ",", "\"", types ), testData );

    Hfs output = new Hfs( new TextLine( new Fields( "line" ) ), outputPath + "/quoted", SinkMode.REPLACE );

    Pipe pipe = new Pipe( "pipe" );

    Flow flow = new FlowConnector( properties ).connect( text, output, pipe );

    flow.complete();

    validateLength( flow, 4, 1, Pattern.compile( "foo\\t(bar.*)\\tbaz\\t[0-9]" ) );
    }
  }
