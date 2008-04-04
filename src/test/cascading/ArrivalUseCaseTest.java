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

package cascading;

import java.io.File;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.Regexes;
import cascading.operation.text.Texts;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.Group;
import cascading.pipe.Pipe;
import cascading.scheme.TextLine;
import cascading.tap.Lfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/** @version $Id: //depot/calku/cascading/src/test/cascading/ArrivalUseCaseTest.java#2 $ */
public class ArrivalUseCaseTest extends CascadingTestCase
  {
  String inputFileApache = "build/test/data/apache.200.txt";

  String outputPath = "build/test/output/arrival/";

  public ArrivalUseCaseTest()
    {
    super( "arrival rate" );
    }

  /**
   * Calculate the arrival rate in a (small) apache log file
   *
   * @throws java.io.IOException
   */
  public void testArrivalRate() throws Exception
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    Tap source = new Lfs( new TextLine(), inputFileApache );
    Tap sink = new Lfs( new TextLine(), outputPath, true );

    Pipe pipe = new Pipe( "apache" );

    // input: offset, line
    // args: line
    // output: "ip", "time", "method", "event", "status", "size"
    pipe = new Each( pipe, new Fields( 1 ), Regexes.APACHE_COMMON_PARSER );

    // args: time
    // output: ts
    pipe = new Each( pipe, new Fields( 1 ), Texts.APACHE_DATE_PARSER );

    pipe = new Group( pipe, new Fields( 0 ) );
    pipe = new Every( pipe, new Fields( 0 ), new Count(), new Fields( 0, 1 ) );

    Flow flow = new FlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 161 );
    }

  public void testArrivalRateNames() throws Exception
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    Tap source = new Lfs( new TextLine(), inputFileApache );
    Tap sink = new Lfs( new TextLine(), outputPath, true );

    Pipe pipe = new Pipe( "apache" );

    // RegexParser.APACHE declares: "time", "method", "event", "status", "size"
    pipe = new Each( pipe, new Fields( "line" ), Regexes.APACHE_COMMON_PARSER );

    // DateParser.APACHE declares: "ts"
    pipe = new Each( pipe, new Fields( "time" ), Texts.APACHE_DATE_PARSER );

    pipe = new Group( pipe, new Fields( "ts" ) );
    pipe = new Every( pipe, new Fields( "ts" ), new Count() );

    Flow flow = new FlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 161 );
    }
  }
