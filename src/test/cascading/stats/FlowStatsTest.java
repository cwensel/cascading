/*
 * Copyright (c) 2007-2009 Concurrent, Inc. All Rights Reserved.
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

package cascading.stats;

import cascading.ClusterTestCase;
import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.regex.RegexParser;
import cascading.operation.state.Counter;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;

import java.io.File;

/**
 *
 */
public class FlowStatsTest extends ClusterTestCase
  {

  String inputFileApache = "build/test/data/apache.10.txt";
  String outputPath = "build/test/output/flowstats/";

  enum TestEnum
    {
      FIRST, SECOND
    }

  public FlowStatsTest()
    {
    super( "flow stats tests", true );
    }

  public void testStatsCounters() throws Exception
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileApache );

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileApache );

    Pipe pipe = new Pipe( "first" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );
    pipe = new GroupBy( pipe, new Fields( "ip" ) );
    pipe = new Each( pipe, new Counter( TestEnum.FIRST ) );
    pipe = new GroupBy( pipe, new Fields( "ip" ) );
    pipe = new Each( pipe, new Counter( TestEnum.FIRST ) );
    pipe = new Each( pipe, new Counter( TestEnum.SECOND ) );

    Tap sink1 = new Hfs( new TextLine(), outputPath + "flowstats1", true );
    Tap sink2 = new Hfs( new TextLine(), outputPath + "flowstats2", true );

    Flow flow1 = new FlowConnector( getProperties() ).connect( "stats1 test", source, sink1, pipe );
    Flow flow2 = new FlowConnector( getProperties() ).connect( "stats2 test", source, sink2, pipe );

    Cascade cascade = new CascadeConnector().connect( flow1, flow2 );

    cascade.complete();

    CascadeStats cascadeStats = cascade.getCascadeStats();

    assertEquals( 40, cascadeStats.getCounter( TestEnum.FIRST ) );
    assertEquals( 20, cascadeStats.getCounter( TestEnum.SECOND ) );

    FlowStats flowStats1 = flow1.getFlowStats();

    assertEquals( 20, flowStats1.getCounter( TestEnum.FIRST ) );
    assertEquals( 10, flowStats1.getCounter( TestEnum.SECOND ) );

    FlowStats fowStats2 = flow2.getFlowStats();

    assertEquals( 20, fowStats2.getCounter( TestEnum.FIRST ) );
    assertEquals( 10, fowStats2.getCounter( TestEnum.SECOND ) );
    }
  }
