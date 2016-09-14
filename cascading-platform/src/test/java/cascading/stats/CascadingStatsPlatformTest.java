/*
 * Copyright (c) 2007-2016 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cascading.stats;

import java.util.HashMap;
import java.util.Map;

import cascading.PlatformTestCase;
import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowRuntimeProps;
import cascading.flow.SliceCounters;
import cascading.operation.Function;
import cascading.operation.regex.RegexParser;
import cascading.operation.regex.RegexSplitter;
import cascading.operation.state.Counter;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.HashJoin;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import org.junit.Test;

import static data.InputData.*;

/**
 *
 */
public class CascadingStatsPlatformTest extends PlatformTestCase
  {
  enum TestEnum
    {
      FIRST, SECOND, THIRD
    }

  public CascadingStatsPlatformTest()
    {
    super( true );
    }

  @Test
  public void testStatsCounters() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( inputFileApache );

    Pipe pipe = new Pipe( "first" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );
    pipe = new GroupBy( pipe, new Fields( "ip" ) );
    pipe = new Each( pipe, new Counter( TestEnum.FIRST ) );
    pipe = new GroupBy( pipe, new Fields( "ip" ) );
    pipe = new Each( pipe, new Counter( TestEnum.FIRST ) );
    pipe = new Each( pipe, new Counter( TestEnum.SECOND ) );

    Tap sink1 = getPlatform().getTextFile( getOutputPath( "flowstats1" ), SinkMode.REPLACE );
    Tap sink2 = getPlatform().getTextFile( getOutputPath( "flowstats2" ), SinkMode.REPLACE );

    Map<Object, Object> properties = getProperties();

    properties = FlowRuntimeProps.flowRuntimeProps()
      .addLogCounter( SliceCounters.Tuples_Read )
      .buildProperties( properties );

    FlowConnector flowConnector = getPlatform().getFlowConnector( properties );

    Flow flow1 = flowConnector.connect( "stats1 test", source, sink1, pipe );
    Flow flow2 = flowConnector.connect( "stats2 test", source, sink2, pipe );

    Cascade cascade = new CascadeConnector( getProperties() ).connect( flow1, flow2 );

    cascade.complete();

    CascadeStats cascadeStats = cascade.getCascadeStats();

    assertNotNull( cascadeStats.getID() );

    assertEquals( 2, cascadeStats.getCountersFor( TestEnum.class.getName() ).size() );
    assertEquals( 2, cascadeStats.getCountersFor( TestEnum.class ).size() );
    assertEquals( 40, cascadeStats.getCounterValue( TestEnum.FIRST ) );
    assertEquals( 20, cascadeStats.getCounterValue( TestEnum.SECOND ) );

    // should not throw npe
    assertEquals( 0, cascadeStats.getCounterValue( TestEnum.THIRD ) );
    assertEquals( 0, cascadeStats.getCounterValue( "FOO", "BAR" ) );

    FlowStats flowStats1 = flow1.getFlowStats();

    assertNotNull( flowStats1.getID() );

    assertEquals( 20, flowStats1.getCounterValue( TestEnum.FIRST ) );
    assertEquals( 10, flowStats1.getCounterValue( TestEnum.SECOND ) );

    // should not throw npe
    assertEquals( 0, flowStats1.getCounterValue( TestEnum.THIRD ) );
    assertEquals( 0, flowStats1.getCounterValue( "FOO", "BAR" ) );

    FlowStats flowStats2 = flow2.getFlowStats();

    assertNotNull( flowStats2.getID() );

    assertEquals( 20, flowStats2.getCounterValue( TestEnum.FIRST ) );
    assertEquals( 10, flowStats2.getCounterValue( TestEnum.SECOND ) );

    cascadeStats.captureDetail();
    }

  @Test
  public void testStatsOnJoin() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    Tap sourceLower = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLower );
    Tap sourceUpper = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileUpper );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "join" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    pipeLower = new Each( pipeLower, new Counter( TestEnum.FIRST ) );

    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );

    pipeUpper = new Each( pipeUpper, new Counter( TestEnum.SECOND ) );

    Pipe splice = new HashJoin( pipeLower, new Fields( "num" ), pipeUpper, new Fields( "num" ), Fields.size( 4 ) );

    Map<Object, Object> properties = getProperties();

    Flow flow = getPlatform().getFlowConnector( properties ).connect( sources, sink, splice );

    flow.complete();

    validateLength( flow, 5 );

    FlowStats flowStats = flow.getFlowStats();

    assertNotNull( flowStats.getID() );

    long firstCounter = flowStats.getCounterValue( TestEnum.FIRST );
    long secondCounter = flowStats.getCounterValue( TestEnum.SECOND );

    assertEquals( 5, firstCounter );
    assertNotSame( 0, secondCounter ); // verifies accumulated side counters fired
    assertEquals( firstCounter + secondCounter, flowStats.getCounterValue( SliceCounters.Tuples_Read ) );
    }
  }
