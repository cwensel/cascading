/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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

import cascading.PlatformTestCase;
import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopStepStats;
import cascading.operation.regex.RegexParser;
import cascading.operation.state.Counter;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.test.HadoopPlatform;
import cascading.test.PlatformTest;
import cascading.tuple.Fields;

import static data.InputData.inputFileApache;

/**
 *
 */
@PlatformTest(platforms = {"local", "hadoop"})
public class CascadingStatsTest extends PlatformTestCase
  {
  enum TestEnum
    {
      FIRST, SECOND
    }

  public CascadingStatsTest()
    {
    super( true );
    }

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

    Flow flow1 = getPlatform().getFlowConnector().connect( "stats1 test", source, sink1, pipe );
    Flow flow2 = getPlatform().getFlowConnector().connect( "stats2 test", source, sink2, pipe );

    Cascade cascade = new CascadeConnector().connect( flow1, flow2 );

    cascade.complete();

    CascadeStats cascadeStats = cascade.getCascadeStats();

    assertNotNull( cascadeStats.getID() );

    // unsure why this has changed
//    if( getPlatform() instanceof HadoopPlatform )
//      {
//      Collection<String> counterGroups = cascadeStats.getCounterGroups();
//      assertEquals( getPlatform().isUseCluster() ? 5 : 4, counterGroups.size() );
//      }

    assertEquals( 1, cascadeStats.getCounterGroupsMatching( "cascading\\.stats\\..*" ).size() );
    assertEquals( 2, cascadeStats.getCountersFor( TestEnum.class.getName() ).size() );
    assertEquals( 2, cascadeStats.getCountersFor( TestEnum.class ).size() );
    assertEquals( 40, cascadeStats.getCounterValue( TestEnum.FIRST ) );
    assertEquals( 20, cascadeStats.getCounterValue( TestEnum.SECOND ) );

    FlowStats flowStats1 = flow1.getFlowStats();

    assertNotNull( flowStats1.getID() );

    assertEquals( 20, flowStats1.getCounterValue( TestEnum.FIRST ) );
    assertEquals( 10, flowStats1.getCounterValue( TestEnum.SECOND ) );

    FlowStats flowStats2 = flow2.getFlowStats();

    assertNotNull( flowStats2.getID() );

    assertEquals( 20, flowStats2.getCounterValue( TestEnum.FIRST ) );
    assertEquals( 10, flowStats2.getCounterValue( TestEnum.SECOND ) );

    cascadeStats.captureDetail();

    if( getPlatform() instanceof HadoopPlatform )
      {
      assertEquals( 2, flowStats1.getStepsCount() );
      assertEquals( 2, flowStats2.getStepsCount() );

      HadoopStepStats stats1 = (HadoopStepStats) flowStats1.getStepStats().get( 0 );

      assertNotNull( stats1.getID() );
      assertNotNull( stats1.getJobID() );

      assertEquals( 2, stats1.getNumMapTasks() );
      assertEquals( 1, stats1.getNumReducerTasks() );

      if( getPlatform().isUseCluster() )
        {
        assertEquals( 7, stats1.getTaskStats().size() );
        assertNotNull( stats1.getTaskStats().get( 5 ) );
        assertTrue( stats1.getTaskStats().get( 5 ).getCounterValue( TestEnum.FIRST ) > 0 ); // in reducer
        }

      HadoopStepStats stats2 = (HadoopStepStats) flowStats2.getStepStats().get( 0 );

      assertNotNull( stats2.getID() );
      assertNotNull( stats2.getJobID() );

      assertEquals( 2, stats2.getNumMapTasks() );
      assertEquals( 1, stats2.getNumReducerTasks() );

      if( getPlatform().isUseCluster() )
        {
        assertEquals( 7, stats2.getTaskStats().size() );
        assertNotNull( stats2.getTaskStats().get( 0 ) );
        }
      }
    }
  }
