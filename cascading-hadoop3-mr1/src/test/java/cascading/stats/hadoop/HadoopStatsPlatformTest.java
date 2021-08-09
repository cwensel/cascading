/*
 * Copyright (c) 2007-2017 Xplenty, Inc. All Rights Reserved.
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

package cascading.stats.hadoop;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cascading.PlatformTestCase;
import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
import cascading.flow.Flow;
import cascading.flow.SliceCounters;
import cascading.operation.regex.RegexParser;
import cascading.operation.state.Counter;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.stats.CascadeStats;
import cascading.stats.FlowNodeStats;
import cascading.stats.FlowSliceStats;
import cascading.stats.FlowStats;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import org.junit.Test;

import static data.InputData.inputFileApache;

/**
 *
 */
public class HadoopStatsPlatformTest extends PlatformTestCase
  {
  enum TestEnum
    {
      FIRST, SECOND, THIRD
    }

  public HadoopStatsPlatformTest()
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

    Flow flow1 = getPlatform().getFlowConnector().connect( "stats1 test", source, sink1, pipe );
    Flow flow2 = getPlatform().getFlowConnector().connect( "stats2 test", source, sink2, pipe );

    Cascade cascade = new CascadeConnector( getProperties() ).connect( flow1, flow2 );

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

    // should not throw npe
    assertEquals( 0, cascadeStats.getCounterValue( TestEnum.THIRD ) );
    assertEquals( 0, cascadeStats.getCounterValue( "FOO", "BAR" ) );

    FlowStats flowStats1 = flow1.getFlowStats();

    assertNotNull( flowStats1.getID() );

    assertEquals( 20, flowStats1.getCounterValue( TestEnum.FIRST ) );
    assertEquals( 10, flowStats1.getCounterValue( TestEnum.SECOND ) );

    assertEquals( 10, flowStats1.getFlowStepStats().get( 0 ).getCounterValue( TestEnum.FIRST ) );
    assertEquals( 10, flowStats1.getFlowStepStats().get( 1 ).getCounterValue( TestEnum.FIRST ) );

    // should not throw npe
    assertEquals( 0, flowStats1.getCounterValue( TestEnum.THIRD ) );
    assertEquals( 0, flowStats1.getCounterValue( "FOO", "BAR" ) );

    FlowStats flowStats2 = flow2.getFlowStats();

    assertNotNull( flowStats2.getID() );

    assertEquals( 20, flowStats2.getCounterValue( TestEnum.FIRST ) );
    assertEquals( 10, flowStats2.getCounterValue( TestEnum.SECOND ) );

    cascadeStats.captureDetail();

    assertEquals( 2, flowStats1.getStepsCount() );
    assertEquals( 2, flowStats2.getStepsCount() );

    HadoopStepStats stats1 = (HadoopStepStats) flowStats1.getFlowStepStats().get( 0 );

    assertNotNull( stats1.getID() );
    assertNotNull( stats1.getProcessStepID() );

//      assertEquals( 2, stats1.getNumMapTasks() );
//      assertEquals( 1, stats1.getNumReducerTasks() );

    if( getPlatform().isUseCluster() )
      {
      assertTrue( stats1.getCounterValue( SliceCounters.Process_Duration ) != 0L );

      List<FlowNodeStats> flowNodeStats = stats1.getFlowNodeStats();

      assertTrue( flowNodeStats.get( 0 ).getCounterValue( SliceCounters.Process_Duration ) != 0L );

      assertEquals( 2, flowNodeStats.size() );

      Map<String, FlowNodeStats> statsMap = new HashMap<>();

      statsMap.put( flowNodeStats.get( 0 ).getKind(), flowNodeStats.get( 0 ) );
      statsMap.put( flowNodeStats.get( 1 ).getKind(), flowNodeStats.get( 1 ) );

      assertNotNull( statsMap.get( "MAPPER" ) );
      assertNotNull( statsMap.get( "REDUCER" ) );

      assertEquals( 4, statsMap.get( "MAPPER" ).getChildren().size() );
      assertEquals( 1, statsMap.get( "REDUCER" ).getChildren().size() );

      Collection<FlowSliceStats> children = statsMap.get( "REDUCER" ).getChildren();
      for( FlowSliceStats flowSliceStats : children )
        {
        assertNotSame( -1, flowSliceStats.getProcessStartTime() );
        assertNotSame( -1, flowSliceStats.getProcessSubmitTime() );
        assertNotSame( -1, flowSliceStats.getProcessRunTime() );
        assertNotSame( -1, flowSliceStats.getProcessFinishTime() );

        assertTrue( flowSliceStats.getProcessStartTime() <= flowSliceStats.getProcessSubmitTime() );
        assertTrue( flowSliceStats.getProcessSubmitTime() <= flowSliceStats.getProcessRunTime() );
        assertTrue( flowSliceStats.getProcessRunTime() < flowSliceStats.getProcessFinishTime() );

        HadoopSliceStats hadoopSliceStats = (HadoopSliceStats) flowSliceStats;

        if( hadoopSliceStats.getTaskIDNum() == 0 && hadoopSliceStats.getKind() == HadoopSliceStats.Kind.REDUCER )
          assertTrue( hadoopSliceStats.getCounterValue( TestEnum.FIRST ) > 0 ); // in reducer
        }
      }

    HadoopStepStats stats2 = (HadoopStepStats) flowStats2.getFlowStepStats().get( 0 );

    assertNotNull( stats2.getID() );
    assertNotNull( stats2.getProcessStepID() );

    if( getPlatform().isUseCluster() )
      {
      List<FlowNodeStats> flowNodeStats = stats2.getFlowNodeStats();
      assertEquals( 2, flowNodeStats.size() );

      Map<String, FlowNodeStats> statsMap = new HashMap<>();

      statsMap.put( flowNodeStats.get( 0 ).getKind(), flowNodeStats.get( 0 ) );
      statsMap.put( flowNodeStats.get( 1 ).getKind(), flowNodeStats.get( 1 ) );

      assertNotNull( statsMap.get( "MAPPER" ) );
      assertNotNull( statsMap.get( "REDUCER" ) );

      assertEquals( 4, statsMap.get( "MAPPER" ).getChildren().size() );
      assertEquals( 1, statsMap.get( "REDUCER" ).getChildren().size() );
      }
    }
  }
