/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
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

import cascading.CascadingException;
import cascading.PlatformTestCase;
import cascading.TestFailAggregator;
import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
import cascading.flow.Flow;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.planner.HadoopFlowStepJob;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.regex.RegexParser;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.stats.CascadeStats;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static data.InputData.inputFileApache;

/**
 * Testing error reporting in hadoop local mode.
 */
public class CascadingStatsLocalHadoopErrorPlatformTest extends PlatformTestCase
  {
  public CascadingStatsLocalHadoopErrorPlatformTest()
    {
    super( false );
    }

  @Before
  public void setUp()
    {
    HadoopFlowStepJob.reportLocalError( null );
    }

  @After
  public void tearDown()
    {
    HadoopFlowStepJob.reportLocalError( null );
    }

  public class FailFunction extends BaseOperation implements Function
    {
    public FailFunction( Fields fieldDeclaration )
      {
      super( 1, fieldDeclaration );
      }

    @Override
    public void operate( FlowProcess flowProcess, FunctionCall functionCall )
      {
      throw new CascadingException( "testing" );
      }
    }

  @Test
  public void testLocalErrorReportingInMapper() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( inputFileApache );

    Pipe pipe = new Pipe( "failing mapper" );
    pipe = new Each( pipe, new Fields( "line" ), new FailFunction( new Fields( "ip" ) ), new Fields( "ip" ) );

    Tap sink = getPlatform().getTextFile( getOutputPath( "mapperfail" ), SinkMode.REPLACE );

    Flow flow = getPlatform().getFlowConnector().connect( "mapper fail test", source, sink, pipe );

    Cascade cascade = new CascadeConnector().connect( flow );
    assertNull( cascade.getCascadeStats().getThrowable() );

    try
      {
      cascade.complete();
      fail( "An exception should have been thrown" );
      }
    catch( Throwable throwable )
      {
      CascadeStats cascadeStats = cascade.getCascadeStats();
      assertEquals( throwable, cascadeStats.getThrowable() );
      }
    }

  @Test
  public void testLocalErrorReportingInReducer() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( inputFileApache );

    Pipe pipe = new Pipe( "failing reducer" );
    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );
    pipe = new GroupBy( pipe, new Fields( "ip" ) );
    pipe = new Every( pipe, new TestFailAggregator( new Fields( "count" ), 1 ) );

    Tap sink = getPlatform().getTextFile( getOutputPath( "reducerfail" ), SinkMode.REPLACE );

    Flow flow = getPlatform().getFlowConnector().connect( "reducer fail test", source, sink, pipe );

    Cascade cascade = new CascadeConnector().connect( flow );
    assertNull( cascade.getCascadeStats().getThrowable() );
    try
      {
      cascade.complete();
      fail( "An exception should have been thrown" );
      }
    catch( Throwable throwable )
      {
      CascadeStats cascadeStats = cascade.getCascadeStats();
      assertEquals( throwable, cascadeStats.getThrowable() );
      }
    }
  }
