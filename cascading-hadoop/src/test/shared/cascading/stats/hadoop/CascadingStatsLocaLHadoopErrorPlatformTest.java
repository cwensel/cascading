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

package cascading.stats.hadoop;

import cascading.CascadingException;
import cascading.PlatformTestCase;
import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
import cascading.flow.Flow;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.planner.HadoopFlowStepJob;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
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
 * Testing error reporting in hadooop local mode.
 */
public class CascadingStatsLocaLHadoopErrorPlatformTest extends PlatformTestCase
  {
  public CascadingStatsLocaLHadoopErrorPlatformTest()
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

  public class FailAggregation extends BaseOperation implements Aggregator
    {
    public FailAggregation( Fields fieldDeclaration )
      {
      super( 1, fieldDeclaration );
      }

    @Override
    public void start( FlowProcess flowProcess, AggregatorCall aggregatorCall )
      {
      throw new CascadingException( "testing" );
      }

    @Override
    public void aggregate( FlowProcess flowProcess, AggregatorCall aggregatorCall )
      {
      throw new CascadingException( "testing" );
      }

    @Override
    public void complete( FlowProcess flowProcess, AggregatorCall aggregatorCall )
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
    pipe = new Each( pipe, new Fields( "line" ), new FailFunction( new Fields( "ip" )), new Fields( "ip" ) );

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
    pipe = new Every( pipe, new FailAggregation( new Fields( "count" ) ) );

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
