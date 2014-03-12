/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow.hadoop;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import cascading.PlatformTestCase;
import cascading.flow.FailingFlowListener;
import cascading.flow.Flow;
import cascading.flow.FlowProcess;
import cascading.flow.FlowStep;
import cascading.flow.Flows;
import cascading.flow.LockingFlowListener;
import cascading.flow.hadoop.planner.HadoopFlowStepJob;
import cascading.flow.hadoop.planner.HadoopPlanner;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.flow.planner.FlowStepJob;
import cascading.operation.BaseOperation;
import cascading.operation.Debug;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.operation.Function;
import cascading.operation.expression.ExpressionFunction;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.platform.hadoop.BaseHadoopPlatform;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.Fields;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static data.InputData.inputFileLower;
import static data.InputData.inputFileUpper;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

/**
 *
 */
public class FlowPlatformTest extends PlatformTestCase
  {
  private static final Logger LOG = LoggerFactory.getLogger( FlowPlatformTest.class );

  public FlowPlatformTest()
    {
    super( true ); // must be run in cluster mode
    }

  // test is not executed, just guarantees flow is run locally

  @Test
  public void testLocalModeSource() throws Exception
    {
    Tap source = new Lfs( new TextLine(), "input/path" );
    Tap sink = new Hfs( new TextLine(), "output/path", true );

    Pipe pipe = new Pipe( "test" );

    Map<Object, Object> props = getProperties();
    Flow flow = getPlatform().getFlowConnector( props ).connect( source, sink, pipe );

    List<FlowStep> steps = flow.getFlowSteps();

    assertEquals( "wrong size", 1, steps.size() );

    FlowStep step = steps.get( 0 );

    boolean isLocal = HadoopUtil.isLocal( ( (HadoopFlowStep) step ).getInitializedConfig( flow.getFlowProcess(), HadoopPlanner.createJobConf( props ) ) );

    assertTrue( "is not local", isLocal );
    }

  // test is not executed, just guarantees flow is run locally

  @Test
  public void testLocalModeSink() throws Exception
    {
    Tap source = new Hfs( new TextLine(), "input/path" );
    Tap sink = new Lfs( new TextLine(), "output/path", true );

    Pipe pipe = new Pipe( "test" );

    Map<Object, Object> props = getProperties();
    Flow flow = getPlatform().getFlowConnector( props ).connect( source, sink, pipe );

    List<FlowStep> steps = flow.getFlowSteps();

    assertEquals( "wrong size", 1, steps.size() );

    FlowStep step = steps.get( 0 );

    boolean isLocal = HadoopUtil.isLocal( ( (HadoopFlowStep) step ).getInitializedConfig( flow.getFlowProcess(), HadoopPlanner.createJobConf( props ) ) );

    assertTrue( "is not local", isLocal );
    }

  // test is not executed, just guarantees flow is run on cluster
  @Test
  public void testNotLocalMode() throws Exception
    {
    if( !getPlatform().isUseCluster() )
      return;

    Tap source = new Hfs( new TextLine(), "input/path" );
    Tap sink = new Hfs( new TextLine(), "output/path", true );

    Pipe pipe = new Pipe( "test" );

    Map<Object, Object> props = getProperties();
    Flow flow = getPlatform().getFlowConnector( props ).connect( source, sink, pipe );

    List<FlowStep> steps = flow.getFlowSteps();

    assertEquals( "wrong size", 1, steps.size() );

    FlowStep step = steps.get( 0 );

    boolean isLocal = HadoopUtil.isLocal( ( (HadoopFlowStep) step ).getInitializedConfig( flow.getFlowProcess(), HadoopPlanner.createJobConf( props ) ) );

    assertTrue( "is local", !isLocal );
    }

  @Test
  public void testStop() throws Exception
    {
    // introduces race condition if run without cluster
    if( !getPlatform().isUseCluster() )
      return;

    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    Tap sourceLower = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileLower );
    Tap sourceUpper = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileUpper );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    // using null pos so all fields are written
    Tap sink = new Hfs( new TextLine(), getOutputPath( "stopped" ), true );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );

    pipeLower = new GroupBy( pipeLower, new Fields( "num" ) );

    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );

    pipeUpper = new Each( pipeUpper, new Fields( "num", "char" ), new ExpressionFunction( Fields.ALL, "Thread.sleep(1000)" ) );

    pipeUpper = new GroupBy( pipeUpper, new Fields( "num" ) );

    Pipe splice = new CoGroup( pipeLower, new Fields( "num" ), pipeUpper, new Fields( "num" ), Fields.size( 4 ) );

    final Flow flow = getPlatform().getFlowConnector( getProperties() ).connect( sources, sink, splice );

//    countFlow.writeDOT( "stopped.dot" );

    final LockingFlowListener listener = new LockingFlowListener();

    flow.addListener( listener );

    LOG.info( "calling start" );
    flow.start();

    assertTrue( "did not start", listener.started.tryAcquire( 60, TimeUnit.SECONDS ) );

    while( true )
      {
      LOG.info( "testing if running" );
      Thread.sleep( 100 );

      Map<String, FlowStepJob> map = Flows.getJobsMap( flow );

      if( map == null || map.values().size() == 0 )
        continue;

      if( ( (HadoopFlowStepJob) map.values().iterator().next() ).isStarted() )
        break;
      }

    final Semaphore start = new Semaphore( 0 );
    final long startTime = System.nanoTime();

    Future<Long> future = newSingleThreadExecutor().submit( new Callable<Long>()
    {
    @Override
    public Long call() throws Exception
      {
      start.release();
      LOG.info( "calling complete" );
      flow.complete();
      return System.nanoTime() - startTime;
      }
    } );

    start.acquire();
    LOG.info( "calling stop" );
    flow.stop();

    long stopTime = System.nanoTime() - startTime;
    long completeTime = future.get();

    assertTrue( String.format( "stop: %s complete: %s", stopTime, completeTime ), stopTime <= completeTime );

    assertTrue( "did not stop", listener.stopped.tryAcquire( 60, TimeUnit.SECONDS ) );
    assertTrue( "did not complete", listener.completed.tryAcquire( 60, TimeUnit.SECONDS ) );
    }

  private static class BadFilter extends BaseOperation implements Filter
    {
    private Object object = new Object(); // intentional

    public boolean isRemove( FlowProcess flowProcess, FilterCall filterCall )
      {
      return false;
      }
    }

  @Test
  public void testFailedSerialization() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );

    Tap sourceLower = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileLower );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    // using null pos so all fields are written
    Tap sink = new Hfs( new TextLine(), getOutputPath( "badserialization" ), true );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );

    pipeLower = new Each( pipeLower, new Fields( "num" ), new BadFilter() );

    pipeLower = new GroupBy( pipeLower, new Fields( "num" ) );

    try
      {
      // assembly serialization now happens during Flow construction, no chance to use a listener to catch
      Flow flow = getPlatform().getFlowConnector( getProperties() ).connect( sources, sink, pipeLower );
      fail( "did not throw serialization exception" );
      }
    catch( Exception exception )
      {
      // ignore
      }
    }

  @Test
  public void testStartStopRace() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );

    Tap sourceLower = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileLower );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    // using null pos so all fields are written
    Tap sink = new Hfs( new TextLine(), getOutputPath( "startstop" ), SinkMode.REPLACE );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );

    pipeLower = new GroupBy( pipeLower, new Fields( "num" ) );

    Flow flow = getPlatform().getFlowConnector( getProperties() ).connect( sources, sink, pipeLower );

    flow.start();
    flow.stop(); // should not fail
    }

  @Test
  public void testFailingListenerStarting() throws Exception
    {
    failingListenerTest( FailingFlowListener.OnFail.STARTING );
    }

  @Test
  public void testFailingListenerStopping() throws Exception
    {
    failingListenerTest( FailingFlowListener.OnFail.STOPPING );
    }

  @Test
  public void testFailingListenerCompleted() throws Exception
    {
    failingListenerTest( FailingFlowListener.OnFail.COMPLETED );
    }

  @Test
  public void testFailingListenerThrowable() throws Exception
    {
    failingListenerTest( FailingFlowListener.OnFail.THROWABLE );
    }

  private void failingListenerTest( FailingFlowListener.OnFail onFail ) throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    Tap sourceLower = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileLower );
    Tap sourceUpper = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileUpper );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    // using null pos so all fields are written
    Tap sink = new Hfs( new TextLine(), getOutputPath( onFail + "/stopped" ), true );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );

    if( onFail == FailingFlowListener.OnFail.THROWABLE )
      {
      pipeLower = new Each( pipeLower, new Debug()
      {
      @Override
      public boolean isRemove( FlowProcess flowProcess, FilterCall filterCall )
        {
        throw new RuntimeException( "failing inside pipe assembly intentionally" );
        }
      } );
      }

    pipeLower = new GroupBy( pipeLower, new Fields( "num" ) );

    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );

    pipeUpper = new GroupBy( pipeUpper, new Fields( "num" ) );

    Pipe splice = new CoGroup( pipeLower, new Fields( "num" ), pipeUpper, new Fields( "num" ), Fields.size( 4 ) );

    Flow flow = getPlatform().getFlowConnector( getProperties() ).connect( sources, sink, splice );

//    countFlow.writeDOT( "stopped.dot" );

    FailingFlowListener listener = new FailingFlowListener( onFail );

    flow.addListener( listener );

    LOG.info( "calling start" );
    flow.start();

    assertTrue( "did not start", listener.started.tryAcquire( 120, TimeUnit.SECONDS ) );

    if( onFail == FailingFlowListener.OnFail.STOPPING )
      {
      while( true )
        {
        LOG.info( "testing if running" );
        Thread.sleep( 1000 );

        Map<String, FlowStepJob> map = Flows.getJobsMap( flow );

        if( map == null || map.values().size() == 0 )
          continue;

        if( ( (HadoopFlowStepJob) map.values().iterator().next() ).isStarted() )
          break;
        }

      LOG.info( "calling stop" );

      flow.stop();
      }

    assertTrue( "did not complete", listener.completed.tryAcquire( 120, TimeUnit.SECONDS ) );
    assertTrue( "did not stop", listener.stopped.tryAcquire( 120, TimeUnit.SECONDS ) );

    try
      {
      flow.complete();
      fail( "did not rethrow exception from listener" );
      }
    catch( Exception exception )
      {
      // ignore
      }
    }

  @Test
  public void testFlowID() throws Exception
    {
    Tap source = new Lfs( new TextLine(), "input/path" );
    Tap sink = new Hfs( new TextLine(), "output/path", true );

    Pipe pipe = new Pipe( "test" );

    Map<Object, Object> props = getProperties();
    Flow flow1 = getPlatform().getFlowConnector( props ).connect( source, sink, pipe );

//    System.out.println( "flow.getID() = " + flow1.getID() );

    assertNotNull( "missing id", flow1.getID() );
    assertNotNull( "missing id in conf", ( (HadoopFlow) flow1 ).getConfig().get( "cascading.flow.id" ) );

    Flow flow2 = getPlatform().getFlowConnector( props ).connect( source, sink, pipe );

    assertTrue( "same id", !flow1.getID().equalsIgnoreCase( flow2.getID() ) );
    }

  @Test
  public void testCopyConfig() throws Exception
    {
    Tap source = new Lfs( new TextLine(), "input/path" );
    Tap sink = new Hfs( new TextLine(), "output/path", true );

    Pipe pipe = new Pipe( "test" );

    JobConf conf = ( (BaseHadoopPlatform) getPlatform() ).getJobConf();

    conf.set( AppProps.APP_NAME, "testname" );

    AppProps props = AppProps.appProps().setVersion( "1.2.3" );

    Properties properties = props.buildProperties( conf ); // convert job conf to properties instance

    Flow flow = getPlatform().getFlowConnector( properties ).connect( source, sink, pipe );

    assertEquals( "testname", flow.getProperty( AppProps.APP_NAME ) );
    assertEquals( "1.2.3", flow.getProperty( AppProps.APP_VERSION ) );
    }
  }
