/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
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

package cascading.cascade;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import cascading.ComparePlatformsTest;
import cascading.PlatformTestCase;
import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.FlowSkipStrategy;
import cascading.flow.LockingFlowListener;
import cascading.flow.planner.FlowStepJob;
import cascading.operation.Identity;
import cascading.operation.regex.RegexSplitter;
import cascading.operation.text.FieldJoiner;
import cascading.pipe.Checkpoint;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tap.MultiSourceTap;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import org.junit.Test;

import static data.InputData.inputFileIps;


public class CascadePlatformTest extends PlatformTestCase
  {
  public CascadePlatformTest()
    {
    super( true );
    }

  private Flow firstFlow( String path )
    {
    Tap source = getPlatform().getTextFile( inputFileIps );

    Pipe pipe = new Pipe( "first" );

    pipe = new Each( pipe, new Fields( "line" ), new Identity( new Fields( "ip" ) ), new Fields( "ip" ) );

    Tap sink = getPlatform().getTabDelimitedFile( new Fields( "ip" ), getOutputPath( path ), SinkMode.REPLACE );

    return getPlatform().getFlowConnector().connect( source, sink, pipe );
    }

  private Flow secondFlow( Tap source, String path )
    {
    Pipe pipe = new Pipe( "second" );

    pipe = new Each( pipe, new RegexSplitter( new Fields( "first", "second", "third", "fourth" ), "\\." ) );

    Tap sink = getPlatform().getTabDelimitedFile( new Fields( "first", "second", "third", "fourth" ), getOutputPath( path ), SinkMode.REPLACE );

    return getPlatform().getFlowConnector().connect( source, sink, pipe );
    }

  private Flow thirdFlow( Tap source, String path )
    {
    Pipe pipe = new Pipe( "third" );

    pipe = new Each( pipe, new FieldJoiner( new Fields( "mangled" ), "-" ) );

    Tap sink = getPlatform().getTabDelimitedFile( new Fields( "mangled" ), getOutputPath( path ), SinkMode.REPLACE );

    return getPlatform().getFlowConnector().connect( source, sink, pipe );
    }

  private Flow thirdCheckpointFlow( Tap source, String path )
    {
    Pipe pipe = new Pipe( "third" );

    pipe = new Each( pipe, new FieldJoiner( new Fields( "mangled" ), "-" ) );

    pipe = new Checkpoint( "checkpoint", pipe );

    pipe = new Each( pipe, new Identity() );

    Tap sink = getPlatform().getTabDelimitedFile( new Fields( "mangled" ), getOutputPath( "unusedpath" ), SinkMode.REPLACE );
    Tap checkpoint = getPlatform().getTabDelimitedFile( Fields.ALL, getOutputPath( path ), SinkMode.REPLACE );

    FlowDef flowDef = FlowDef.flowDef()
      .addSource( pipe, source )
      .addTailSink( pipe, sink )
      .addCheckpoint( "checkpoint", checkpoint );

    return getPlatform().getFlowConnector().connect( flowDef );
    }

  private Flow fourthFlow( Tap source, String path )
    {
    Pipe pipe = new Pipe( "fourth" );

    pipe = new Each( pipe, new Identity() );

    Tap sink = getPlatform().getTextFile( getOutputPath( path ), SinkMode.REPLACE );

    return getPlatform().getFlowConnector().connect( source, sink, pipe );
    }

  private Flow previousMultiTapFlow( String path, String ordinal )
    {
    Tap source = getPlatform().getTextFile( inputFileIps );

    Pipe pipe = new Pipe( ordinal );

    pipe = new Each( pipe, new Fields( "line" ), new Identity( new Fields( "ip" ) ), new Fields( "ip" ) );

    Tap sink = getPlatform().getTabDelimitedFile( new Fields( "ip" ), getOutputPath( path + "/" + ordinal ), SinkMode.REPLACE );

    return getPlatform().getFlowConnector().connect( source, sink, pipe );
    }

  private Flow multiTapFlow( Tap[] sources, String path )
    {
    Pipe pipe = new Pipe( "multitap" );

    pipe = new Each( pipe, new Identity() );

    Tap source = new MultiSourceTap( sources );
    Tap sink = getPlatform().getTextFile( getOutputPath( path + "/multitap" ), SinkMode.REPLACE );

    return getPlatform().getFlowConnector().connect( source, sink, pipe );
    }

  @Test
  public void testSimpleCascade() throws IOException
    {
    getPlatform().copyFromLocal( inputFileIps );

    String path = "simple";

    Flow first = firstFlow( path + "/first" );
    Flow second = secondFlow( first.getSink(), path + "/second" );
    Flow third = thirdFlow( second.getSink(), path + "/third" );
    Flow fourth = fourthFlow( third.getSink(), path + "/fourth" );

    Cascade cascade = new CascadeConnector().connect( fourth, second, third, first );

    cascade.start();

    cascade.complete();

    validateLength( fourth, 20 );

    assertTrue( cascade.getHeadFlows().contains( first ) );
    assertTrue( cascade.getSourceTaps().containsAll( first.getSourcesCollection() ) );

    assertTrue( cascade.getTailFlows().contains( fourth ) );
    assertTrue( cascade.getSinkTaps().containsAll( fourth.getSinksCollection() ) );
    }

  @Test
  public void testMultiTapCascade() throws IOException
    {
    getPlatform().copyFromLocal( inputFileIps );

    String path = "multitap";

    Flow first = previousMultiTapFlow( path, "first" );
    Flow second = previousMultiTapFlow( path, "second" );
    Flow multitap = multiTapFlow( Tap.taps( first.getSink(), second.getSink() ), path );

    Cascade cascade = new CascadeConnector().connect( multitap, first, second );

    cascade.start();

    cascade.complete();

    validateLength( multitap, 40 );
    }

  @Test
  public void testSkippedCascade() throws IOException
    {
    getPlatform().copyFromLocal( inputFileIps );

    String path = "skipped";

    Flow first = firstFlow( path + "/first" );
    Flow second = secondFlow( first.getSink(), path + "/second" );
    Flow third = thirdFlow( second.getSink(), path + "/third" );
    Flow fourth = fourthFlow( third.getSink(), path + "/fourth" );

    Cascade cascade = new CascadeConnector().connect( first, second, third, fourth );

    cascade.setFlowSkipStrategy( new FlowSkipStrategy()
    {

    public boolean skipFlow( Flow flow ) throws IOException
      {
      return true;
      }
    } );

    cascade.start();

    cascade.complete();

    assertFalse( "file exists", fourth.getSink().resourceExists( fourth.getConfig() ) );
    }

  @Test
  public void testSimpleCascadeStop() throws IOException, InterruptedException
    {
    getPlatform().copyFromLocal( inputFileIps );

    String path = "stopped";

    // remove from comparison tests
    Flow first = firstFlow( path + "/first" + ComparePlatformsTest.NONDETERMINISTIC );
    Flow second = secondFlow( first.getSink(), path + "/second" + ComparePlatformsTest.NONDETERMINISTIC );
    Flow third = thirdFlow( second.getSink(), path + "/third" + ComparePlatformsTest.NONDETERMINISTIC );
    Flow fourth = fourthFlow( third.getSink(), path + "/fourth" + ComparePlatformsTest.NONDETERMINISTIC );

    LockingCascadeListener cascadeListener = new LockingCascadeListener();
    LockingFlowListener flowListener = new LockingFlowListener();

    first.addListener( flowListener );

    Cascade cascade = new CascadeConnector().connect( first, second, third, fourth );

    cascade.addListener( cascadeListener );

    System.out.println( "calling start" );
    cascade.start();

    assertTrue( "did not start", flowListener.started.tryAcquire( 60, TimeUnit.SECONDS ) );

    assertTrue( "cascade did not start", cascadeListener.started.tryAcquire( 60, TimeUnit.SECONDS ) );

    while( true )
      {
      System.out.println( "testing if running" );

      if( getPlatform().isMapReduce() )
        Thread.sleep( 1000 );

      Map<String, Callable<Throwable>> map = LockingFlowListener.getJobsMap( first );

      if( map == null || map.values().size() == 0 )
        continue;

      FlowStepJob flowStepJob = (FlowStepJob) map.values().iterator().next();

      if( flowStepJob.isStarted() )
        break;
      }

    System.out.println( "calling stop" );

    cascade.stop();

    assertTrue( "did not stop", flowListener.stopped.tryAcquire( 60, TimeUnit.SECONDS ) );
    assertTrue( "did not complete", flowListener.completed.tryAcquire( 60, TimeUnit.SECONDS ) );

    assertTrue( "cascade did not stop", cascadeListener.stopped.tryAcquire( 60, TimeUnit.SECONDS ) );
    assertTrue( "cascade did not complete", cascadeListener.completed.tryAcquire( 60, TimeUnit.SECONDS ) );
    }

  @Test
  public void testCascadeID() throws IOException
    {
    String path = "idtest";

    Flow first = firstFlow( path + "/first" );
    Flow second = secondFlow( first.getSink(), path + "/second" );
    Flow third = thirdFlow( second.getSink(), path + "/third" );
    Flow fourth = fourthFlow( third.getSink(), path + "/fourth" );

    Cascade cascade = new CascadeConnector().connect( first, second, third, fourth );

    String id = cascade.getID();

    assertNotNull( "id is null", id );
    assertEquals( first.getProperty( "cascading.cascade.id" ), id );
    assertEquals( second.getProperty( "cascading.cascade.id" ), id );
    assertEquals( third.getProperty( "cascading.cascade.id" ), id );
    assertEquals( fourth.getProperty( "cascading.cascade.id" ), id );
    }

  @Test
  public void testCheckpointTapCascade() throws IOException
    {
    if( !getPlatform().isMapReduce() )
      return;

    getPlatform().copyFromLocal( inputFileIps );

    String path = "checkpoint";

    Flow first = firstFlow( path + "/first" );
    Flow second = secondFlow( first.getSink(), path + "/second" );
    Flow third = thirdCheckpointFlow( second.getSink(), path + "/third" );
    Flow fourth = fourthFlow( (Tap) third.getCheckpoints().values().iterator().next(), path + "/fourth" );

    Cascade cascade = new CascadeConnector().connect( fourth, second, third, first );

    cascade.start();

    cascade.complete();

    validateLength( fourth, 20 );

    assertTrue( cascade.getHeadFlows().contains( first ) );
    assertTrue( cascade.getSourceTaps().containsAll( first.getSourcesCollection() ) );

    assertTrue( cascade.getIntermediateTaps().containsAll( third.getCheckpointsCollection() ) );
    assertTrue( cascade.getCheckpointsTaps().containsAll( third.getCheckpointsCollection() ) );

    assertTrue( cascade.getTailFlows().contains( fourth ) );
    assertTrue( cascade.getSinkTaps().containsAll( fourth.getSinksCollection() ) );
    }
  }
