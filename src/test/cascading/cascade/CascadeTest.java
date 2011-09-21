/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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

package cascading.cascade;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import cascading.PlatformTestCase;
import cascading.flow.Flow;
import cascading.flow.FlowSkipStrategy;
import cascading.flow.LockingFlowListener;
import cascading.flow.planner.FlowStepJob;
import cascading.operation.Identity;
import cascading.operation.regex.RegexSplitter;
import cascading.operation.text.FieldJoiner;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tap.MultiSourceTap;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.test.HadoopPlatform;
import cascading.test.PlatformTest;
import cascading.tuple.Fields;

import static data.InputData.inputFileIps;

@PlatformTest(platforms = {"local", "hadoop"})
public class CascadeTest extends PlatformTestCase
  {
  public CascadeTest()
    {
    super( true );
    }

  private Flow firstFlow( String path )
    {
    Tap source = getPlatform().getTextFile( inputFileIps );

    Pipe pipe = new Pipe( "first" );

    pipe = new Each( pipe, new Fields( "line" ), new Identity( new Fields( "ip" ) ), new Fields( "ip" ) );

    Tap sink = getPlatform().getDelimitedFile( new Fields( "ip" ), getOutputPath( path + "/first" ), SinkMode.REPLACE );

    return getPlatform().getFlowConnector().connect( source, sink, pipe );
    }

  private Flow secondFlow( Tap source, String path )
    {
    Pipe pipe = new Pipe( "second" );

    pipe = new Each( pipe, new RegexSplitter( new Fields( "first", "second", "third", "fourth" ), "\\." ) );

    Tap sink = getPlatform().getDelimitedFile( new Fields( "first", "second", "third", "fourth" ), getOutputPath( path + "/second" ), SinkMode.REPLACE );

    return getPlatform().getFlowConnector().connect( source, sink, pipe );
    }

  private Flow thirdFlow( Tap source, String path )
    {
    Pipe pipe = new Pipe( "third" );

    pipe = new Each( pipe, new FieldJoiner( new Fields( "mangled" ), "-" ) );

    Tap sink = getPlatform().getDelimitedFile( new Fields( "mangled" ), getOutputPath( path + "/third" ), SinkMode.REPLACE );

    return getPlatform().getFlowConnector().connect( source, sink, pipe );
    }

  private Flow fourthFlow( Tap source, String path )
    {
    Pipe pipe = new Pipe( "fourth" );

    pipe = new Each( pipe, new Identity() );

    Tap sink = getPlatform().getTextFile( getOutputPath( path + "/fourth" ), SinkMode.REPLACE );

    return getPlatform().getFlowConnector().connect( source, sink, pipe );
    }

  private Flow previousMultiTapFlow( String path, String ordinal )
    {
    Tap source = getPlatform().getTextFile( inputFileIps );

    Pipe pipe = new Pipe( ordinal );

    pipe = new Each( pipe, new Fields( "line" ), new Identity( new Fields( "ip" ) ), new Fields( "ip" ) );

    Tap sink = getPlatform().getDelimitedFile( new Fields( "ip" ), getOutputPath( path + "/" + ordinal ), SinkMode.REPLACE );

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

  public void testSimpleCascade() throws IOException
    {
    getPlatform().copyFromLocal( inputFileIps );

    String path = "simple";

    Flow first = firstFlow( path );
    Flow second = secondFlow( first.getSink(), path );
    Flow third = thirdFlow( second.getSink(), path );
    Flow fourth = fourthFlow( third.getSink(), path );

    Cascade cascade = new CascadeConnector().connect( fourth, second, third, first );

    cascade.start();

    cascade.complete();

    validateLength( fourth, 20 );
    }

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

  public void testSkippedCascade() throws IOException
    {
    getPlatform().copyFromLocal( inputFileIps );

    String path = "skipped";

    Flow first = firstFlow( path );
    Flow second = secondFlow( first.getSink(), path );
    Flow third = thirdFlow( second.getSink(), path );
    Flow fourth = fourthFlow( third.getSink(), path );

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

  public void testSimpleCascadeStop() throws IOException, InterruptedException
    {
    getPlatform().copyFromLocal( inputFileIps );

    String path = "stopped";

    Flow first = firstFlow( path );
    Flow second = secondFlow( first.getSink(), path );
    Flow third = thirdFlow( second.getSink(), path );
    Flow fourth = fourthFlow( third.getSink(), path );

    LockingFlowListener listener = new LockingFlowListener();

    first.addListener( listener );

    Cascade cascade = new CascadeConnector().connect( first, second, third, fourth );

    System.out.println( "calling start" );
    cascade.start();

    assertTrue( "did not start", listener.started.tryAcquire( 60, TimeUnit.SECONDS ) );

    while( true )
      {
      System.out.println( "testing if running" );

      if( getPlatform() instanceof HadoopPlatform )
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

    assertTrue( "did not stop", listener.stopped.tryAcquire( 60, TimeUnit.SECONDS ) );
    assertTrue( "did not complete", listener.completed.tryAcquire( 60, TimeUnit.SECONDS ) );
    }

  public void testCascadeID() throws IOException
    {
    String path = "idtest";

    Flow first = firstFlow( path );
    Flow second = secondFlow( first.getSink(), path );
    Flow third = thirdFlow( second.getSink(), path );
    Flow fourth = fourthFlow( third.getSink(), path );

    Cascade cascade = new CascadeConnector().connect( first, second, third, fourth );

    String id = cascade.getID();

    assertNotNull( "id is null", id );
    assertEquals( first.getProperty( "cascading.cascade.id" ), id );
    assertEquals( second.getProperty( "cascading.cascade.id" ), id );
    assertEquals( third.getProperty( "cascading.cascade.id" ), id );
    assertEquals( fourth.getProperty( "cascading.cascade.id" ), id );
    }
  }
