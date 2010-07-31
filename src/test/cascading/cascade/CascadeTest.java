/*
 * Copyright (c) 2007-2010 Concurrent, Inc. All Rights Reserved.
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

import cascading.ClusterTestCase;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowSkipStrategy;
import cascading.flow.FlowStepJob;
import cascading.flow.LockingFlowListener;
import cascading.flow.ProcessFlow;
import cascading.operation.Identity;
import cascading.operation.regex.RegexSplitter;
import cascading.operation.text.FieldJoiner;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.SequenceFile;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.MultiSourceTap;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import riffle.process.scheduler.ProcessChain;

public class CascadeTest extends ClusterTestCase
  {
  String inputFile = "build/test/data/ips.20.txt";
  String outputPath = "build/test/output/cascade/";

  public CascadeTest()
    {
    super( "cascade tests", true );
    }

  private Flow firstFlow( String path )
    {
    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFile );

    Pipe pipe = new Pipe( "first" );

    pipe = new Each( pipe, new Fields( "line" ), new Identity( new Fields( "ip" ) ), new Fields( "ip" ) );

    Tap sink = new Hfs( new SequenceFile( new Fields( "ip" ) ), outputPath + path + "/first", true );

    return new FlowConnector( getProperties() ).connect( source, sink, pipe );
    }

  private Flow secondFlow( Tap source, String path )
    {
    Pipe pipe = new Pipe( "second" );

    pipe = new Each( pipe, new RegexSplitter( new Fields( "first", "second", "third", "fourth" ), "\\." ) );

    Tap sink = new Hfs( new SequenceFile( new Fields( "first", "second", "third", "fourth" ) ), outputPath + path + "/second", true );

    return new FlowConnector( getProperties() ).connect( source, sink, pipe );
    }

  private Flow thirdFlow( Tap source, String path )
    {
    Pipe pipe = new Pipe( "third" );

    pipe = new Each( pipe, new FieldJoiner( new Fields( "mangled" ), "-" ) );

    Tap sink = new Hfs( new SequenceFile( new Fields( "mangled" ) ), outputPath + path + "/third", true );

    return new FlowConnector( getProperties() ).connect( source, sink, pipe );
    }

  private Flow fourthFlow( Tap source, String path )
    {
    Pipe pipe = new Pipe( "fourth" );

    pipe = new Each( pipe, new Identity() );

    Tap sink = new Hfs( new TextLine(), outputPath + path + "/fourth", true );

    return new FlowConnector( getProperties() ).connect( source, sink, pipe );
    }

  private Flow previousMultiTapFlow( String path, String ordinal )
    {
    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFile );

    Pipe pipe = new Pipe( ordinal );

    pipe = new Each( pipe, new Fields( "line" ), new Identity( new Fields( "ip" ) ), new Fields( "ip" ) );

    Tap sink = new Hfs( new SequenceFile( new Fields( "ip" ) ), outputPath + path + "/" + ordinal, true );

    return new FlowConnector( getProperties() ).connect( source, sink, pipe );
    }


  private Flow multiTapFlow( Tap[] sources, String path )
    {
    Pipe pipe = new Pipe( "multitap" );

    pipe = new Each( pipe, new Identity() );

    Tap source = new MultiSourceTap( sources );
    Tap sink = new Hfs( new TextLine(), outputPath + path + "/multitap", true );

    return new FlowConnector( getProperties() ).connect( source, sink, pipe );
    }

  public void testSimpleCascade() throws IOException
    {
    copyFromLocal( inputFile );

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
    copyFromLocal( inputFile );

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
    copyFromLocal( inputFile );

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

    assertFalse( "file exists", fourth.getSink().pathExists( fourth.getJobConf() ) );
    }

  public void testSimpleCascadeStop() throws IOException, InterruptedException
    {
    copyFromLocal( inputFile );

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
      Thread.sleep( 1000 );

      Map<String, Callable<Throwable>> map = LockingFlowListener.getJobsMap( first );

      if( map == null || map.values().size() == 0 )
        continue;

      if( ( (FlowStepJob) map.values().iterator().next() ).wasStarted() )
        break;
      }

    System.out.println( "calling stop" );

    cascade.stop();

    assertTrue( "did not stop", listener.stopped.tryAcquire( 60, TimeUnit.SECONDS ) );
    assertTrue( "did not complete", listener.completed.tryAcquire( 60, TimeUnit.SECONDS ) );
    }

  public void testCascadeID() throws IOException
    {
    String path = "simple";

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

  public void testSimplePerpetual() throws IOException
    {
    copyFromLocal( inputFile );

    String path = "perpetual";

    Flow first = firstFlow( path );
    Flow second = secondFlow( first.getSink(), path );
    Flow third = thirdFlow( second.getSink(), path );
    Flow fourth = fourthFlow( third.getSink(), path );

    ProcessChain chain = new ProcessChain( true, fourth, second, first, third );

    chain.start();

    chain.complete();

    validateLength( fourth, 20 );
    }

  public void testSimplePerpetualCascade() throws IOException
    {
    copyFromLocal( inputFile );

    String path = "perpetualcascade";

    Flow first = firstFlow( path );
    Flow second = secondFlow( first.getSink(), path );
    Flow third = thirdFlow( second.getSink(), path );
    Flow fourth = fourthFlow( third.getSink(), path );

    ProcessFlow firstProcess = new ProcessFlow( "first", first );
    ProcessFlow secondProcess = new ProcessFlow( "second", second );
    ProcessFlow thirdProcess = new ProcessFlow( "third", third );
    ProcessFlow fourthProcess = new ProcessFlow( "fourth", fourth );

    Cascade cascade = new CascadeConnector().connect( fourthProcess, secondProcess, firstProcess, thirdProcess );

    cascade.start();

    cascade.complete();

    validateLength( fourth, 20 );
    }

  }
