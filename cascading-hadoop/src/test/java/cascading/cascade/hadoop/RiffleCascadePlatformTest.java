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

package cascading.cascade.hadoop;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import cascading.PlatformTestCase;
import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
import cascading.flow.Flow;
import cascading.flow.LockingFlowListener;
import cascading.flow.hadoop.ProcessFlow;
import cascading.operation.Identity;
import cascading.operation.regex.RegexSplitter;
import cascading.operation.text.FieldJoiner;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import org.junit.Test;
import riffle.process.scheduler.ProcessChain;

import static data.InputData.inputFileIps;


public class RiffleCascadePlatformTest extends PlatformTestCase
  {
  public RiffleCascadePlatformTest()
    {
    super( true );
    }

  private Flow firstFlow( String path )
    {
    Tap source = getPlatform().getTextFile( inputFileIps );

    Pipe pipe = new Pipe( "first" );

    pipe = new Each( pipe, new Fields( "line" ), new Identity( new Fields( "ip" ) ), new Fields( "ip" ) );

    Tap sink = getPlatform().getTabDelimitedFile( new Fields( "ip" ), getOutputPath( path + "/first" ), SinkMode.REPLACE );

    return getPlatform().getFlowConnector().connect( source, sink, pipe );
    }

  private Flow secondFlow( Tap source, String path )
    {
    Pipe pipe = new Pipe( "second" );

    pipe = new Each( pipe, new RegexSplitter( new Fields( "first", "second", "third", "fourth" ), "\\." ) );

    Tap sink = getPlatform().getTabDelimitedFile( new Fields( "first", "second", "third", "fourth" ), getOutputPath( path + "/second" ), SinkMode.REPLACE );

    return getPlatform().getFlowConnector().connect( source, sink, pipe );
    }

  private Flow thirdFlow( Tap source, String path )
    {
    Pipe pipe = new Pipe( "third" );

    pipe = new Each( pipe, new FieldJoiner( new Fields( "mangled" ), "-" ) );

    Tap sink = getPlatform().getTabDelimitedFile( new Fields( "mangled" ), getOutputPath( path + "/third" ), SinkMode.REPLACE );

    return getPlatform().getFlowConnector().connect( source, sink, pipe );
    }

  private Flow fourthFlow( Tap source, String path )
    {
    Pipe pipe = new Pipe( "fourth" );

    pipe = new Each( pipe, new Identity() );

    Tap sink = getPlatform().getTextFile( getOutputPath( path + "/fourth" ), SinkMode.REPLACE );

    return getPlatform().getFlowConnector().connect( source, sink, pipe );
    }

  @Test
  public void testSimpleRiffle() throws IOException
    {
    getPlatform().copyFromLocal( inputFileIps );

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

  @Test
  public void testSimpleRiffleCascade() throws IOException, InterruptedException
    {
    getPlatform().copyFromLocal( inputFileIps );

    String path = "perpetualcascade";

    Flow first = firstFlow( path );
    Flow second = secondFlow( first.getSink(), path );
    Flow third = thirdFlow( second.getSink(), path );
    Flow fourth = fourthFlow( third.getSink(), path );

    ProcessFlow firstProcess = new ProcessFlow( "first", first );
    ProcessFlow secondProcess = new ProcessFlow( "second", second );
    ProcessFlow thirdProcess = new ProcessFlow( "third", third );
    ProcessFlow fourthProcess = new ProcessFlow( "fourth", fourth );

    LockingFlowListener flowListener = new LockingFlowListener();
    secondProcess.addListener( flowListener );

    Cascade cascade = new CascadeConnector().connect( fourthProcess, secondProcess, firstProcess, thirdProcess );

    cascade.start();

    cascade.complete();

    assertTrue( "did not start", flowListener.started.tryAcquire( 2, TimeUnit.SECONDS ) );
    assertTrue( "did not complete", flowListener.completed.tryAcquire( 2, TimeUnit.SECONDS ) );

    validateLength( fourth, 20 );
    }
  }
