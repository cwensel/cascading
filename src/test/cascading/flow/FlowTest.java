/*
 * Copyright (c) 2007-2008 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import cascading.ClusterTestCase;
import cascading.operation.Function;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/**
 *
 */
public class FlowTest extends ClusterTestCase
  {
  String inputFileUpper = "build/test/data/upper.txt";
  String inputFileLower = "build/test/data/lower.txt";

  String outputPath = "build/test/output/flow/";

  public FlowTest()
    {
    super( "flow test", false );
    }

  public void testStop() throws Exception
    {
    if( !new File( inputFileLower ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileLower );
    copyFromLocal( inputFileUpper );

    Tap sourceLower = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileLower );
    Tap sourceUpper = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileUpper );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    // using null pos so all fields are written
    Tap sink = new Hfs( new TextLine(), outputPath + "/stopped/", true );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );

    pipeLower = new GroupBy( pipeLower, new Fields( "num" ) );

    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );

    pipeUpper = new GroupBy( pipeUpper, new Fields( "num" ) );

    Pipe splice = new CoGroup( pipeLower, new Fields( "num" ), pipeUpper, new Fields( "num" ), Fields.size( 4 ) );

    Flow flow = new FlowConnector( jobConf ).connect( sources, sink, splice );

//    countFlow.writeDOT( "stopped.dot" );

    LockingFlowListener listener = new LockingFlowListener();

    flow.addListener( listener );

    System.out.println( "calling start" );
    flow.start();

    assertTrue( "did not start", listener.started.tryAcquire( 60, TimeUnit.SECONDS ) );

    while( true )
      {
      System.out.println( "testing if running" );
      Thread.sleep( 1000 );

      Map<String, Callable<Throwable>> map = flow.getJobsMap();

      if( map == null || map.values().size() == 0 )
        continue;

      if( ( (FlowStep.FlowStepJob) map.values().iterator().next() ).wasStarted() )
        break;
      }

    System.out.println( "calling stop" );

    flow.stop();

    assertTrue( "did not stop", listener.stopped.tryAcquire( 60, TimeUnit.SECONDS ) );
    assertTrue( "did not complete", listener.completed.tryAcquire( 60, TimeUnit.SECONDS ) );
    }

  class LockingFlowListener implements FlowListener
    {
    Semaphore started = new Semaphore( 0 );
    Semaphore stopped = new Semaphore( 0 );
    Semaphore completed = new Semaphore( 0 );
    Semaphore thrown = new Semaphore( 0 );

    LockingFlowListener()
      {
      }

    public void onStarting( Flow flow )
      {
      started.release();
      }

    public void onStopping( Flow flow )
      {
      stopped.release();
      }

    public void onCompleted( Flow flow )
      {
      completed.release();
      }

    public boolean onThrowable( Flow flow, Throwable throwable )
      {
      thrown.release();
      return false;
      }
    }


  }
