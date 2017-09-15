/*
 * Copyright (c) 2016-2017 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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

package cascading.flow.local.planner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;

import cascading.flow.FlowNode;
import cascading.flow.FlowProcess;
import cascading.flow.local.LocalFlowStep;
import cascading.flow.local.stream.graph.LocalStepStreamGraph;
import cascading.flow.stream.duct.Duct;
import cascading.flow.stream.graph.StreamGraph;
import cascading.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cascading.util.LogUtil.logCounters;
import static cascading.util.LogUtil.logMemory;

/**
 *
 */
public class LocalStepRunner implements Callable<Throwable>
  {
  private static final Logger LOG = LoggerFactory.getLogger( LocalStepRunner.class );

  private final FlowProcess<Properties> currentProcess;

  private volatile boolean started = false;
  private volatile boolean completed = false;
  private volatile boolean successful = false;
  private volatile boolean stopped = false;

  private final FlowNode flowNode;
  private final StreamGraph streamGraph;
  private final Collection<Duct> heads;
  private Throwable throwable = null;

  private Semaphore markComplete = new Semaphore( 0 );
  private List<Future<Throwable>> futures = Collections.emptyList();

  public LocalStepRunner( FlowProcess<Properties> flowProcess, LocalFlowStep step )
    {
    this.currentProcess = flowProcess;
    this.flowNode = Util.getFirst( step.getFlowNodeGraph().vertexSet() );
    this.streamGraph = new LocalStepStreamGraph( this.currentProcess, step, flowNode );
    this.heads = streamGraph.getHeads();
    }

  public FlowProcess<Properties> getFlowProcess()
    {
    return currentProcess;
    }

  public boolean isCompleted()
    {
    return completed;
    }

  public void blockUntilStopped()
    {
    if( !started || completed )
      return;

    stopped = true;

    for( Future<Throwable> future : futures )
      future.cancel( true );

    try
      {
      markComplete.acquire();
      }
    catch( InterruptedException exception )
      {
      // do nothing
      }
    }

  public boolean isStopped()
    {
    return stopped;
    }

  public boolean isSuccessful()
    {
    return successful;
    }

  public Throwable getThrowable()
    {
    return throwable;
    }

  @Override
  public Throwable call() throws Exception
    {
    started = true;
    boolean attemptedCleanup = false;

    try
      {
      try
        {
        streamGraph.prepare();

        logMemory( LOG, "flow node id: " + flowNode.getID() + ", mem on start" );
        }
      catch( Throwable currentThrowable )
        {
        try
          {
          if( !( currentThrowable instanceof OutOfMemoryError ) )
            LOG.error( "unable to prepare operation graph", currentThrowable );

          completed = true;
          successful = false;
          throwable = currentThrowable;

          return throwable;
          }
        finally
          {
          markComplete.release();
          }
        }

      if( stopped )
        {
        markComplete.release();

        return null;
        }

      try
        {
        futures = spawnHeads();

        for( Future<Throwable> future : futures )
          {
          try
            {
            throwable = future.get();
            }
          catch( CancellationException exception )
            {
            break;
            }

          if( throwable != null )
            break;
          }

        futures = Collections.emptyList();
        }
      catch( Throwable currentThrowable )
        {
        if( !( currentThrowable instanceof OutOfMemoryError ) )
          LOG.error( "unable to complete step", currentThrowable );

        throwable = currentThrowable;
        }

      try
        {
        attemptedCleanup = true; // set so we don't try again regardless

        if( !( throwable instanceof OutOfMemoryError ) )
          streamGraph.cleanup();
        }
      catch( Throwable currentThrowable )
        {
        if( !( currentThrowable instanceof OutOfMemoryError ) )
          LOG.error( "unable to cleanup operation graph", currentThrowable );

        if( throwable == null )
          throwable = currentThrowable;
        }

      completed = true;
      successful = throwable == null;

      return throwable;
      }
    finally
      {
      try
        {
        if( !attemptedCleanup )
          streamGraph.cleanup();
        }
      catch( Throwable currentThrowable )
        {
        if( !( currentThrowable instanceof OutOfMemoryError ) )
          LOG.error( "unable to cleanup operation graph", currentThrowable );

        if( throwable == null )
          throwable = currentThrowable;

        successful = false;
        }
      finally
        {
        markComplete.release();
        }

      String message = "flow node id: " + flowNode.getID();
      logMemory( LOG, message + ", mem on close" );
      logCounters( LOG, message + ", counter:", currentProcess );
      }
    }

  private List<Future<Throwable>> spawnHeads()
    {
    // todo: consider a CyclicBarrier to syn all threads after the openForRead
    // todo: should find all Callable Ducts and spawn them, group ducts may run on a timer etc
    ExecutorService executors = Executors.newFixedThreadPool( heads.size() );
    List<Future<Throwable>> futures = new ArrayList<Future<Throwable>>();

    for( Duct head : heads )
      futures.add( executors.submit( (Callable) head ) );

    executors.shutdown();

    return futures;
    }
  }
