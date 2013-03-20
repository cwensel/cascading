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

package cascading.flow.local.planner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import cascading.flow.FlowProcess;
import cascading.flow.local.LocalFlowStep;
import cascading.flow.local.stream.LocalStepStreamGraph;
import cascading.flow.stream.Duct;
import cascading.flow.stream.StreamGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class LocalStepRunner implements Callable<Throwable>
  {
  private static final Logger LOG = LoggerFactory.getLogger( LocalStepRunner.class );

  private final FlowProcess<Properties> flowProcess;

  private boolean complete = false;
  private boolean successful = false;

  private final StreamGraph graph;
  private final Collection<Duct> heads;
  private Throwable throwable = null;

  public LocalStepRunner( FlowProcess<Properties> flowProcess, LocalFlowStep step )
    {
    this.flowProcess = flowProcess;
    this.graph = new LocalStepStreamGraph( this.flowProcess, step );
    this.heads = graph.getHeads();
    }

  public FlowProcess<Properties> getFlowProcess()
    {
    return flowProcess;
    }

  public boolean isComplete()
    {
    return complete;
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
    boolean attemptedCleanup = false;

    try
      {
      try
        {
        graph.prepare();
        }
      catch( Throwable currentThrowable )
        {
        if( !( currentThrowable instanceof OutOfMemoryError ) )
          LOG.error( "unable to prepare operation graph", currentThrowable );

        complete = true;
        successful = false;
        throwable = currentThrowable;

        return throwable;
        }

      try
        {
        List<Future<Throwable>> futures = spawnHeads();

        for( Future<Throwable> future : futures )
          {
          throwable = future.get();

          if( throwable != null )
            break;
          }
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
          graph.cleanup();
        }
      catch( Throwable currentThrowable )
        {
        if( !( currentThrowable instanceof OutOfMemoryError ) )
          LOG.error( "unable to cleanup operation graph", currentThrowable );

        if( throwable == null )
          throwable = currentThrowable;
        }

      complete = true;
      successful = throwable == null;

      return throwable;
      }
    finally
      {
      try
        {
        if( !attemptedCleanup )
          graph.cleanup();
        }
      catch( Throwable currentThrowable )
        {
        if( !( currentThrowable instanceof OutOfMemoryError ) )
          LOG.error( "unable to cleanup operation graph", currentThrowable );

        if( throwable == null )
          throwable = currentThrowable;

        successful = false;
        }
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
