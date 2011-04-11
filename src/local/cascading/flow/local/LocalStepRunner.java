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

package cascading.flow.local;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import cascading.flow.FlowSession;
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

  private final LocalFlowProcess flowProcess;

  private boolean complete = false;
  private boolean successful = false;

  private final StreamGraph graph;
  private final Collection<Duct> heads;

  public LocalStepRunner( Properties properties, LocalFlowStep step )
    {
    this.flowProcess = new LocalFlowProcess( new FlowSession(), properties );

    graph = new LocalStepStreamGraph( flowProcess, step );

    heads = graph.getHeads();
    }

  public LocalFlowProcess getFlowProcess()
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
      catch( Exception exception )
        {
        LOG.error( "unable to prepare operation graph", exception );

        complete = true;
        successful = false;

        return exception;
        }

      Throwable throwable = null;
      List<Future<Throwable>> futures = spawnHeads();

      for( Future<Throwable> future : futures )
        {
        throwable = future.get();

        if( throwable != null )
          break;
        }

      try
        {
        attemptedCleanup = true;
        graph.cleanup();
        }
      catch( Exception exception )
        {
        LOG.error( "unable to cleanup operation graph", exception );

        if( throwable == null )
          throwable = exception;
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
      catch( Exception exception )
        {
        LOG.error( "unable to cleanup operation graph", exception );
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
