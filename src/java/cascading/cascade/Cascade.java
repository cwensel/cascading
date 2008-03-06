/*
 * Copyright (c) 2007-2008 Vinculum Technologies, Inc. All Rights Reserved.
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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import cascading.CascadingException;
import cascading.flow.Flow;
import cascading.flow.FlowException;
import cascading.tap.Tap;
import org.apache.log4j.Logger;
import org.jgrapht.Graphs;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.jgrapht.traverse.TopologicalOrderIterator;

/**
 * A Cascade is an assembly of {@link Flow} instances that share or depend the same {@link Tap} instances and are executed as
 * a single group. The most common case is where one Flow instance depends on a Tap created by a second Flow instance. This
 * dependency chain can continue as practical.
 * <p/>
 * Additionally, a Cascade allows for incremental builds of complex data processing processes. If a given source {@link Tap} is newer than
 * a subsequent sink {@link Tap} in the assembly, the connecting {@link Flow}(s) will be executed
 * when the Cascade executed. If all the targets (sinks) are up to date, the Cascade exits immediately and does nothing.
 */
public class Cascade implements Runnable
  {
  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( Cascade.class );

  /** Field name */
  private String name;
  /** Field jobGraph */
  private final SimpleDirectedGraph<Flow, Integer> jobGraph;
  /** Field thread */
  private Thread thread;
  /** Field throwable */
  private Throwable throwable;

  public Cascade( String name, SimpleDirectedGraph<Flow, Integer> jobGraph )
    {
    this.name = name;
    this.jobGraph = jobGraph;
    }

  /**
   * Method getName returns the name of this Cascade object.
   *
   * @return the name (type String) of this Cascade object.
   */
  public String getName()
    {
    return name;
    }

  /**
   * Method start begins the current Cascade process. It returns immediately. See method {@link #complete()} to block
   * until the Cascade completes.
   */
  public void start()
    {
    if( thread != null )
      return;

    // todo: thread pooling might be smart
    thread = new Thread( this, "flow" );

    thread.start();
    }

  /**
   * Method complete begins the current Cascade process if method {@link #start()} was not previously called. This method
   * blocks until the process completes.
   *
   * @throws RuntimeException wrapping any exception thrown internally.
   */
  public void complete()
    {
    start();

    try
      {
      try
        {
        thread.join();
        }
      catch( InterruptedException exception )
        {
        throw new FlowException( "thread interrupted", exception );
        }

      if( throwable instanceof CascadingException )
        throw (CascadingException) throwable;

      if( throwable != null )
        throw new CascadeException( "unhandled exception", throwable );
      }
    finally
      {
      thread = null;
      throwable = null;
      }
    }

  /** Method run implements the Runnable run method. */
  public void run()
    {
    if( LOG.isInfoEnabled() )
      LOG.info( "starting cascade: " + getName() );

    try
      {
      // keep topo order
      Map<String, Callable<Throwable>> jobsMap = new LinkedHashMap<String, Callable<Throwable>>();
      TopologicalOrderIterator<Flow, Integer> topoIterator = new TopologicalOrderIterator<Flow, Integer>( jobGraph );

      Flow flow = null;
      while( topoIterator.hasNext() )
        {
        flow = topoIterator.next();
        CascadeJob job = new CascadeJob( flow );

        jobsMap.put( flow.getName(), job );

        List<CascadeJob> predecessors = new ArrayList<CascadeJob>();

        for( Flow predecessor : Graphs.predecessorListOf( jobGraph, flow ) )
          predecessors.add( (CascadeJob) jobsMap.get( predecessor.getName() ) );

        job.init( predecessors );
        }

      // if jobs are run local, then only use one thread to force execution serially
      int numThreads = flow.jobsAreLocal() ? 1 : jobsMap.size();

      if( LOG.isDebugEnabled() )
        {
        LOG.debug( "is running all local: " + flow.jobsAreLocal() );
        LOG.debug( "num flows: " + jobsMap.size() );
        LOG.debug( "allocating num threads: " + numThreads );
        }

      ExecutorService executor = Executors.newFixedThreadPool( numThreads );
      List<Future<Throwable>> futures = executor.invokeAll( jobsMap.values() );

      executor.shutdown(); // don't accept any more work

      for( Future<Throwable> future : futures )
        {
        throwable = future.get();

        if( throwable != null )
          {
          LOG.warn( "stopping flows" );

          for( Callable<Throwable> callable : jobsMap.values() )
            ( (CascadeJob) callable ).stop();

          LOG.warn( "shutting down flow executor" );

          executor.awaitTermination( 5 * 60, TimeUnit.SECONDS );

          LOG.warn( "shutdown complete" );
          break;
          }
        }
      }
    catch( Throwable throwable )
      {
      this.throwable = throwable;
      }
    }

  @Override
  public String toString()
    {
    return getName();
    }

  protected static class CascadeJob implements Callable<Throwable>
    {
    Flow flow;
    private List<CascadeJob> predecessors;

    private CountDownLatch latch = new CountDownLatch( 1 );
    private boolean stop = false;
    private boolean failed = false;

    public CascadeJob( Flow flow )
      {
      this.flow = flow;
      }

    public String getName()
      {
      return flow.getName();
      }

    public boolean start() throws IOException
      {
      if( !flow.areSinksStale() )
        return false;

      flow.deleteSinks();
      flow.start();

      return true;
      }

    public Throwable call()
      {
      try
        {
        for( CascadeJob predecessor : predecessors )
          {
          if( !predecessor.isSuccessful() )
            return null;
          }

        if( stop )
          return null;

        try
          {
          if( LOG.isInfoEnabled() )
            LOG.info( "starting flow: " + flow.getName() );

          flow.complete();

          if( LOG.isInfoEnabled() )
            LOG.info( "completed flow: " + flow.getName() );
          }
        catch( Throwable exception )
          {
          LOG.info( "flow failed: " + flow.getName(), exception );
          failed = true;
          return new CascadeException( "flow failed: " + flow.getName(), exception );
          }
        }
      catch( Throwable throwable )
        {
        failed = true;
        return throwable;
        }
      finally
        {
        latch.countDown();
        }

      return null;
      }

    public void init( List<CascadeJob> predecessors )
      {
      this.predecessors = predecessors;
      }

    public void stop()
      {
      if( LOG.isInfoEnabled() )
        LOG.info( "stopping flow: " + flow.getName() );

      stop = true;

      if( flow != null )
        flow.stop();
      }

    public boolean isSuccessful()
      {
      try
        {
        latch.await();

        return flow != null && !failed;
        }
      catch( InterruptedException exception )
        {
        LOG.warn( "latch interrupted", exception );
        }

      return false;
      }
    }

  }
