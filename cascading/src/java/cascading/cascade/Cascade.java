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

package cascading.cascade;

import java.util.ArrayList;
import java.util.Collections;
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
import cascading.stats.CascadeStats;
import cascading.tap.Tap;
import cascading.util.Util;
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
  /** Field cascadeStats */
  private CascadeStats cascadeStats = new CascadeStats();
  /** Field thread */
  private Thread thread;
  /** Field throwable */
  private Throwable throwable;
  private ExecutorService executor;
  private Map<String, Callable<Throwable>> jobsMap;
  private boolean stop;

  Cascade( String name, SimpleDirectedGraph<Flow, Integer> jobGraph )
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
   * Method getCascadeStats returns the cascadeStats of this Cascade object.
   *
   * @return the cascadeStats (type CascadeStats) of this Cascade object.
   */
  public CascadeStats getCascadeStats()
    {
    return cascadeStats;
    }

  /**
   * Method getFlows returns the flows managed by this Cascade object. The returned {@link Flow} instances
   * will be in topological order.
   *
   * @return the flows (type Collection<Flow>) of this Cascade object.
   */
  public List<Flow> getFlows()
    {
    List<Flow> flows = new ArrayList<Flow>();
    TopologicalOrderIterator<Flow, Integer> topoIterator = new TopologicalOrderIterator<Flow, Integer>( jobGraph );

    while( topoIterator.hasNext() )
      flows.add( topoIterator.next() );

    return flows;
    }

  /**
   * Method start begins the current Cascade process. It returns immediately. See method {@link #complete()} to block
   * until the Cascade completes.
   */
  public void start()
    {
    if( thread != null )
      return;

    thread = new Thread( this, ( "cascade " + Util.toNull( getName() ) ).trim() );

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
      logInfo( "starting" );

    try
      {
      cascadeStats.markRunning();

      initializeNewJobsMap();

      int numThreads = jobsMap.size();

      if( LOG.isInfoEnabled() )
        {
        logInfo( " starting flows: " + numThreads );
        logInfo( " allocating threads: " + numThreads );
        }

      executor = Executors.newFixedThreadPool( numThreads );
      List<Future<Throwable>> futures = executor.invokeAll( jobsMap.values() );

      executor.shutdown(); // don't accept any more work

      for( Future<Throwable> future : futures )
        {
        throwable = future.get();

        if( throwable != null )
          {
          cascadeStats.markFailed( throwable );

          if( !stop )
            internalStopAllFlows();

          handleExecutorShutdown();
          break;
          }
        }
      }
    catch( Throwable throwable )
      {
      this.throwable = throwable;
      }
    finally
      {
      if( !cascadeStats.isFinished() )
        cascadeStats.markCompleted();
      }
    }

  private void initializeNewJobsMap()
    {
    // keep topo order
    jobsMap = new LinkedHashMap<String, Callable<Throwable>>();
    TopologicalOrderIterator<Flow, Integer> topoIterator = new TopologicalOrderIterator<Flow, Integer>( jobGraph );

    while( topoIterator.hasNext() )
      {
      Flow flow = topoIterator.next();

      cascadeStats.addFlowStats( flow.getFlowStats() );

      CascadeJob job = new CascadeJob( flow );

      jobsMap.put( flow.getName(), job );

      List<CascadeJob> predecessors = new ArrayList<CascadeJob>();

      for( Flow predecessor : Graphs.predecessorListOf( jobGraph, flow ) )
        predecessors.add( (CascadeJob) jobsMap.get( predecessor.getName() ) );

      job.init( predecessors );
      }
    }

  public synchronized void stop()
    {
    if( stop )
      return;

    stop = true;

    if( !cascadeStats.isFailed() )
      cascadeStats.markStopped();

    internalStopAllFlows();
    handleExecutorShutdown();
    }

  private void handleExecutorShutdown()
    {
    if( executor == null )
      return;

    logWarn( "shutting down flow executor" );

    try
      {
      executor.awaitTermination( 5 * 60, TimeUnit.SECONDS );
      }
    catch( InterruptedException exception )
      {
      // ignore
      }

    logWarn( "shutdown complete" );
    }

  private void internalStopAllFlows()
    {
    logWarn( "stopping flows" );

    List<Callable<Throwable>> jobs = new ArrayList<Callable<Throwable>>( jobsMap.values() );

    Collections.reverse( jobs );

    for( Callable<Throwable> callable : jobs )
      ( (CascadeJob) callable ).stop();

    logWarn( "stopped flows" );
    }

  @Override
  public String toString()
    {
    return getName();
    }

  private void logInfo( String message )
    {
    LOG.info( "[" + Util.truncate( getName(), 25 ) + "] " + message );
    }

  private void logWarn( String message )
    {
    logWarn( message, null );
    }

  private void logWarn( String message, Throwable throwable )
    {
    LOG.warn( "[" + Util.truncate( getName(), 25 ) + "] " + message, throwable );
    }

  /** Class CascadeJob manages Flow execution in the current Cascade instance. */
  protected class CascadeJob implements Callable<Throwable>
    {
    /** Field flow */
    Flow flow;
    /** Field predecessors */
    private List<CascadeJob> predecessors;
    /** Field latch */
    private CountDownLatch latch = new CountDownLatch( 1 );
    /** Field stop */
    private boolean stop = false;
    /** Field failed */
    private boolean failed = false;

    public CascadeJob( Flow flow )
      {
      this.flow = flow;
      }

    public String getName()
      {
      return flow.getName();
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
            logInfo( "starting flow: " + flow.getName() );

          if( flow.isSkipFlow() )
            {
            if( LOG.isInfoEnabled() )
              logInfo( "skipping flow, sinks are uptodate: " + flow.getName() );

            return null;
            }

          flow.deleteSinks();
          flow.complete();

          if( LOG.isInfoEnabled() )
            logInfo( "completed flow: " + flow.getName() );
          }
        catch( Throwable exception )
          {
          logWarn( "flow failed: " + flow.getName(), exception );
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
        logInfo( "stopping flow: " + flow.getName() );

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
        logWarn( "latch interrupted", exception );
        }

      return false;
      }
    }

  }
