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
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import cascading.CascadingException;
import cascading.flow.Flow;
import cascading.flow.FlowException;
import cascading.flow.FlowSkipStrategy;
import cascading.stats.CascadeStats;
import cascading.tap.Tap;
import cascading.util.Util;
import org.apache.log4j.Logger;
import org.jgrapht.Graphs;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.jgrapht.traverse.TopologicalOrderIterator;

/**
 * A Cascade is an assembly of {@link Flow} instances that share or depend on equivalent {@link Tap} instances and are executed as
 * a single group. The most common case is where one Flow instance depends on a Tap created by a second Flow instance. This
 * dependency chain can continue as practical.
 * <p/>
 * Note Flow instances that have no shared dependencies will be executed in parallel.
 * <p/>
 * Additionally, a Cascade allows for incremental builds of complex data processing processes. If a given source {@link Tap} is newer than
 * a subsequent sink {@link Tap} in the assembly, the connecting {@link Flow}(s) will be executed
 * when the Cascade executed. If all the targets (sinks) are up to date, the Cascade exits immediately and does nothing.
 * <p/>
 * The concept of 'stale' is pluggable, see the {@link cascading.flow.FlowSkipStrategy} class.
 * <p/>
 * When a Cascade starts up, if first verifies which Flow instances have stale sinks, if the sinks are not stale, the
 * method {@link cascading.flow.Flow#deleteSinksIfNotAppend()} is called. Before appends were supported (logically)
 * the Cascade deleted all the sinks in a Flow.
 * <p/>
 * The new consequence of this is if the Cascade fails, but does complete a Flow that appended data, re-running
 * the Cascade (and the successful append Flow) will re-append data to the source. Some systems may be idempotent and
 * may not have any side-effects. So plan accordingly.
 *
 * @see Flow
 * @see cascading.flow.FlowSkipStrategy
 */
public class Cascade implements Runnable
  {
  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( Cascade.class );
  /** Field versionProperties */
  private static Properties versionProperties;

  /** Field id */
  private String id;
  /** Field name */
  private String name;
  /** Field jobGraph */
  private final SimpleDirectedGraph<Flow, Integer> jobGraph;
  /** Field cascadeStats */
  private final CascadeStats cascadeStats;
  /** Field thread */
  private Thread thread;
  /** Field throwable */
  private Throwable throwable;
  /** Field executor */
  private ExecutorService executor;
  /** Field jobsMap */
  private Map<String, Callable<Throwable>> jobsMap;
  /** Field stop */
  private boolean stop;
  /** Field flowSkipStrategy */
  private FlowSkipStrategy flowSkipStrategy = null;

  Cascade( String name, SimpleDirectedGraph<Flow, Integer> jobGraph )
    {
    this.name = name;
    this.jobGraph = jobGraph;
    this.cascadeStats = new CascadeStats( getID() );
    setIDOnFlow();
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
   * Method getID returns the ID of this Cascade object.
   * <p/>
   * The ID value is a long HEX String used to identify this instance globally. Subsequent Cascade
   * instances created with identical paramers will not return the same ID.
   *
   * @return the ID (type String) of this Cascade object.
   */
  public String getID()
    {
    if( id == null )
      id = Util.createUniqueID( getName() );

    return id;
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

  private void setIDOnFlow()
    {
    for( Flow flow : getFlows() )
      flow.setProperty( "cascading.cascade.id", getID() );
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
   * Method getFlowSkipStrategy returns the current {@link cascading.flow.FlowSkipStrategy} used by this Flow.
   *
   * @return FlowSkipStrategy
   */
  public FlowSkipStrategy getFlowSkipStrategy()
    {
    return flowSkipStrategy;
    }

  /**
   * Method setFlowSkipStrategy sets a new {@link cascading.flow.FlowSkipStrategy}, the current strategy, if any, is returned.
   * If a strategy is given, it will be used as the strategy for all {@link Flow} instances managed by this Cascade instance.
   * To revert back to consulting the strategies associated with each Flow instance, re-set this value to {@code null}, its
   * default value.
   * <p/>
   * FlowSkipStrategy instances define when a Flow instance should be skipped. The default strategy is {@link cascading.flow.FlowSkipIfSinkStale}
   * and is inherited from the Flow instance in question. An alternative strategy would be {@link cascading.flow.FlowSkipIfSinkExists}.
   * <p/>
   * A FlowSkipStrategy will not be consulted when executing a Flow directly through {@link #start()}
   *
   * @param flowSkipStrategy of type FlowSkipStrategy
   * @return FlowSkipStrategy
   */
  public FlowSkipStrategy setFlowSkipStrategy( FlowSkipStrategy flowSkipStrategy )
    {
    try
      {
      return this.flowSkipStrategy;
      }
    finally
      {
      this.flowSkipStrategy = flowSkipStrategy;
      }
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
    printBanner();

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
        cascadeStats.markSuccessful();
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

  public static synchronized void printBanner()
    {
    if( versionProperties != null )
      return;

    versionProperties = new Properties();

    try
      {
      InputStream stream = Cascade.class.getClassLoader().getResourceAsStream( "cascading/version.properties" );

      if( stream == null )
        return;

      versionProperties.load( stream );

      stream = Cascade.class.getClassLoader().getResourceAsStream( "cascading/build.number.properties" );

      if( stream != null )
        versionProperties.load( stream );

      String releaseMajor = versionProperties.getProperty( "cascading.release.major" );
      String releaseMinor = versionProperties.getProperty( "cascading.release.minor", null );
      String releaseBuild = versionProperties.getProperty( "build.number", null );
      String hadoopVersion = versionProperties.getProperty( "cascading.hadoop.compatible.version" );
      String releaseFull = null;

      if( releaseMinor == null )
        releaseFull = releaseMajor;
      else
        releaseFull = String.format( "%s.%s", releaseMajor, releaseMinor );

      String message = null;

      if( releaseBuild == null )
        message = String.format( "Concurrent, Inc - Cascading %s [%s]", releaseFull, hadoopVersion );
      else
        message = String.format( "Concurrent, Inc - Cascading %s-%s [%s]", releaseFull, releaseBuild, hadoopVersion );

      LOG.info( message );
      }
    catch( IOException exception )
      {
      LOG.warn( "unable to load version information", exception );
      }
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

          if( flowSkipStrategy == null ? flow.isSkipFlow() : flowSkipStrategy.skipFlow( flow ) )
            {
            if( LOG.isInfoEnabled() )
              logInfo( "skipping flow: " + flow.getName() );

            return null;
            }

          flow.deleteSinksIfNotAppend(); // do not delete append mode taps
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
