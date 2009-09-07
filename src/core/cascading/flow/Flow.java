/*
 * Copyright (c) 2007-2009 Concurrent, Inc. All Rights Reserved.
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import cascading.CascadingException;
import cascading.cascade.Cascade;
import cascading.pipe.Pipe;
import cascading.stats.FlowStats;
import cascading.tap.Tap;
import cascading.tap.hadoop.HttpFileSystem;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import cascading.tuple.TupleIterator;
import cascading.util.Util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;
import org.jgrapht.Graphs;
import org.jgrapht.traverse.TopologicalOrderIterator;

/**
 * A {@link Pipe} assembly is connected to the necessary number of {@link Tap} sinks and
 * sources into a Flow.  A Flow is then executed to push the incoming source data through
 * the assembly into one or more sinks.
 * <p/>
 * Note that {@link Pipe} assemblies can be reused in multiple Flow instances. They maintain
 * no state regarding the Flow execution. Subsequently, {@link Pipe} assemblies can be given
 * parameters through its calling Flow so they can be built in a generic fashion.
 * <p/>
 * When a Flow is created, an optimized internal representation is created that is then executed
 * within the cluster. Thus any overhead inherent to a give {@link Pipe} assembly will be removed
 * once it's placed in context with the actual execution environment.
 * <p/>
 * <strong>Properties</strong><br/>
 * <ul>
 * <li>cascading.flow.preservetemporaryfiles</li>
 * <li>cascading.flow.stopjobsonexit</li>
 * </ul>
 *
 * @see cascading.flow.FlowConnector
 */
public class Flow implements FlowContext<Configuration>, Runnable
  {
  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( Flow.class );

  /** Field hdfsShutdown */
  private static Thread hdfsShutdown = null;
  /** Field shutdownCount */
  private static int shutdownCount = 0;

  /** Field id */
  private String id;
  /** Field name */
  private String name;
  /** Field listeners */
  private List<SafeFlowListener> listeners;
  /** Field skipStrategy */
  private FlowSkipStrategy flowSkipStrategy = new FlowSkipIfSinkStale();
  /** Field flowStats */
  private final FlowStats flowStats = new FlowStats(); // don't use a listener to set values
  /** Field sources */
  private Map<String, Tap> sources;
  /** Field sinks */
  private Map<String, Tap> sinks;
  /** Field traps */
  private Map<String, Tap> traps;
  /** Field preserveTemporaryFiles */
  private boolean preserveTemporaryFiles = false;
  /** Field stopJobsOnExit */
  protected boolean stopJobsOnExit = true;

  /** Field stepGraph */
  private StepGraph stepGraph;
  /** Field jobConf */
  private Configuration conf;
  /** Field thread */
  private Thread thread;
  /** Field throwable */
  private Throwable throwable;
  /** Field stop */
  private boolean stop;

  /** Field pipeGraph */
  private ElementGraph pipeGraph; // only used for documentation purposes

  /** Field steps */
  private transient List<FlowStep> steps;
  /** Field jobsMap */
  private transient Map<String, Callable<Throwable>> jobsMap;
  /** Field executor */
  private transient ExecutorService executor;
  /** Field shutdownHook */
  private transient Thread shutdownHook;

  /**
   * Property preserveTemporaryFiles forces the Flow instance to keep any temporary intermediate data sets. Useful
   * for debugging. Defaults to {@code false}.
   *
   * @param properties             of type Map
   * @param preserveTemporaryFiles of type boolean
   */
  public static void setPreserveTemporaryFiles( Map<Object, Object> properties, boolean preserveTemporaryFiles )
    {
    properties.put( "cascading.flow.preservetemporaryfiles", Boolean.toString( preserveTemporaryFiles ) );
    }

  /**
   * Returns property preserveTemporaryFiles.
   *
   * @param properties of type Map
   * @return a boolean
   */
  public static boolean getPreserveTemporaryFiles( Map<Object, Object> properties )
    {
    return Util.getProperty( properties, "cascading.flow.preservetemporaryfiles", false );
    }

  /**
   * Property stopJobsOnExit will tell the Flow to add a JVM shutdown hook that will kill all running processes if the
   * underlying computing system supports it. Defaults to {@code true}.
   *
   * @param properties     of type Map
   * @param stopJobsOnExit of type boolean
   */
  public static void setStopJobsOnExit( Map<Object, Object> properties, boolean stopJobsOnExit )
    {
    properties.put( "cascading.flow.stopjobsonexit", Boolean.toString( stopJobsOnExit ) );
    }

  /**
   * Returns property stopJobsOnExit.
   *
   * @param properties of type Map
   * @return a boolean
   */
  public static boolean getStopJobsOnExit( Map<Object, Object> properties )
    {
    return Util.getProperty( properties, "cascading.flow.stopjobsonexit", true );
    }

  /**
   * Property jobPollingInterval will set the time to wait between polling the remote server for the status of a job.
   * The default value is 5000 msec (5 seconds).   *
   *
   * @param properties of type Map
   * @param interval   of type long
   */
  public static void setJobPollingInterval( Map<Object, Object> properties, long interval )
    {
    properties.put( "cascading.flow.job.pollinginterval", Long.toString( interval ) );
    }

  /**
   * Returns property jobPollingInterval. The default is 5000 (5 sec).
   *
   * @param properties of type Map
   * @return a long
   */
  public static long getJobPollingInterval( Map<Object, Object> properties )
    {
    return Util.getProperty( properties, "cascading.flow.job.pollinginterval", 500 );
    }

  public static long getJobPollingInterval( Configuration conf )
    {
    return conf.getLong( "cascading.flow.job.pollinginterval", 5000 );
    }

  /**
   * Returns true if the given Configuration was created inside a running Flow.
   *
   * @param conf
   * @return
   */
  public static boolean isInflow( Configuration conf )
    {
    return conf.get( "cascading.flow.step" ) != null;
    }

  /** Used for testing. */
  protected Flow()
    {
    }

  protected Flow( Map<Object, Object> properties, Configuration conf, String name, ElementGraph pipeGraph, StepGraph stepGraph, Map<String, Tap> sources, Map<String, Tap> sinks, Map<String, Tap> traps )
    {
    this.name = name;
    this.pipeGraph = pipeGraph;
    this.stepGraph = stepGraph;
    setConf( conf );
    setSources( sources );
    setSinks( sinks );
    setTraps( traps );
    initFromProperties( properties );
    initFromTaps();
    }

  protected Flow( Map<Object, Object> properties, Configuration conf, String name, StepGraph stepGraph, Map<String, Tap> sources, Map<String, Tap> sinks, Map<String, Tap> traps )
    {
    this.name = name;
    this.stepGraph = stepGraph;
    setConf( conf );
    setSources( sources );
    setSinks( sinks );
    setTraps( traps );
    initFromProperties( properties );
    initFromTaps();
    }

  private void initFromProperties( Map<Object, Object> properties )
    {
    preserveTemporaryFiles = getPreserveTemporaryFiles( properties );
    stopJobsOnExit = getStopJobsOnExit( properties );
    }

  private void initFromTaps()
    {
    initFromTaps( sources );
    initFromTaps( sinks );
    initFromTaps( traps );
    }

  private void initFromTaps( Map<String, Tap> taps )
    {
    for( Tap tap : taps.values() )
      tap.flowInit( this );
    }

  /**
   * Method getName returns the name of this Flow object.
   *
   * @return the name (type String) of this Flow object.
   */
  public String getName()
    {
    return name;
    }

  protected void setName( String name )
    {
    this.name = name;
    }

  /**
   * Method getID returns the ID of this Flow object.
   * <p/>
   * The ID value is a long HEX String used to identify this instance globally. Subsequent Flow
   * instances created with identical paramers will not return the same ID.
   *
   * @return the ID (type String) of this Flow object.
   */
  public String getID()
    {
    if( id == null )
      id = Util.createUniqueID( getName() );

    return id;
    }

  protected void setSources( Map<String, Tap> sources )
    {
    addListeners( sources.values() );
    this.sources = sources;
    }

  protected void setSinks( Map<String, Tap> sinks )
    {
    addListeners( sinks.values() );
    this.sinks = sinks;
    }

  protected void setTraps( Map<String, Tap> traps )
    {
    addListeners( traps.values() );
    this.traps = traps;
    }

  protected void setStepGraph( StepGraph stepGraph )
    {
    this.stepGraph = stepGraph;
    }

  private void setConf( Configuration conf )
    {
    if( conf == null ) // this is ok, getJobConf will pass a default parent in
      return;

    this.conf = new Configuration( conf ); // prevent local values from being shared
    this.conf.set( "fs.http.impl", HttpFileSystem.class.getName() );
    this.conf.set( "fs.https.impl", HttpFileSystem.class.getName() );

    // set the ID for future reference
    this.conf.set( "cascading.flow.id", getID() );
    }

  /**
   * Method getJobConf returns the jobConf of this Flow object.
   *
   * @return the jobConf (type JobConf) of this Flow object.
   */
  public Configuration getConfiguration()
    {
    if( conf == null )
      setConf( new Configuration() );

    return conf;
    }

  /**
   * Method setProperty sets the given key and value on the underlying properites system.
   *
   * @param key   of type String
   * @param value of type String
   */
  public void setProperty( String key, String value )
    {
    getConfiguration().set( key, value );
    }

  /**
   * Method getProperty returns the value associated with the given key from the underlying properties system.
   *
   * @param key of type String
   * @return String
   */
  public String getProperty( String key )
    {
    return getConfiguration().get( key );
    }

  /**
   * Method getFlowStats returns the flowStats of this Flow object.
   *
   * @return the flowStats (type FlowStats) of this Flow object.
   */
  public FlowStats getFlowStats()
    {
    return flowStats;
    }

  void addListeners( Collection listeners )
    {
    for( Object listener : listeners )
      {
      if( listener instanceof FlowListener )
        addListener( (FlowListener) listener );
      }
    }

  List<SafeFlowListener> getListeners()
    {
    if( listeners == null )
      listeners = new LinkedList<SafeFlowListener>();

    return listeners;
    }

  /**
   * Method hasListeners returns true if {@link FlowListener} instances have been registered.
   *
   * @return boolean
   */
  public boolean hasListeners()
    {
    return listeners != null && !listeners.isEmpty();
    }

  /**
   * Method addListener registers the given flowListener with this instance.
   *
   * @param flowListener of type FlowListener
   */
  public void addListener( FlowListener flowListener )
    {
    getListeners().add( new SafeFlowListener( flowListener ) );
    }

  /**
   * Method removeListener removes the given flowListener from this instance.
   *
   * @param flowListener of type FlowListener
   * @return true if the listener was removed
   */
  public boolean removeListener( FlowListener flowListener )
    {
    return getListeners().remove( new SafeFlowListener( flowListener ) );
    }

  /**
   * Method getSources returns the sources of this Flow object.
   *
   * @return the sources (type Map) of this Flow object.
   */
  public Map<String, Tap> getSources()
    {
    return Collections.unmodifiableMap( sources );
    }

  /**
   * Method getSinks returns the sinks of this Flow object.
   *
   * @return the sinks (type Map) of this Flow object.
   */
  public Map<String, Tap> getSinks()
    {
    return Collections.unmodifiableMap( sinks );
    }

  /**
   * Method getTraps returns the traps of this Flow object.
   *
   * @return the traps (type Map<String, Tap>) of this Flow object.
   */
  public Map<String, Tap> getTraps()
    {
    return Collections.unmodifiableMap( traps );
    }

  /**
   * Method getSink returns the first sink of this Flow object.
   *
   * @return the sink (type Tap) of this Flow object.
   */
  public Tap getSink()
    {
    return sinks.values().iterator().next();
    }

  /**
   * Method isPreserveTemporaryFiles returns true if temporary files will be cleaned when this Flow completes.
   *
   * @return the preserveTemporaryFiles (type boolean) of this Flow object.
   */
  public boolean isPreserveTemporaryFiles()
    {
    return preserveTemporaryFiles;
    }

  /**
   * Method isStopJobsOnExit returns the stopJobsOnExit of this Flow object. Defaults to {@code true}.
   *
   * @return the stopJobsOnExit (type boolean) of this Flow object.
   */
  public boolean isStopJobsOnExit()
    {
    return stopJobsOnExit;
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
   * Method setFlowSkipStrategy sets a new {@link cascading.flow.FlowSkipStrategy}, the current strategy is returned.
   * <p/>
   * FlowSkipStrategy instances define when a Flow instance should be skipped. The default strategy is {@link cascading.flow.FlowSkipIfSinkStale}.
   * An alternative strategy would be {@link cascading.flow.FlowSkipIfSinkExists}.
   * <p/>
   * A FlowSkipStrategy will not be consulted when executing a Flow directly through {@link #start()} or {@link #complete()}. Only
   * when the Flow is executed through a {@link Cascade} instance.
   *
   * @param flowSkipStrategy of type FlowSkipStrategy
   * @return FlowSkipStrategy
   */
  public FlowSkipStrategy setFlowSkipStrategy( FlowSkipStrategy flowSkipStrategy )
    {
    if( flowSkipStrategy == null )
      throw new IllegalArgumentException( "flowSkipStrategy may not be null" );

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
   * Method isSkipFlow returns true if the parent {@link Cascade} should skip this Flow instance. True is returned
   * if the current {@link cascading.flow.FlowSkipStrategy} returns true.
   *
   * @return the skipFlow (type boolean) of this Flow object.
   * @throws IOException when
   */
  public boolean isSkipFlow() throws IOException
    {
    return flowSkipStrategy.skipFlow( this );
    }

  /**
   * Method areSinksStale returns true if any of the sinks referenced are out of date in relation to the sources. Or
   * if any sink method {@link Tap#isReplace()} returns true.
   *
   * @return boolean
   * @throws IOException when
   */
  public boolean areSinksStale() throws IOException
    {
    return areSourcesNewer( getSinkModified() );
    }

  /**
   * Method areSourcesNewer returns true if any source is newer than the given sinkModified date value.
   *
   * @param sinkModified of type long
   * @return boolean
   * @throws IOException when
   */
  public boolean areSourcesNewer( long sinkModified ) throws IOException
    {
    Job job = new Job( getConfiguration() ); // let's not add unused values by accident
    long sourceMod = 0;

    try
      {
      for( Tap source : sources.values() )
        {
        if( !source.pathExists( job ) )
          throw new FlowException( "source does not exist: " + source );

        sourceMod = source.getPathModified( job );

        if( sinkModified < sourceMod )
          return true;
        }

      return false;
      }
    finally
      {
      if( LOG.isInfoEnabled() )
        logInfo( "source modification date at: " + new Date( sourceMod ) ); // not oldest, we didnt check them all
      }
    }

  /**
   * Method getSinkModified returns the youngest modified date of any sink {@link Tap} managed by this Flow instance.
   * <p/>
   * If zero (0) is returned, atleast one of the sink resources does not exist. If minus one (-1) is returned,
   * atleast one of the sinks are marked for delete ({@link Tap#isReplace() returns true}).
   *
   * @return the sinkModified (type long) of this Flow object.
   * @throws IOException when
   */
  public long getSinkModified() throws IOException
    {
    Job job = new Job( getConfiguration() ); // let's not add unused values by accident
    long sinkModified = Long.MAX_VALUE;

    for( Tap sink : sinks.values() )
      {
      if( sink.isReplace() || sink.isUpdate() )
        sinkModified = -1L;
      else
        {
        if( !sink.pathExists( job ) )
          sinkModified = 0L;
        else
          sinkModified = Math.min( sinkModified, sink.getPathModified( job ) ); // return youngest mod date
        }
      }

    if( LOG.isInfoEnabled() )
      {
      if( sinkModified == -1L )
        logInfo( "atleast one sink is marked for delete" );
      if( sinkModified == 0L )
        logInfo( "atleast one sink does not exist" );
      else
        logInfo( "sink oldest modified date: " + new Date( sinkModified ) );
      }

    return sinkModified;
    }

  /**
   * Method getSteps returns the steps of this Flow object. They will be in topological order.
   *
   * @return the steps (type List<FlowStep>) of this Flow object.
   */
  public List<FlowStep> getSteps()
    {
    if( steps != null )
      return steps;

    TopologicalOrderIterator topoIterator = new TopologicalOrderIterator<FlowStep, Integer>( stepGraph );

    steps = new ArrayList<FlowStep>();

    while( topoIterator.hasNext() )
      steps.add( (FlowStep) topoIterator.next() );

    return steps;
    }

  /**
   * Method start begins the execution of this Flow instance. It will return immediately. Use the method {@link #complete()}
   * to block until this Flow completes.
   */
  public synchronized void start()
    {
    if( thread != null )
      return;

//    registerShutdownHook();

    thread = new Thread( this, ( "flow " + Util.toNull( getName() ) ).trim() );

    thread.start();
    }

  /** Method stop stops all running jobs, killing any currently executing. */
  public synchronized void stop()
    {
    if( stop )
      return;

    if( thread == null )
      return;

    stop = true;

    fireOnStopping();

    if( !flowStats.isFinished() )
      flowStats.markStopped();

    internalStopAllJobs();

    handleExecutorShutdown();
    }

  /** Method complete starts the current Flow instance if it has not be previously started, then block until completion. */
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
        throw new FlowException( getName(), "thread interrupted", exception );
        }

      if( throwable instanceof FlowException )
        ( (FlowException) throwable ).setFlowName( getName() );

      if( throwable instanceof CascadingException )
        throw (CascadingException) throwable;

      if( throwable != null )
        throw new FlowException( getName(), "unhandled exception", throwable );

      if( hasListeners() )
        {
        for( SafeFlowListener safeFlowListener : getListeners() )
          {
          if( safeFlowListener.throwable != null )
            throw new FlowException( getName(), "unhandled listener exception", throwable );
          }
        }
      }
    finally
      {
      thread = null;
      throwable = null;

      if( hasListeners() )
        {
        for( SafeFlowListener safeFlowListener : getListeners() )
          safeFlowListener.throwable = null;
        }
      }
    }

  /**
   * Method openSource opens the first source Tap.
   *
   * @return TupleIterator
   * @throws IOException when
   */
  public TupleEntryIterator openSource() throws IOException
    {
    return sources.values().iterator().next().openForRead( this );
    }

  /**
   * Method openSource opens the named source Tap.
   *
   * @param name of type String
   * @return TupleIterator
   * @throws IOException when
   */
  public TupleEntryIterator openSource( String name ) throws IOException
    {
    return sources.get( name ).openForRead( this );
    }

  /**
   * Method openSink opens the first sink Tap.
   *
   * @return TupleIterator
   * @throws IOException when
   */
  public TupleEntryIterator openSink() throws IOException
    {
    return sinks.values().iterator().next().openForRead( this );
    }

  /**
   * Method openSink opens the named sink Tap.
   *
   * @param name of type String
   * @return TupleIterator
   * @throws IOException when
   */
  public TupleEntryIterator openSink( String name ) throws IOException
    {
    return sinks.get( name ).openForRead( this );
    }

  /**
   * Method openTrap opens the first trap Tap.
   *
   * @return TupleIterator
   * @throws IOException when
   */
  public TupleEntryIterator openTrap() throws IOException
    {
    return traps.values().iterator().next().openForRead( this );
    }

  /**
   * Method openTrap opens the named trap Tap.
   *
   * @param name of type String
   * @return TupleIterator
   * @throws IOException when
   */
  public TupleEntryIterator openTrap( String name ) throws IOException
    {
    return traps.get( name ).openForRead( this );
    }

  /**
   * Method deleteSinks deletes all sinks, whether or not they are configured for {@link cascading.tap.SinkMode#UPDATE}.
   * <p/>
   * Use with caution.
   *
   * @throws IOException when
   * @see cascading.flow.Flow#deleteSinksIfNotAppend()
   */
  public void deleteSinks() throws IOException
    {
    for( Tap tap : sinks.values() )
      tap.deletePath( new Job( getConfiguration() ) );
    }

  /**
   * Method deleteSinksIfNotAppend deletes all sinks if they are not configured with the {@link cascading.tap.SinkMode#APPEND} flag.
   * <p/>
   * Typically used by a {@link Cascade} before executing the flow if the sinks are stale.
   * <p/>
   * Use with caution.
   *
   * @throws IOException when
   */
  public void deleteSinksIfNotAppend() throws IOException
    {
    for( Tap tap : sinks.values() )
      {
      if( !tap.isUpdate() )
        tap.deletePath( new Job( getConfiguration() ) );
      }
    }

  /**
   * Method deleteSinksIfNotUpdate deletes all sinks if they are not configured with the {@link cascading.tap.SinkMode#UPDATE} flag.
   * <p/>
   * Typically used by a {@link Cascade} before executing the flow if the sinks are stale.
   * <p/>
   * Use with caution.
   *
   * @throws IOException when
   */
  public void deleteSinksIfNotUpdate() throws IOException
    {
    for( Tap tap : sinks.values() )
      {
      if( !tap.isUpdate() )
        tap.deletePath( new Job( getConfiguration() ) );
      }
    }

  /**
   * Method tapExists returns true if the resource represented by the given Tap instance exists.
   *
   * @param tap of type Tap
   * @return boolean
   * @throws IOException when
   */
  public boolean tapPathExists( Tap tap ) throws IOException
    {
    return tap.pathExists( new Job( getConfiguration() ) );
    }

  /**
   * Method openTapForRead return a {@link TupleIterator} for the given Tap instance.
   *
   * @param tap of type Tap
   * @return TupleIterator
   * @throws IOException when there is an error opening the resource
   */
  public TupleEntryIterator openTapForRead( Tap tap ) throws IOException
    {
    return tap.openForRead( this );
    }

  /**
   * Method openTapForWrite returns a (@link TupleCollector} for the given Tap instance.
   *
   * @param tap of type Tap
   * @return TupleCollector
   * @throws IOException when there is an error opening the resource
   */
  public TupleEntryCollector openTapForWrite( Tap tap ) throws IOException
    {
    return tap.openForWrite( this );
    }

  /**
   * Method jobsAreLocal returns true if all jobs are executed in-process as a single map and reduce task.
   *
   * @return boolean
   */
  public boolean jobsAreLocal()
    {
    return getConfiguration().get( "mapred.job.tracker" ).equalsIgnoreCase( "local" );
    }

  /** Method run implements the Runnable run method and should not be called by users. */
  public void run()
    {
    if( thread == null )
      throw new IllegalStateException( "to start a Flow call start() or complete(), not Runnable#run()" );

    Cascade.printBanner();

    try
      {
      flowStats.markRunning();

      fireOnStarting();

      if( LOG.isInfoEnabled() )
        {
        logInfo( "starting" );

        for( Tap source : getSources().values() )
          logInfo( " source: " + source );
        for( Tap sink : getSinks().values() )
          logInfo( " sink: " + sink );
        }

      initializeNewJobsMap();

      // if jobs are run local, then only use one thread to force execution serially
      int numThreads = jobsAreLocal() ? 1 : jobsMap.size();

      if( numThreads == 0 )
        throw new IllegalStateException( "no jobs rendered for flow: " + getName() );

      if( LOG.isInfoEnabled() )
        {
        logInfo( " parallel execution is enabled: " + !jobsAreLocal() );
        logInfo( " starting jobs: " + jobsMap.size() );
        logInfo( " allocating threads: " + numThreads );
        }

      List<Future<Throwable>> futures = spawnJobs( numThreads );

      for( Future<Throwable> future : futures )
        {
        throwable = future.get();

        if( throwable != null )
          {
          if( !stop )
            internalStopAllJobs();

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
      if( !isPreserveTemporaryFiles() )
        cleanTemporaryFiles();

      handleThrowableAndMarkFailed();

      if( !stop && !flowStats.isFinished() )
        flowStats.markSuccessful();

      try
        {
        fireOnCompleted();
        }
      finally
        {
//        deregisterShutdownHook();
        }
      }
    }

  private List<Future<Throwable>> spawnJobs( int numThreads ) throws InterruptedException
    {
    if( stop )
      return new ArrayList<Future<Throwable>>();

    executor = Executors.newFixedThreadPool( numThreads );
    List<Future<Throwable>> futures = executor.invokeAll( jobsMap.values() ); // todo: consider submit()
    executor.shutdown(); // don't accept any more work
    return futures;
    }

  private void handleThrowableAndMarkFailed()
    {
    if( throwable != null && !stop )
      {
      flowStats.markFailed( throwable );

      fireOnThrowable();
      }
    }

  synchronized Map<String, Callable<Throwable>> getJobsMap()
    {
    return jobsMap;
    }

  private synchronized void initializeNewJobsMap() throws IOException
    {
    // keep topo order
    jobsMap = new LinkedHashMap<String, Callable<Throwable>>();
    TopologicalOrderIterator topoIterator = stepGraph.getTopologicalIterator();

    while( topoIterator.hasNext() )
      {
      FlowStep step = (FlowStep) topoIterator.next();
      FlowStepJob flowStepJob = step.createFlowStepJob( getConfiguration() );

      jobsMap.put( step.getName(), flowStepJob );

      List<FlowStepJob> predecessors = new ArrayList<FlowStepJob>();

      for( FlowStep flowStep : Graphs.predecessorListOf( stepGraph, step ) )
        predecessors.add( (FlowStepJob) jobsMap.get( flowStep.getName() ) );

      flowStepJob.setPredecessors( predecessors );

      flowStats.addStepStats( flowStepJob.getStepStats() );
      }
    }

  private void internalStopAllJobs()
    {
    LOG.warn( "stopping jobs" );

    try
      {
      if( jobsMap == null )
        return;

      List<Callable<Throwable>> jobs = new ArrayList<Callable<Throwable>>( jobsMap.values() );

      Collections.reverse( jobs );

      for( Callable<Throwable> callable : jobs )
        ( (FlowStepJob) callable ).stop();
      }
    finally
      {
      LOG.warn( "stopped jobs" );
      }
    }

  private void handleExecutorShutdown()
    {
    if( executor == null )
      return;

    LOG.warn( "shutting down job executor" );

    try
      {
      executor.awaitTermination( 5 * 60, TimeUnit.SECONDS );
      }
    catch( InterruptedException exception )
      {
      // ignore
      }

    LOG.warn( "shutdown complete" );
    }

  private void fireOnCompleted()
    {
    if( hasListeners() )
      {
      if( LOG.isDebugEnabled() )
        logDebug( "firing onCompleted event: " + getListeners().size() );

      for( FlowListener flowListener : getListeners() )
        flowListener.onCompleted( this );
      }
    }

  private void fireOnThrowable()
    {
    if( hasListeners() )
      {
      if( LOG.isDebugEnabled() )
        logDebug( "firing onThrowable event: " + getListeners().size() );

      boolean isHandled = false;

      for( FlowListener flowListener : getListeners() )
        isHandled = flowListener.onThrowable( this, throwable ) || isHandled;

      if( isHandled )
        throwable = null;
      }
    }

  private void fireOnStopping()
    {
    if( hasListeners() )
      {
      if( LOG.isDebugEnabled() )
        logDebug( "firing onStopping event: " + getListeners().size() );

      for( FlowListener flowListener : getListeners() )
        flowListener.onStopping( this );
      }
    }

  private void fireOnStarting()
    {
    if( hasListeners() )
      {
      if( LOG.isDebugEnabled() )
        logDebug( "firing onStarting event: " + getListeners().size() );

      for( FlowListener flowListener : getListeners() )
        flowListener.onStarting( this );
      }
    }

  private void cleanTemporaryFiles()
    {
    if( stop ) // unstable to call fs operations during shutdown
      return;

    for( FlowStep step : getSteps() )
      step.clean( getConfiguration() );
    }

  private void registerShutdownHook()
    {
    if( !isStopJobsOnExit() )
      return;

    getHdfsShutdownHook();

    shutdownHook = new Thread()
    {
    @Override
    public void run()
      {
      Flow.this.stop();

      callHdfsShutdownHook();
      }
    };

    Runtime.getRuntime().addShutdownHook( shutdownHook );
    }

  private synchronized static void callHdfsShutdownHook()
    {
    if( --shutdownCount != 0 )
      return;

    if( hdfsShutdown != null )
      hdfsShutdown.start();
    }

  private synchronized static void getHdfsShutdownHook()
    {
    shutdownCount++;

    if( hdfsShutdown == null )
      hdfsShutdown = Util.getHDFSShutdownHook();
    }

  private void deregisterShutdownHook()
    {
    if( !isStopJobsOnExit() || stop )
      return;

    Runtime.getRuntime().removeShutdownHook( shutdownHook );
    }

  @Override
  public String toString()
    {
    StringBuffer buffer = new StringBuffer();

    if( getName() != null )
      buffer.append( getName() ).append( ": " );

    for( FlowStep step : getSteps() )
      buffer.append( step );

    return buffer.toString();
    }

  private void logInfo( String message )
    {
    LOG.info( "[" + Util.truncate( getName(), 25 ) + "] " + message );
    }

  private void logDebug( String message )
    {
    LOG.debug( "[" + Util.truncate( getName(), 25 ) + "] " + message );
    }

  private void logWarn( String message, Throwable throwable )
    {
    LOG.warn( "[" + Util.truncate( getName(), 25 ) + "] " + message, throwable );
    }

  /**
   * Method writeDOT writes this Flow instance to the given filename as a DOT file for import into a graphics package.
   *
   * @param filename of type String
   */
  public void writeDOT( String filename )
    {
    if( pipeGraph == null )
      throw new UnsupportedOperationException( "this flow instance cannot write a DOT file" );

    pipeGraph.writeDOT( filename );
    }

  /**
   * Method writeStepsDOT writes this Flow step graph to the given filename as a DOT file for import into a graphics package.
   *
   * @param filename of type String
   */
  public void writeStepsDOT( String filename )
    {
    if( stepGraph == null )
      throw new UnsupportedOperationException( "this flow instance cannot write a DOT file" );

    stepGraph.writeDOT( filename );
    }

  /**
   * Used to return a simple wrapper for use as an edge in a graph where there can only be
   * one instance of every edge.
   *
   * @return FlowHolder
   */
  public FlowHolder getHolder()
    {
    return new FlowHolder( this );
    }

  /** Class FlowHolder is a helper class for wrapping Flow instances. */
  public static class FlowHolder
    {
    /** Field flow */
    public Flow flow;

    public FlowHolder()
      {
      }

    public FlowHolder( Flow flow )
      {
      this.flow = flow;
      }
    }

  /**
   * Class SafeFlowListener safely calls a wrapped FlowListener.
   * <p/>
   * This is done for a few reasons, the primary reason is so exceptions thrown by the Listener
   * can be caught by the calling Thread. Since Flow is asyncronous, much of the work is done in the run() method
   * which in turn is run in a new Thread.
   */
  private class SafeFlowListener implements FlowListener
    {
    /** Field flowListener */
    final FlowListener flowListener;
    /** Field throwable */
    Throwable throwable;

    private SafeFlowListener( FlowListener flowListener )
      {
      this.flowListener = flowListener;
      }

    public void onStarting( Flow flow )
      {
      try
        {
        flowListener.onStarting( flow );
        }
      catch( Throwable throwable )
        {
        handleThrowable( throwable );
        }
      }

    public void onStopping( Flow flow )
      {
      try
        {
        flowListener.onStopping( flow );
        }
      catch( Throwable throwable )
        {
        handleThrowable( throwable );
        }
      }

    public void onCompleted( Flow flow )
      {
      try
        {
        flowListener.onCompleted( flow );
        }
      catch( Throwable throwable )
        {
        handleThrowable( throwable );
        }
      }

    public boolean onThrowable( Flow flow, Throwable flowThrowable )
      {
      try
        {
        return flowListener.onThrowable( flow, flowThrowable );
        }
      catch( Throwable throwable )
        {
        handleThrowable( throwable );
        }

      return false;
      }

    private void handleThrowable( Throwable throwable )
      {
      this.throwable = throwable;

      logWarn( String.format( "flow listener %s threw throwable", flowListener ), throwable );

      // stop this flow
      stop();
      }

    public boolean equals( Object object )
      {
      if( object instanceof SafeFlowListener )
        return flowListener.equals( ( (SafeFlowListener) object ).flowListener );

      return flowListener.equals( object );
      }

    public int hashCode()
      {
      return flowListener.hashCode();
      }
    }

  }
