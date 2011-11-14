/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import cascading.CascadingException;
import cascading.cascade.Cascade;
import cascading.flow.planner.ElementGraph;
import cascading.flow.planner.FlowStep;
import cascading.flow.planner.FlowStepJob;
import cascading.flow.planner.StepGraph;
import cascading.management.CascadingServices;
import cascading.management.ClientState;
import cascading.management.ClientType;
import cascading.pipe.Pipe;
import cascading.stats.FlowStats;
import cascading.tap.Tap;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import cascading.tuple.TupleIterator;
import cascading.util.PropertyUtil;
import cascading.util.Util;
import cascading.util.Version;
import org.jgrapht.Graphs;
import org.jgrapht.traverse.TopologicalOrderIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import riffle.process.DependencyIncoming;
import riffle.process.DependencyOutgoing;
import riffle.process.ProcessCleanup;
import riffle.process.ProcessComplete;
import riffle.process.ProcessPrepare;
import riffle.process.ProcessStart;
import riffle.process.ProcessStop;

/**
 * A Flow is a logical unit of work declared by an assembly of {@link Pipe} instances connected to source
 * and sink {@link Tap} instances.
 * <p/>
 * A Flow is then executed to push the incoming source data through the assembly into one or more sinks.
 * <p/>
 * A Flow sub-class instance may not be instantiated directly in most cases, see sub-classes of {@link FlowConnector} class
 * for supported platforms.
 * <p/>
 * Note that {@link Pipe} assemblies can be reused in multiple Flow instances. They maintain
 * no state regarding the Flow execution. Subsequently, {@link Pipe} assemblies can be given
 * parameters through its calling Flow so they can be built in a generic fashion.
 * <p/>
 * When a Flow is created, an optimized internal representation is created that is then executed
 * on the underlying execution platform.
 * </p>
 * <strong>Properties</strong><br/>
 * <ul>
 * <li>cascading.flow.preservetemporaryfiles</li>
 * <li>cascading.flow.stopjobsonexit</li>
 * </ul>
 *
 * @see cascading.flow.FlowConnector
 */
@riffle.process.Process
public abstract class Flow<Config> implements Runnable
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( Flow.class );

  /** Field id */
  private String id;
  /** Field name */
  private String name;
  /** Field tags */
  private String tags;
  /** Field listeners */
  private List<SafeFlowListener> listeners;
  /** Field skipStrategy */
  private FlowSkipStrategy flowSkipStrategy = new FlowSkipIfSinkNotStale();
  /** Field flowStats */
  protected final FlowStats flowStats; // don't use a listener to set values
  /** Field sources */
  protected Map<String, Tap> sources = Collections.EMPTY_MAP;
  /** Field sinks */
  protected Map<String, Tap> sinks = Collections.EMPTY_MAP;
  /** Field traps */
  private Map<String, Tap> traps = Collections.EMPTY_MAP;
  /** Field stopJobsOnExit */
  protected boolean stopJobsOnExit = true;
  /** Field submitPriority */
  private int submitPriority = 5;

  /** Field stepGraph */
  private StepGraph stepGraph;
  /** Field thread */
  protected transient Thread thread;
  /** Field throwable */
  private Throwable throwable;
  /** Field stop */
  protected boolean stop;

  /** Field pipeGraph */
  private ElementGraph pipeGraph; // only used for documentation purposes

  private transient CascadingServices cascadingServices;

  /** Field steps */
  private transient List<FlowStep> steps;
  /** Field jobsMap */
  private transient Map<String, Callable<Throwable>> jobsMap;
  /** Field executor */
  private transient ExecutorService executor;

  private transient ReentrantLock stopLock = new ReentrantLock( true );

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
    return Boolean.parseBoolean( PropertyUtil.getProperty( properties, "cascading.flow.stopjobsonexit", "true" ) );
    }

  /** Used for testing. */
  protected Flow()
    {
    this.name = "NA";
    this.flowStats = createPrepareFlowStats();
    }

  protected Flow( Map<Object, Object> properties, Config defaultConfig, String name )
    {
    this.name = name;
    addSessionProperties( properties );
    initConfig( properties, defaultConfig );
    initSteps();
    initFromProperties( properties );

    this.flowStats = createPrepareFlowStats(); // must be last
    }

  protected Flow( Map<Object, Object> properties, Config defaultConfig, FlowDef flowDef, ElementGraph pipeGraph, StepGraph stepGraph )
    {
    this.name = flowDef.getName();
    this.tags = flowDef.getTags();
    this.pipeGraph = pipeGraph;
    this.stepGraph = stepGraph;
    addSessionProperties( properties );
    initConfig( properties, defaultConfig );
    initSteps();
    setSources( flowDef.getSourcesCopy() );
    setSinks( flowDef.getSinksCopy() );
    setTraps( flowDef.getTrapsCopy() );
    initFromProperties( properties );
    initFromTaps();

    this.flowStats = createPrepareFlowStats(); // must be last

    initializeNewJobsMap();
    }

  private void addSessionProperties( Map<Object, Object> properties )
    {
    if( properties == null )
      return;

    PropertyUtil.setProperty( properties, "cascading.flow.id", getID() );
    PropertyUtil.setProperty( properties, "cascading.flow.tags", getTags() );
    FlowConnector.setApplicationID( properties );
    PropertyUtil.setProperty( properties, "cascading.app.name", makeAppName( properties ) );
    PropertyUtil.setProperty( properties, "cascading.app.version", makeAppVersion( properties ) );
    }

  private String makeAppName( Map<Object, Object> properties )
    {
    if( properties == null )
      return null;

    String name = FlowConnector.getApplicationName( properties );

    if( name != null )
      return name;

    String path = FlowConnector.getApplicationJarPath( properties );

    if( path == null )
      return null;

    String[] split = path.split( "/" );

    path = split[ split.length - 1 ];

    return path.substring( 0, path.lastIndexOf( '.' ) );
    }

  private String makeAppVersion( Map<Object, Object> properties )
    {
    if( properties == null )
      return null;

    String name = FlowConnector.getApplicationVersion( properties );

    if( name != null )
      return name;

    String path = FlowConnector.getApplicationJarPath( properties );

    if( path == null )
      return null;

    String[] split = path.split( "/" );

    path = split[ split.length - 1 ];

    path = path.substring( 0, path.lastIndexOf( '.' ) );

    return path.replace( "^\\D*(.*)$", "$1" );
    }

  private FlowStats createPrepareFlowStats()
    {
    FlowStats flowStats = new FlowStats( this, getClientState() );

    flowStats.prepare();
    flowStats.markPending();

    return flowStats;
    }

  public CascadingServices getCascadingServices()
    {
    if( cascadingServices == null )
      cascadingServices = new CascadingServices( getConfigAsProperties() );

    return cascadingServices;
    }

  private ClientState getClientState()
    {
    return getFlowSession().getCascadingServices().createClientState( ClientType.session, getID() );
    }

  private void initSteps()
    {
    if( stepGraph == null )
      return;

    for( FlowStep flowStep : stepGraph.vertexSet() )
      flowStep.setFlowID( getID() );
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
      tap.flowConfInit( this );
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
   * instances created with identical parameters will not return the same ID.
   *
   * @return the ID (type String) of this Flow object.
   */
  public String getID()
    {
    if( id == null )
      id = Util.createUniqueID( getName() );

    return id;
    }

  public String getTags()
    {
    return tags;
    }

  /**
   * Method getSubmitPriority returns the submitPriority of this Flow object.
   * <p/>
   * 10 is lowest, 1 is the highest, 5 is the default.
   *
   * @return the submitPriority (type int) of this FlowStep object.
   */
  public int getSubmitPriority()
    {
    return submitPriority;
    }

  /**
   * Method setSubmitPriority sets the submitPriority of this Flow object.
   * <p/>
   * 10 is lowest, 1 is the highest, 5 is the default.
   *
   * @param submitPriority the submitPriority of this FlowStep object.
   */
  public void setSubmitPriority( int submitPriority )
    {
    if( submitPriority < 1 || submitPriority > 10 )
      throw new IllegalArgumentException( "submitPriority must be between 1 and 10 inclusive, was: " + submitPriority );

    this.submitPriority = submitPriority;
    }

  public ElementGraph getPipeGraph()
    {
    return pipeGraph;
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

  /**
   * This method creates a new internal Config with the parentConfig as defaults using the properties to override
   * the defaults.
   *
   * @param properties
   * @param parentConfig
   */
  protected abstract void initConfig( Map<Object, Object> properties, Config parentConfig );

  public Config createConfig( Map<Object, Object> properties, Config defaultConfig )
    {
    Config config = newConfig( defaultConfig );

    if( properties == null )
      return config;

    Set<Object> keys = new HashSet<Object>( properties.keySet() );

    // keys will only be grabbed if both key/value are String, so keep orig keys
    if( properties instanceof Properties )
      keys.addAll( ( (Properties) properties ).stringPropertyNames() );

    for( Object key : keys )
      {
      Object value = properties.get( key );

      if( value == null && properties instanceof Properties && key instanceof String )
        value = ( (Properties) properties ).getProperty( (String) key );

      if( value == null ) // don't stuff null values
        continue;

      setConfigProperty( config, key, value );
      }

    return config;
    }

  protected abstract void setConfigProperty( Config config, Object key, Object value );

  protected abstract Config newConfig( Config defaultConfig );

  public abstract Config getConfig();

  public abstract Config getConfigCopy();

  public abstract Map<Object, Object> getConfigAsProperties();

  public void setProperty( String key, String value )
    {
    setConfigProperty( getConfig(), key, value );
    }

  public abstract String getProperty( String key );

  protected void initFromProperties( Map<Object, Object> properties )
    {
    stopJobsOnExit = getStopJobsOnExit( properties );
    }

  public FlowSession getFlowSession()
    {
    return new FlowSession( getCascadingServices() );
    }

  public abstract FlowProcess getFlowProcess();

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

  public Tap getSource( String name )
    {
    return sources.get( name );
    }

  /**
   * Method getSourcesCollection returns a {@link Collection} of source {@link Tap}s for this Flow object.
   *
   * @return the sourcesCollection (type Collection<Tap>) of this Flow object.
   */
  @DependencyIncoming
  public Collection<Tap> getSourcesCollection()
    {
    return getSources().values();
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

  public Tap getSink( String name )
    {
    return sinks.get( name );
    }

  /**
   * Method getSinksCollection returns a {@link Collection} of sink {@link Tap}s for this Flow object.
   *
   * @return the sinkCollection (type Collection<Tap>) of this Flow object.
   */
  @DependencyOutgoing
  public Collection<Tap> getSinksCollection()
    {
    return getSinks().values();
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
   * Method getTrapsCollection returns a {@link Collection} of trap {@link Tap}s for this Flow object.
   *
   * @return the trapsCollection (type Collection<Tap>) of this Flow object.
   */
  public Collection<Tap> getTrapsCollection()
    {
    return getTraps().values();
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
   * FlowSkipStrategy instances define when a Flow instance should be skipped. The default strategy is {@link FlowSkipIfSinkNotStale}.
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
   * if any sink method {@link cascading.tap.Tap#isReplace()} returns true.
   *
   * @return boolean
   * @throws java.io.IOException when
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
   * @throws java.io.IOException when
   */
  public boolean areSourcesNewer( long sinkModified ) throws IOException
    {
    Config confCopy = getConfigCopy();
    long sourceMod = 0;

    try
      {
      for( Tap source : sources.values() )
        {
        if( !source.resourceExists( confCopy ) )
          throw new FlowException( "source does not exist: " + source );

        sourceMod = source.getModifiedTime( confCopy );

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
   * Method getSinkModified returns the youngest modified date of any sink {@link cascading.tap.Tap} managed by this Flow instance.
   * <p/>
   * If zero (0) is returned, at least one of the sink resources does not exist. If minus one (-1) is returned,
   * atleast one of the sinks are marked for delete ({@link cascading.tap.Tap#isReplace() returns true}).
   *
   * @return the sinkModified (type long) of this Flow object.
   * @throws java.io.IOException when
   */
  public long getSinkModified() throws IOException
    {
    Config confCopy = getConfigCopy();
    long sinkModified = Long.MAX_VALUE;

    for( Tap sink : sinks.values() )
      {
      if( sink.isReplace() || sink.isUpdate() )
        sinkModified = -1L;
      else
        {
        if( !sink.resourceExists( confCopy ) )
          sinkModified = 0L;
        else
          sinkModified = Math.min( sinkModified, sink.getModifiedTime( confCopy ) ); // return youngest mod date
        }
      }

    if( LOG.isInfoEnabled() )
      {
      if( sinkModified == -1L )
        logInfo( "at least one sink is marked for delete" );
      if( sinkModified == 0L )
        logInfo( "at least one sink does not exist" );
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

    if( stepGraph == null )
      return Collections.EMPTY_LIST;

    TopologicalOrderIterator topoIterator = new TopologicalOrderIterator<FlowStep, Integer>( stepGraph );

    steps = new ArrayList<FlowStep>();

    while( topoIterator.hasNext() )
      steps.add( (FlowStep) topoIterator.next() );

    return steps;
    }

  /**
   * Method prepare is used by a {@link Cascade} to notify the given Flow it should initialize or clear any resources
   * necessary for {@link #start()} to be called successfully.
   * <p/>
   * Specifically, this implementation calls {@link #deleteSinksIfNotUpdate()} && {@link #deleteTrapsIfNotUpdate()}.
   *
   * @throws IOException when
   */
  @ProcessPrepare
  public void prepare()
    {
    try
      {
      deleteSinksIfNotUpdate();
      deleteTrapsIfNotUpdate();
      }
    catch( IOException exception )
      {
      throw new FlowException( "unable to prepare flow", exception );
      }
    }

  /**
   * Method start begins the execution of this Flow instance. It will return immediately. Use the method {@link #complete()}
   * to block until this Flow completes.
   */
  @ProcessStart
  public synchronized void start()
    {
    if( thread != null )
      return;

    if( stop )
      return;

    internalStart();

    thread = new Thread( this, ( "flow " + Util.toNull( getName() ) ).trim() );

    thread.start();
    }

  protected abstract void internalStart();

  /** Method stop stops all running jobs, killing any currently executing. */
  @ProcessStop
  public synchronized void stop()
    {
    stopLock.lock();

    try
      {
      if( stop )
        return;

      stop = true;

      fireOnStopping();

      if( !flowStats.isFinished() )
        flowStats.markStopped();

      internalStopAllJobs();

      handleExecutorShutdown();

      internalClean( true );
      }
    finally
      {
      flowStats.cleanup();
      stopLock.unlock();
      }
    }

  protected abstract void internalClean( boolean force );

  /** Method complete starts the current Flow instance if it has not be previously started, then block until completion. */
  @ProcessComplete
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

      // if in #stop and stopping, lets wait till its done in this thread
      try
        {
        stopLock.lock();
        }
      finally
        {
        stopLock.unlock();
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

      try
        {
        if( hasListeners() )
          {
          for( SafeFlowListener safeFlowListener : getListeners() )
            safeFlowListener.throwable = null;
          }
        }
      finally
        {
        flowStats.cleanup();
        }
      }
    }

  @ProcessCleanup
  public void cleanup()
    {
    // do nothing
    }

  /**
   * Method openSource opens the first source Tap.
   *
   * @return TupleIterator
   * @throws IOException when
   */
  public TupleEntryIterator openSource() throws IOException
    {
    return sources.values().iterator().next().openForRead( getFlowProcess() );
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
    if( !sources.containsKey( name ) )
      throw new IllegalArgumentException( "source does not exist: " + name );

    return sources.get( name ).openForRead( getFlowProcess() );
    }

  /**
   * Method openSink opens the first sink Tap.
   *
   * @return TupleIterator
   * @throws IOException when
   */
  public TupleEntryIterator openSink() throws IOException
    {
    return sinks.values().iterator().next().openForRead( getFlowProcess() );
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
    if( !sinks.containsKey( name ) )
      throw new IllegalArgumentException( "sink does not exist: " + name );

    return sinks.get( name ).openForRead( getFlowProcess(), null );
    }

  /**
   * Method openTrap opens the first trap Tap.
   *
   * @return TupleIterator
   * @throws IOException when
   */
  public TupleEntryIterator openTrap() throws IOException
    {
    return traps.values().iterator().next().openForRead( getFlowProcess() );
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
    if( !traps.containsKey( name ) )
      throw new IllegalArgumentException( "trap does not exist: " + name );

    return traps.get( name ).openForRead( getFlowProcess() );
    }

  /**
   * Method deleteSinks deletes all sinks, whether or not they are configured for {@link cascading.tap.SinkMode#UPDATE}.
   * <p/>
   * Use with caution.
   *
   * @throws IOException when
   * @see Flow#deleteSinksIfNotUpdate()
   */
  public void deleteSinks() throws IOException
    {
    for( Tap tap : sinks.values() )
      tap.deleteResource( getConfig() );
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
        tap.deleteResource( getConfig() );
      }
    }

  public void deleteSinksIfReplace() throws IOException
    {
    for( Tap tap : sinks.values() )
      {
      if( tap.isReplace() )
        tap.deleteResource( getConfig() );
      }
    }

  public void deleteTrapsIfNotUpdate() throws IOException
    {
    for( Tap tap : traps.values() )
      {
      if( !tap.isUpdate() )
        tap.deleteResource( getConfig() );
      }
    }

  public void deleteTrapsIfReplace() throws IOException
    {
    for( Tap tap : traps.values() )
      {
      if( tap.isReplace() )
        tap.deleteResource( getConfig() );
      }
    }

  /**
   * Method resourceExists returns true if the resource represented by the given Tap instance exists.
   *
   * @param tap of type Tap
   * @return boolean
   * @throws IOException when
   */
  public boolean resourceExists( Tap tap ) throws IOException
    {
    return tap.resourceExists( getConfig() );
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
    return tap.openForRead( getFlowProcess(), null );
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
    return tap.openForWrite( getFlowProcess(), null );
    }

  /**
   * Method jobsAreLocal returns true if all jobs are executed in-process as a single map and reduce task.
   *
   * @return boolean
   */
  public abstract boolean stepsAreLocal();

  /** Method run implements the Runnable run method and should not be called by users. */
  public void run()
    {
    if( thread == null )
      throw new IllegalStateException( "to start a Flow call start() or complete(), not Runnable#run()" );

    Version.printBanner();

    try
      {
      // mark only running, not submitted
      flowStats.markStartedThenRunning();

      fireOnStarting();

      if( LOG.isInfoEnabled() )
        {
        logInfo( "starting" );

        for( Tap source : getSourcesCollection() )
          logInfo( " source: " + source );
        for( Tap sink : getSinksCollection() )
          logInfo( " sink: " + sink );
        }

      // if jobs are run local, then only use one thread to force execution serially
      //int numThreads = jobsAreLocal() ? 1 : getMaxConcurrentSteps( getJobConf() );
      int numThreads = getMaxNumParallelSteps();

      if( numThreads == 0 )
        numThreads = jobsMap.size();

      if( numThreads == 0 )
        throw new IllegalStateException( "no jobs rendered for flow: " + getName() );

      if( LOG.isInfoEnabled() )
        {
        logInfo( " parallel execution is enabled: " + ( getMaxNumParallelSteps() != 1 ) );
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
      internalClean( stop );

      handleThrowableAndMarkFailed();

      if( !stop && !flowStats.isFinished() )
        flowStats.markSuccessful();

      try
        {
        fireOnCompleted();
        }
      finally
        {
        flowStats.cleanup();
        internalShutdown();
        }
      }
    }

  protected abstract int getMaxNumParallelSteps();

  protected abstract void internalShutdown();

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

  public synchronized Map<String, Callable<Throwable>> getJobsMap()
    {
    return jobsMap;
    }

  protected synchronized void initializeNewJobsMap()
    {
    // keep topo order
    jobsMap = new LinkedHashMap<String, Callable<Throwable>>();
    TopologicalOrderIterator topoIterator = stepGraph.getTopologicalIterator();

    while( topoIterator.hasNext() )
      {
      FlowStep step = (FlowStep) topoIterator.next();
      FlowStepJob flowStepJob = step.getFlowStepJob( getFlowProcess(), getConfig() );

      jobsMap.put( step.getName(), flowStepJob );

      List<FlowStepJob> predecessors = new ArrayList<FlowStepJob>();

      for( FlowStep flowStep : Graphs.predecessorListOf( stepGraph, step ) )
        predecessors.add( (FlowStepJob) jobsMap.get( flowStep.getName() ) );

      flowStepJob.setPredecessors( predecessors );

      flowStats.addStepStats( flowStepJob.getStepStats() );
      }
    }

  protected void internalStopAllJobs()
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

  protected void handleExecutorShutdown()
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

  protected void fireOnStopping()
    {
    if( hasListeners() )
      {
      if( LOG.isDebugEnabled() )
        logDebug( "firing onStopping event: " + getListeners().size() );

      for( FlowListener flowListener : getListeners() )
        flowListener.onStopping( this );
      }
    }

  protected void fireOnStarting()
    {
    if( hasListeners() )
      {
      if( LOG.isDebugEnabled() )
        logDebug( "firing onStarting event: " + getListeners().size() );

      for( FlowListener flowListener : getListeners() )
        flowListener.onStarting( this );
      }
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

  protected void logInfo( String message )
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

  public void setCascade( Cascade cascade )
    {
    setConfigProperty( getConfig(), "cascading.cascade.id", cascade.getID() );
    flowStats.recordInfo();
    }

  public String getCascadeID()
    {
    return getProperty( "cascading.cascade.id" );
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
   * can be caught by the calling Thread. Since Flow is asynchronous, much of the work is done in the run() method
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
      if( object instanceof Flow.SafeFlowListener )
        return flowListener.equals( ( (Flow.SafeFlowListener) object ).flowListener );

      return flowListener.equals( object );
      }

    public int hashCode()
      {
      return flowListener.hashCode();
      }
    }
  }
