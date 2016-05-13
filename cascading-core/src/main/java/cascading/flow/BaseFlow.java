/*
 * Copyright (c) 2007-2016 Concurrent, Inc. All Rights Reserved.
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import cascading.CascadingException;
import cascading.cascade.Cascade;
import cascading.flow.planner.BaseFlowNode;
import cascading.flow.planner.BaseFlowStep;
import cascading.flow.planner.FlowStepJob;
import cascading.flow.planner.PlannerInfo;
import cascading.flow.planner.PlatformInfo;
import cascading.flow.planner.graph.ElementGraphs;
import cascading.flow.planner.graph.FlowElementGraph;
import cascading.flow.planner.process.FlowStepGraph;
import cascading.flow.planner.process.ProcessGraphs;
import cascading.management.CascadingServices;
import cascading.management.UnitOfWorkExecutorStrategy;
import cascading.management.UnitOfWorkSpawnStrategy;
import cascading.management.state.ClientState;
import cascading.property.AppProps;
import cascading.property.PropertyUtil;
import cascading.stats.FlowStats;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import cascading.util.ProcessLogger;
import cascading.util.ShutdownUtil;
import cascading.util.Update;
import cascading.util.Util;
import cascading.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import riffle.process.DependencyIncoming;
import riffle.process.DependencyOutgoing;
import riffle.process.ProcessCleanup;
import riffle.process.ProcessComplete;
import riffle.process.ProcessPrepare;
import riffle.process.ProcessStart;
import riffle.process.ProcessStop;

import static cascading.util.Util.formatDurationFromMillis;

@riffle.process.Process
public abstract class BaseFlow<Config> implements Flow<Config>, ProcessLogger
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( Flow.class ); // wrapped by ProcessLogger interface methods
  private static final int LOG_FLOW_NAME_MAX = 25;

  private PlannerInfo plannerInfo = PlannerInfo.NULL;
  protected PlatformInfo platformInfo = PlatformInfo.NULL;

  /** Field id */
  private String id;
  /** Field name */
  private String name;
  /** Fields runID */
  private String runID;
  /** Fields classpath */
  private List<String> classPath; // may remain null
  /** Field tags */
  private String tags;
  /** Field listeners */
  private List<SafeFlowListener> listeners;
  /** Field skipStrategy */
  private FlowSkipStrategy flowSkipStrategy = new FlowSkipIfSinkNotStale();
  /** Field flowStats */
  protected FlowStats flowStats; // don't use a listener to set values
  /** Field sources */
  protected Map<String, Tap> sources = Collections.emptyMap();
  /** Field sinks */
  protected Map<String, Tap> sinks = Collections.emptyMap();
  /** Field traps */
  private Map<String, Tap> traps = Collections.emptyMap();
  /** Field checkpoints */
  private Map<String, Tap> checkpoints = Collections.emptyMap();
  /** Field stopJobsOnExit */
  protected boolean stopJobsOnExit = true;
  /** Field submitPriority */
  private int submitPriority = 5;

  /** Field stepGraph */
  protected FlowStepGraph flowStepGraph;
  /** Field thread */
  protected transient Thread thread;
  /** Field throwable */
  private Throwable throwable;
  /** Field stop */
  protected boolean stop;

  /** Field flowCanonicalHash */
  protected String flowCanonicalHash;
  /** Field flowElementGraph */
  protected FlowElementGraph flowElementGraph; // only used for documentation purposes

  private transient CascadingServices cascadingServices;

  private FlowStepStrategy<Config> flowStepStrategy = null;
  /** Field steps */
  protected transient List<FlowStep<Config>> steps;
  /** Field jobsMap */
  private transient Map<String, FlowStepJob<Config>> jobsMap;
  private transient UnitOfWorkSpawnStrategy spawnStrategy = new UnitOfWorkExecutorStrategy();

  private transient ReentrantLock stopLock = new ReentrantLock( true );
  protected ShutdownUtil.Hook shutdownHook;

  protected HashMap<String, String> flowDescriptor;

  /**
   * Returns property stopJobsOnExit.
   *
   * @param properties of type Map
   * @return a boolean
   */
  static boolean getStopJobsOnExit( Map<Object, Object> properties )
    {
    return Boolean.parseBoolean( PropertyUtil.getProperty( properties, FlowProps.STOP_JOBS_ON_EXIT, "true" ) );
    }

  /** Used for testing. */
  protected BaseFlow()
    {
    this.name = "NA";
    this.flowStats = createPrepareFlowStats();
    }

  /**
   * Does not initialize stats
   *
   * @param name
   */
  protected BaseFlow( PlatformInfo platformInfo, String name )
    {
    if( platformInfo != null )
      this.platformInfo = platformInfo;

    this.name = name;
    }

  protected BaseFlow( PlatformInfo platformInfo, Map<Object, Object> properties, Config defaultConfig, String name, Map<String, String> flowDescriptor )
    {
    if( platformInfo != null )
      this.platformInfo = platformInfo;

    this.name = name;

    if( flowDescriptor != null )
      this.flowDescriptor = new LinkedHashMap<>( flowDescriptor );

    addSessionProperties( properties );
    initConfig( properties, defaultConfig );
    }

  protected BaseFlow( PlatformInfo platformInfo, Map<Object, Object> properties, Config defaultConfig, FlowDef flowDef )
    {
    properties = PropertyUtil.asFlatMap( properties );

    if( platformInfo != null )
      this.platformInfo = platformInfo;

    this.name = flowDef.getName();
    this.tags = flowDef.getTags();
    this.runID = flowDef.getRunID();
    this.classPath = flowDef.getClassPath();

    if( !flowDef.getFlowDescriptor().isEmpty() )
      this.flowDescriptor = new LinkedHashMap<>( flowDef.getFlowDescriptor() );

    addSessionProperties( properties );
    initConfig( properties, defaultConfig );
    setSources( flowDef.getSourcesCopy() );
    setSinks( flowDef.getSinksCopy() );
    setTraps( flowDef.getTrapsCopy() );
    setCheckpoints( flowDef.getCheckpointsCopy() );
    initFromTaps();

    retrieveSourceFields();
    retrieveSinkFields();
    }

  public void setPlannerInfo( PlannerInfo plannerInfo )
    {
    this.plannerInfo = plannerInfo;
    }

  @Override
  public PlannerInfo getPlannerInfo()
    {
    return plannerInfo;
    }

  @Override
  public PlatformInfo getPlatformInfo()
    {
    return platformInfo;
    }

  public void initialize( FlowElementGraph flowElementGraph, FlowStepGraph flowStepGraph )
    {
    addPlannerProperties();
    this.flowElementGraph = flowElementGraph;
    this.flowStepGraph = flowStepGraph;

    initSteps();

    this.flowStats = createPrepareFlowStats();

    initializeNewJobsMap();

    initializeChildStats();
    }

  public FlowElementGraph updateSchemes( FlowElementGraph pipeGraph )
    {
    presentSourceFields( pipeGraph );

    presentSinkFields( pipeGraph );

    return new FlowElementGraph( pipeGraph );
    }

  /** Force a Scheme to fetch any fields from a meta-data store */
  protected void retrieveSourceFields()
    {
    for( Tap tap : sources.values() )
      tap.retrieveSourceFields( getFlowProcess() );
    }

  /**
   * Present the current resolved fields for the Tap
   *
   * @param pipeGraph
   */
  protected void presentSourceFields( FlowElementGraph pipeGraph )
    {
    for( Tap tap : sources.values() )
      {
      if( pipeGraph.containsVertex( tap ) )
        tap.presentSourceFields( getFlowProcess(), getFieldsFor( pipeGraph, tap ) );
      }

    for( Tap tap : checkpoints.values() )
      {
      if( pipeGraph.containsVertex( tap ) )
        tap.presentSourceFields( getFlowProcess(), getFieldsFor( pipeGraph, tap ) );
      }
    }

  /** Force a Scheme to fetch any fields from a meta-data store */
  protected void retrieveSinkFields()
    {
    for( Tap tap : sinks.values() )
      tap.retrieveSinkFields( getFlowProcess() );
    }

  /**
   * Present the current resolved fields for the Tap
   *
   * @param pipeGraph
   */
  protected void presentSinkFields( FlowElementGraph pipeGraph )
    {
    for( Tap tap : sinks.values() )
      {
      if( pipeGraph.containsVertex( tap ) )
        tap.presentSinkFields( getFlowProcess(), getFieldsFor( pipeGraph, tap ) );
      }

    for( Tap tap : checkpoints.values() )
      {
      if( pipeGraph.containsVertex( tap ) )
        tap.presentSinkFields( getFlowProcess(), getFieldsFor( pipeGraph, tap ) );
      }
    }

  protected Fields getFieldsFor( FlowElementGraph pipeGraph, Tap tap )
    {
    return pipeGraph.outgoingEdgesOf( tap ).iterator().next().getOutValuesFields();
    }

  protected void addSessionProperties( Map<Object, Object> properties )
    {
    if( properties == null )
      return;

    PropertyUtil.setProperty( properties, CASCADING_FLOW_ID, getID() );
    PropertyUtil.setProperty( properties, "cascading.flow.tags", getTags() );
    AppProps.setApplicationID( properties );
    PropertyUtil.setProperty( properties, "cascading.app.name", makeAppName( properties ) );
    PropertyUtil.setProperty( properties, "cascading.app.version", makeAppVersion( properties ) );
    }

  protected void addPlannerProperties()
    {
    setConfigProperty( getConfig(), "cascading.flow.planner", getPlannerInfo().name );
    setConfigProperty( getConfig(), "cascading.flow.platform", getPlannerInfo().platform );
    setConfigProperty( getConfig(), "cascading.flow.registry", getPlannerInfo().registry );
    }

  private String makeAppName( Map<Object, Object> properties )
    {
    if( properties == null )
      return null;

    String name = AppProps.getApplicationName( properties );

    if( name != null )
      return name;

    return Util.findName( AppProps.getApplicationJarPath( properties ) );
    }

  private String makeAppVersion( Map<Object, Object> properties )
    {
    if( properties == null )
      return null;

    String name = AppProps.getApplicationVersion( properties );

    if( name != null )
      return name;

    return Util.findVersion( AppProps.getApplicationJarPath( properties ) );
    }

  protected FlowStats createPrepareFlowStats()
    {
    FlowStats flowStats = createFlowStats();

    flowStats.prepare();
    flowStats.markPending();

    return flowStats;
    }

  protected FlowStats createFlowStats()
    {
    return new FlowStats( this, getClientState() );
    }

  public CascadingServices getCascadingServices()
    {
    if( cascadingServices == null )
      cascadingServices = new CascadingServices( getConfigAsProperties() );

    return cascadingServices;
    }

  protected ClientState getClientState()
    {
    return getFlowSession().getCascadingServices().createClientState( getID() );
    }

  protected void initSteps()
    {
    if( flowStepGraph == null )
      return;

    Set<FlowStep> flowSteps = flowStepGraph.vertexSet();

    for( FlowStep flowStep : flowSteps )
      {
      ( (BaseFlowStep) flowStep ).setFlow( this );

      Set<FlowNode> flowNodes = flowStep.getFlowNodeGraph().vertexSet();

      for( FlowNode flowNode : flowNodes )
        ( (BaseFlowNode) flowNode ).setFlowStep( flowStep );
      }
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

  @Override
  public String getName()
    {
    return name;
    }

  protected void setName( String name )
    {
    this.name = name;
    }

  @Override
  public String getID()
    {
    if( id == null )
      id = Util.createUniqueID();

    return id;
    }

  @Override
  public String getTags()
    {
    return tags;
    }

  @Override
  public int getSubmitPriority()
    {
    return submitPriority;
    }

  @Override
  public void setSubmitPriority( int submitPriority )
    {
    if( submitPriority < 1 || submitPriority > 10 )
      throw new IllegalArgumentException( "submitPriority must be between 1 and 10 inclusive, was: " + submitPriority );

    this.submitPriority = submitPriority;
    }

  /**
   * The hash value can be used to determine if two unique Flow instances performed the same work.
   * <p/>
   * The source and sink taps are not relevant to the hash.
   *
   * @return a String value
   */
  public String getFlowCanonicalHash()
    {
    if( flowCanonicalHash != null || flowElementGraph == null )
      return flowCanonicalHash;

    // synchronize on flowElementGraph to prevent duplicate hash creation - can be high overhead
    // and to prevent deadlocks if the DocumentService calls back into the flow when transitioning
    // into a running state
    synchronized( flowElementGraph )
      {
      flowCanonicalHash = createFlowCanonicalHash( flowElementGraph );
      }

    return flowCanonicalHash;
    }

  // allow for sub-class overrides
  protected String createFlowCanonicalHash( FlowElementGraph flowElementGraph )
    {
    if( flowElementGraph == null )
      return null;

    return ElementGraphs.canonicalHash( flowElementGraph );
    }

  FlowElementGraph getFlowElementGraph()
    {
    return flowElementGraph;
    }

  FlowStepGraph getFlowStepGraph()
    {
    return flowStepGraph;
    }

  protected void setSources( Map<String, Tap> sources )
    {
    if( sources == null )
      return;

    addListeners( sources.values() );
    this.sources = sources;
    }

  protected void setSinks( Map<String, Tap> sinks )
    {
    if( sinks == null )
      return;

    addListeners( sinks.values() );
    this.sinks = sinks;
    }

  protected void setTraps( Map<String, Tap> traps )
    {
    addListeners( traps.values() );
    this.traps = traps;
    }

  protected void setCheckpoints( Map<String, Tap> checkpoints )
    {
    addListeners( checkpoints.values() );
    this.checkpoints = checkpoints;
    }

  protected void setFlowStepGraph( FlowStepGraph flowStepGraph )
    {
    this.flowStepGraph = flowStepGraph;
    }

  /**
   * This method creates a new internal Config with the parentConfig as defaults using the properties to override
   * the defaults.
   *
   * @param properties   of type Map
   * @param parentConfig of type Config
   */
  protected abstract void initConfig( Map<Object, Object> properties, Config parentConfig );

  public Config createConfig( Map<Object, Object> properties, Config defaultConfig )
    {
    Config config = newConfig( defaultConfig );

    if( properties == null )
      return config;

    Set<Object> keys = new HashSet<>( properties.keySet() );

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

  protected void initFromProperties( Map<Object, Object> properties )
    {
    stopJobsOnExit = getStopJobsOnExit( properties );
    }

  public FlowSession getFlowSession()
    {
    return new FlowSession( getCascadingServices() );
    }

  @Override
  public FlowStats getFlowStats()
    {
    return flowStats;
    }

  @Override
  public Map<String, String> getFlowDescriptor()
    {
    if( flowDescriptor == null )
      return Collections.emptyMap();

    return Collections.unmodifiableMap( flowDescriptor );
    }

  @Override
  public FlowStats getStats()
    {
    return getFlowStats();
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

  @Override
  public boolean hasListeners()
    {
    return listeners != null && !listeners.isEmpty();
    }

  @Override
  public void addListener( FlowListener flowListener )
    {
    getListeners().add( new SafeFlowListener( flowListener ) );
    }

  @Override
  public boolean removeListener( FlowListener flowListener )
    {
    return getListeners().remove( new SafeFlowListener( flowListener ) );
    }

  @Override
  public boolean hasStepListeners()
    {
    boolean hasStepListeners = false;

    for( FlowStep step : getFlowSteps() )
      hasStepListeners |= step.hasListeners();

    return hasStepListeners;
    }

  @Override
  public void addStepListener( FlowStepListener flowStepListener )
    {
    for( FlowStep step : getFlowSteps() )
      step.addListener( flowStepListener );
    }

  @Override
  public boolean removeStepListener( FlowStepListener flowStepListener )
    {
    boolean listenerRemoved = true;

    for( FlowStep step : getFlowSteps() )
      listenerRemoved &= step.removeListener( flowStepListener );

    return listenerRemoved;
    }

  @Override
  public Map<String, Tap> getSources()
    {
    return Collections.unmodifiableMap( sources );
    }

  @Override
  public List<String> getSourceNames()
    {
    return new ArrayList<String>( sources.keySet() );
    }

  @Override
  public Tap getSource( String name )
    {
    return sources.get( name );
    }

  @Override
  @DependencyIncoming
  public Collection<Tap> getSourcesCollection()
    {
    return getSources().values();
    }

  @Override
  public Map<String, Tap> getSinks()
    {
    return Collections.unmodifiableMap( sinks );
    }

  @Override
  public List<String> getSinkNames()
    {
    return new ArrayList<String>( sinks.keySet() );
    }

  @Override
  public Tap getSink( String name )
    {
    return sinks.get( name );
    }

  @Override
  @DependencyOutgoing
  public Collection<Tap> getSinksCollection()
    {
    return getSinks().values();
    }

  @Override
  public Tap getSink()
    {
    return sinks.values().iterator().next();
    }

  @Override
  public Map<String, Tap> getTraps()
    {
    return Collections.unmodifiableMap( traps );
    }

  @Override
  public List<String> getTrapNames()
    {
    return new ArrayList<String>( traps.keySet() );
    }

  @Override
  public Collection<Tap> getTrapsCollection()
    {
    return getTraps().values();
    }

  @Override
  public Map<String, Tap> getCheckpoints()
    {
    return Collections.unmodifiableMap( checkpoints );
    }

  @Override
  public List<String> getCheckpointNames()
    {
    return new ArrayList<String>( checkpoints.keySet() );
    }

  @Override
  public Collection<Tap> getCheckpointsCollection()
    {
    return getCheckpoints().values();
    }

  @Override
  public boolean isStopJobsOnExit()
    {
    return stopJobsOnExit;
    }

  @Override
  public FlowSkipStrategy getFlowSkipStrategy()
    {
    return flowSkipStrategy;
    }

  @Override
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

  @Override
  public boolean isSkipFlow() throws IOException
    {
    return flowSkipStrategy.skipFlow( this );
    }

  @Override
  public boolean areSinksStale() throws IOException
    {
    return areSourcesNewer( getSinkModified() );
    }

  @Override
  public boolean areSourcesNewer( long sinkModified ) throws IOException
    {
    Config config = getConfig();
    Iterator<Tap> values = sources.values().iterator();

    long sourceModified = 0;

    try
      {
      sourceModified = Util.getSourceModified( config, values, sinkModified );

      if( sinkModified < sourceModified )
        return true;

      return false;
      }
    finally
      {
      if( LOG.isInfoEnabled() )
        logInfo( "source modification date at: " + new Date( sourceModified ) ); // not oldest, we didnt check them all
      }
    }

  @Override
  public long getSinkModified() throws IOException
    {
    long sinkModified = Util.getSinkModified( getConfig(), sinks.values() );

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

  @Override
  public FlowStepStrategy getFlowStepStrategy()
    {
    return flowStepStrategy;
    }

  @Override
  public void setFlowStepStrategy( FlowStepStrategy flowStepStrategy )
    {
    this.flowStepStrategy = flowStepStrategy;
    }

  @Override
  public List<FlowStep<Config>> getFlowSteps()
    {
    if( steps != null )
      return steps;

    if( flowStepGraph == null )
      return Collections.emptyList();

    Iterator<FlowStep> topoIterator = flowStepGraph.getTopologicalIterator();

    steps = new ArrayList<>();

    while( topoIterator.hasNext() )
      steps.add( topoIterator.next() );

    return steps;
    }

  @Override
  @ProcessPrepare
  public void prepare()
    {
    try
      {
      deleteSinksIfNotUpdate();
      deleteTrapsIfNotUpdate();
      deleteCheckpointsIfNotUpdate();
      }
    catch( IOException exception )
      {
      throw new FlowException( "unable to prepare flow", exception );
      }
    }

  @Override
  @ProcessStart
  public synchronized void start()
    {
    if( thread != null )
      return;

    if( stop )
      return;

    registerShutdownHook();

    internalStart();

    String threadName = ( "flow " + Util.toNull( getName() ) ).trim();

    thread = createFlowThread( threadName );

    thread.start();
    }

  protected Thread createFlowThread( String threadName )
    {
    return new Thread( new Runnable()
      {
      @Override
      public void run()
        {
        BaseFlow.this.run();
        }
      }, threadName );
    }

  protected abstract void internalStart();

  @Override
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

  protected abstract void internalClean( boolean stop );

  @Override
  @ProcessComplete
  public void complete()
    {
    start();

    try
      {
      try
        {
        synchronized( this ) // prevent NPE on quick stop() & complete() after start()
          {
          while( thread == null && !stop )
            Util.safeSleep( 10 );
          }

        if( thread != null )
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

      if( throwable instanceof OutOfMemoryError )
        throw (OutOfMemoryError) throwable;

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
      }
    }

  private void commitTraps()
    {
    // commit all the traps, don't fail on an error
    for( Tap tap : traps.values() )
      {
      try
        {
        if( !tap.commitResource( getConfig() ) )
          logError( "unable to commit trap: " + tap.getFullIdentifier( getConfig() ) );
        }
      catch( IOException exception )
        {
        logError( "unable to commit trap: " + tap.getFullIdentifier( getConfig() ), exception );
        }
      }
    }

  @Override
  @ProcessCleanup
  public void cleanup()
    {
    // do nothing
    }

  @Override
  public TupleEntryIterator openSource() throws IOException
    {
    return sources.values().iterator().next().openForRead( getFlowProcess() );
    }

  @Override
  public TupleEntryIterator openSource( String name ) throws IOException
    {
    if( !sources.containsKey( name ) )
      throw new IllegalArgumentException( "source does not exist: " + name );

    return sources.get( name ).openForRead( getFlowProcess() );
    }

  @Override
  public TupleEntryIterator openSink() throws IOException
    {
    return sinks.values().iterator().next().openForRead( getFlowProcess() );
    }

  @Override
  public TupleEntryIterator openSink( String name ) throws IOException
    {
    if( !sinks.containsKey( name ) )
      throw new IllegalArgumentException( "sink does not exist: " + name );

    return sinks.get( name ).openForRead( getFlowProcess() );
    }

  @Override
  public TupleEntryIterator openTrap() throws IOException
    {
    return traps.values().iterator().next().openForRead( getFlowProcess() );
    }

  @Override
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
   * @see BaseFlow#deleteSinksIfNotUpdate()
   */
  public void deleteSinks() throws IOException
    {
    for( Tap tap : sinks.values() )
      deleteOrFail( tap );
    }

  private void deleteOrFail( Tap tap ) throws IOException
    {
    if( !tap.resourceExists( getConfig() ) )
      return;

    if( !tap.deleteResource( getConfig() ) )
      throw new FlowException( "unable to delete resource: " + tap.getFullIdentifier( getFlowProcess() ) );
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
        deleteOrFail( tap );
      }
    }

  public void deleteSinksIfReplace() throws IOException
    {
    for( Tap tap : sinks.values() )
      {
      if( tap.isReplace() )
        deleteOrFail( tap );
      }
    }

  public void deleteTrapsIfNotUpdate() throws IOException
    {
    for( Tap tap : traps.values() )
      {
      if( !tap.isUpdate() )
        deleteOrFail( tap );
      }
    }

  public void deleteCheckpointsIfNotUpdate() throws IOException
    {
    for( Tap tap : checkpoints.values() )
      {
      if( !tap.isUpdate() )
        deleteOrFail( tap );
      }
    }

  public void deleteTrapsIfReplace() throws IOException
    {
    for( Tap tap : traps.values() )
      {
      if( tap.isReplace() )
        deleteOrFail( tap );
      }
    }

  public void deleteCheckpointsIfReplace() throws IOException
    {
    for( Tap tap : checkpoints.values() )
      {
      if( tap.isReplace() )
        deleteOrFail( tap );
      }
    }

  @Override
  public boolean resourceExists( Tap tap ) throws IOException
    {
    return tap.resourceExists( getConfig() );
    }

  @Override
  public TupleEntryIterator openTapForRead( Tap tap ) throws IOException
    {
    return tap.openForRead( getFlowProcess() );
    }

  @Override
  public TupleEntryCollector openTapForWrite( Tap tap ) throws IOException
    {
    return tap.openForWrite( getFlowProcess() );
    }

  /** Method run implements the Runnable run method and should not be called by users. */
  private void run()
    {
    if( thread == null )
      throw new IllegalStateException( "to start a Flow call start() or complete(), not Runnable#run()" );

    Version.printBanner();
    Update.checkForUpdate( getPlatformInfo() );

    try
      {
      if( stop )
        return;

      flowStats.markStarted();

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
        logInfo( " parallel execution of steps is enabled: " + ( getMaxNumParallelSteps() != 1 ) );
        logInfo( " executing total steps: " + jobsMap.size() );
        logInfo( " allocating management threads: " + numThreads );
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
      handleThrowableAndMarkFailed();

      if( !stop && !flowStats.isFinished() )
        flowStats.markSuccessful();

      internalClean( stop ); // cleaning temp taps may be determined by success/failure

      commitTraps();

      try
        {
        fireOnCompleted();
        }
      finally
        {
        if( LOG.isInfoEnabled() )
          {
          long totalSliceCPUSeconds = getTotalSliceCPUMilliSeconds();

          if( totalSliceCPUSeconds == -1 )
            logInfo( " completed in: " + formatDurationFromMillis( flowStats.getDuration() ) );
          else
            logInfo( " completed in: " + formatDurationFromMillis( flowStats.getDuration() ) + ", using cpu time: " + formatDurationFromMillis( totalSliceCPUSeconds ) );
          }

        flowStats.cleanup();
        internalShutdown();
        deregisterShutdownHook();
        }
      }
    }

  protected long getTotalSliceCPUMilliSeconds()
    {
    return -1;
    }

  protected abstract int getMaxNumParallelSteps();

  protected abstract void internalShutdown();

  private List<Future<Throwable>> spawnJobs( int numThreads ) throws InterruptedException
    {
    if( spawnStrategy == null )
      {
      logError( "no spawnStrategy set" );
      return new ArrayList<>();
      }

    if( stop )
      return new ArrayList<>();

    List<Callable<Throwable>> list = new ArrayList<>();

    for( FlowStepJob<Config> job : jobsMap.values() )
      list.add( job );

    return spawnStrategy.start( this, numThreads, list );
    }

  private void handleThrowableAndMarkFailed()
    {
    if( throwable != null && !stop )
      {
      flowStats.markFailed( throwable );

      fireOnThrowable();
      }
    }

  Map<String, FlowStepJob<Config>> getJobsMap()
    {
    return jobsMap;
    }

  protected void initializeNewJobsMap()
    {
    jobsMap = new LinkedHashMap<>(); // keep topo order
    Iterator<FlowStep> topoIterator = flowStepGraph.getTopologicalIterator();

    while( topoIterator.hasNext() )
      {
      BaseFlowStep<Config> step = (BaseFlowStep) topoIterator.next();
      FlowStepJob<Config> flowStepJob = step.getCreateFlowStepJob( getFlowProcess(), getConfig() );

      jobsMap.put( step.getID(), flowStepJob );

      List<FlowStepJob<Config>> predecessors = new ArrayList<FlowStepJob<Config>>();

      for( Object flowStep : ProcessGraphs.predecessorListOf( flowStepGraph, step ) )
        predecessors.add( jobsMap.get( ( (FlowStep) flowStep ).getID() ) );

      flowStepJob.setPredecessors( predecessors );
      }
    }

  protected void initializeChildStats()
    {
    for( FlowStepJob<Config> flowStepJob : jobsMap.values() )
      flowStats.addStepStats( flowStepJob.getStepStats() );
    }

  protected void internalStopAllJobs()
    {
    logInfo( "stopping all jobs" );

    try
      {
      if( jobsMap == null )
        return;

      List<FlowStepJob<Config>> jobs = new ArrayList<FlowStepJob<Config>>( jobsMap.values() );

      Collections.reverse( jobs );

      for( FlowStepJob<Config> job : jobs )
        job.stop();
      }
    finally
      {
      logInfo( "stopped all jobs" );
      }
    }

  protected void handleExecutorShutdown()
    {
    if( spawnStrategy == null )
      return;

    if( spawnStrategy.isCompleted( this ) )
      return;

    logDebug( "shutting down job executor" );

    try
      {
      spawnStrategy.complete( this, 5 * 60, TimeUnit.SECONDS );
      }
    catch( InterruptedException exception )
      {
      // ignore
      }

    logDebug( "shutdown of job executor complete" );
    }

  protected void fireOnCompleted()
    {
    if( hasListeners() )
      {
      if( isDebugEnabled() )
        logDebug( "firing onCompleted event: " + getListeners().size() );

      for( FlowListener flowListener : getListeners() )
        flowListener.onCompleted( this );
      }
    }

  protected void fireOnThrowable( Throwable throwable )
    {
    this.throwable = throwable;
    fireOnThrowable();
    }

  protected void fireOnThrowable()
    {
    if( hasListeners() )
      {
      if( isDebugEnabled() )
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
      if( isDebugEnabled() )
        logDebug( "firing onStopping event: " + getListeners().size() );

      for( FlowListener flowListener : getListeners() )
        flowListener.onStopping( this );
      }
    }

  protected void fireOnStarting()
    {
    if( hasListeners() )
      {
      if( isDebugEnabled() )
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

    for( FlowStep step : getFlowSteps() )
      buffer.append( step );

    return buffer.toString();
    }

  @Override
  public final boolean isInfoEnabled()
    {
    return LOG.isInfoEnabled();
    }

  @Override
  public final boolean isDebugEnabled()
    {
    return LOG.isDebugEnabled();
    }

  @Override
  public void logInfo( String message, Object... arguments )
    {
    LOG.info( "[" + Util.truncate( getName(), LOG_FLOW_NAME_MAX ) + "] " + message, arguments );
    }

  @Override
  public void logDebug( String message, Object... arguments )
    {
    LOG.debug( "[" + Util.truncate( getName(), LOG_FLOW_NAME_MAX ) + "] " + message, arguments );
    }

  @Override
  public void logWarn( String message )
    {
    LOG.warn( "[" + Util.truncate( getName(), LOG_FLOW_NAME_MAX ) + "] " + message );
    }

  @Override
  public void logWarn( String message, Throwable throwable )
    {
    LOG.warn( "[" + Util.truncate( getName(), LOG_FLOW_NAME_MAX ) + "] " + message, throwable );
    }

  @Override
  public void logWarn( String message, Object... arguments )
    {
    LOG.warn( "[" + Util.truncate( getName(), LOG_FLOW_NAME_MAX ) + "] " + message, arguments );
    }

  @Override
  public void logError( String message, Object... arguments )
    {
    LOG.error( "[" + Util.truncate( getName(), LOG_FLOW_NAME_MAX ) + "] " + message, arguments );
    }

  @Override
  public void logError( String message, Throwable throwable )
    {
    LOG.error( "[" + Util.truncate( getName(), LOG_FLOW_NAME_MAX ) + "] " + message, throwable );
    }

  @Override
  public void writeDOT( String filename )
    {
    if( flowElementGraph == null )
      throw new UnsupportedOperationException( "this flow instance cannot write a DOT file" );

    flowElementGraph.writeDOT( filename );
    }

  @Override
  public void writeStepsDOT( String filename )
    {
    if( flowStepGraph == null )
      throw new UnsupportedOperationException( "this flow instance cannot write a DOT file" );

    flowStepGraph.writeDOT( filename );
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

  @Override
  public String getCascadeID()
    {
    return getProperty( "cascading.cascade.id" );
    }

  @Override
  public String getRunID()
    {
    return runID;
    }

  public List<String> getClassPath()
    {
    return classPath;
    }

  @Override
  public void setSpawnStrategy( UnitOfWorkSpawnStrategy spawnStrategy )
    {
    this.spawnStrategy = spawnStrategy;
    }

  @Override
  public UnitOfWorkSpawnStrategy getSpawnStrategy()
    {
    return spawnStrategy;
    }

  protected void registerShutdownHook()
    {
    if( !isStopJobsOnExit() )
      return;

    shutdownHook = new ShutdownUtil.Hook()
      {
      @Override
      public Priority priority()
        {
        return Priority.WORK_CHILD;
        }

      @Override
      public void execute()
        {
        logInfo( "shutdown hook calling stop on flow" );

        BaseFlow.this.stop();
        }
      };

    ShutdownUtil.addHook( shutdownHook );
    }

  private void deregisterShutdownHook()
    {
    if( !isStopJobsOnExit() || stop )
      return;

    ShutdownUtil.removeHook( shutdownHook );
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
      if( object instanceof BaseFlow.SafeFlowListener )
        return flowListener.equals( ( (BaseFlow.SafeFlowListener) object ).flowListener );

      return flowListener.equals( object );
      }

    public int hashCode()
      {
      return flowListener.hashCode();
      }
    }
  }
