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

package cascading.cascade;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import cascading.CascadingException;
import cascading.cascade.planner.FlowGraph;
import cascading.cascade.planner.IdentifierGraph;
import cascading.cascade.planner.TapGraph;
import cascading.flow.BaseFlow;
import cascading.flow.Flow;
import cascading.flow.FlowException;
import cascading.flow.FlowSkipStrategy;
import cascading.management.CascadingServices;
import cascading.management.UnitOfWork;
import cascading.management.UnitOfWorkExecutorStrategy;
import cascading.management.UnitOfWorkSpawnStrategy;
import cascading.management.state.ClientState;
import cascading.stats.CascadeStats;
import cascading.tap.Tap;
import cascading.util.ShutdownUtil;
import cascading.util.Util;
import cascading.util.Version;
import org.jgrapht.Graphs;
import org.jgrapht.ext.EdgeNameProvider;
import org.jgrapht.ext.IntegerNameProvider;
import org.jgrapht.ext.VertexNameProvider;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.jgrapht.traverse.TopologicalOrderIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cascading.property.PropertyUtil.getProperty;

/**
 * A Cascade is an assembly of {@link cascading.flow.Flow} instances that share or depend on equivalent {@link Tap} instances and are executed as
 * a single group. The most common case is where one Flow instance depends on a Tap created by a second Flow instance. This
 * dependency chain can continue as practical.
 * <p/>
 * Note Flow instances that have no shared dependencies will be executed in parallel.
 * <p/>
 * Additionally, a Cascade allows for incremental builds of complex data processing processes. If a given source {@link Tap} is newer than
 * a subsequent sink {@link Tap} in the assembly, the connecting {@link cascading.flow.Flow}(s) will be executed
 * when the Cascade executed. If all the targets (sinks) are up to date, the Cascade exits immediately and does nothing.
 * <p/>
 * The concept of 'stale' is pluggable, see the {@link cascading.flow.FlowSkipStrategy} class.
 * <p/>
 * When a Cascade starts up, if first verifies which Flow instances have stale sinks, if the sinks are not stale, the
 * method {@link cascading.flow.BaseFlow#deleteSinksIfNotUpdate()} is called. Before appends/updates were supported (logically)
 * the Cascade deleted all the sinks in a Flow.
 * <p/>
 * The new consequence of this is if the Cascade fails, but does complete a Flow that appended or updated data, re-running
 * the Cascade (and the successful append/update Flow) will re-update data to the source. Some systems may be idempotent and
 * may not have any side-effects. So plan accordingly.
 * <p/>
 * Use the {@link CascadeListener} to receive any events on the life-cycle of the Cascade as it executes. Any
 * {@link Tap} instances owned by managed Flows also implementing CascadeListener will automatically be added to the
 * set of listeners.
 *
 * @see CascadeListener
 * @see cascading.flow.Flow
 * @see cascading.flow.FlowSkipStrategy
 */
public class Cascade implements UnitOfWork<CascadeStats>
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( Cascade.class );

  /** Field id */
  private String id;
  /** Field name */
  private final String name;
  /** Field tags */
  private String tags;
  /** Field properties */
  private final Map<Object, Object> properties;
  /** Fields listeners */
  private List<SafeCascadeListener> listeners;
  /** Field jobGraph */
  private final FlowGraph flowGraph;
  /** Field tapGraph */
  private final IdentifierGraph identifierGraph;
  /** Field cascadeStats */
  private final CascadeStats cascadeStats;
  /** Field cascadingServices */
  private CascadingServices cascadingServices;
  /** Field thread */
  private Thread thread;
  /** Field throwable */
  private Throwable throwable;
  private transient UnitOfWorkSpawnStrategy spawnStrategy = new UnitOfWorkExecutorStrategy();
  /** Field shutdownHook */
  private ShutdownUtil.Hook shutdownHook;
  /** Field jobsMap */
  private final Map<String, Callable<Throwable>> jobsMap = new LinkedHashMap<String, Callable<Throwable>>();
  /** Field stop */
  private boolean stop;
  /** Field flowSkipStrategy */
  private FlowSkipStrategy flowSkipStrategy = null;
  /** Field maxConcurrentFlows */
  private int maxConcurrentFlows = 0;

  /** Field tapGraph * */
  private transient TapGraph tapGraph;

  static int getMaxConcurrentFlows( Map<Object, Object> properties, int maxConcurrentFlows )
    {
    if( maxConcurrentFlows != -1 ) // CascadeDef is -1 by default
      return maxConcurrentFlows;

    return Integer.parseInt( getProperty( properties, CascadeProps.MAX_CONCURRENT_FLOWS, "0" ) );
    }

  /** for testing */
  protected Cascade()
    {
    this.name = null;
    this.tags = null;
    this.properties = null;
    this.flowGraph = null;
    this.identifierGraph = null;
    this.cascadeStats = null;
    }

  Cascade( CascadeDef cascadeDef, Map<Object, Object> properties, FlowGraph flowGraph, IdentifierGraph identifierGraph )
    {
    this.name = cascadeDef.getName();
    this.tags = cascadeDef.getTags();
    this.properties = properties;
    this.flowGraph = flowGraph;
    this.identifierGraph = identifierGraph;
    this.cascadeStats = createPrepareCascadeStats();
    setIDOnFlow();
    this.maxConcurrentFlows = cascadeDef.getMaxConcurrentFlows();

    addListeners( getAllTaps() );
    }

  private CascadeStats createPrepareCascadeStats()
    {
    CascadeStats cascadeStats = new CascadeStats( this, getClientState() );

    cascadeStats.prepare();
    cascadeStats.markPending();

    return cascadeStats;
    }

  /**
   * Method getName returns the name of this Cascade object.
   *
   * @return the name (type String) of this Cascade object.
   */
  @Override
  public String getName()
    {
    return name;
    }

  /**
   * Method getID returns the ID of this Cascade object.
   * <p/>
   * The ID value is a long HEX String used to identify this instance globally. Subsequent Cascade
   * instances created with identical parameters will not return the same ID.
   *
   * @return the ID (type String) of this Cascade object.
   */
  @Override
  public String getID()
    {
    if( id == null )
      id = Util.createUniqueID();

    return id;
    }

  /**
   * Method getTags returns the tags associated with this Cascade object.
   *
   * @return the tags (type String) of this Cascade object.
   */
  @Override
  public String getTags()
    {
    return tags;
    }

  void addListeners( Collection listeners )
    {
    for( Object listener : listeners )
      {
      if( listener instanceof CascadeListener )
        addListener( (CascadeListener) listener );
      }
    }

  List<SafeCascadeListener> getListeners()
    {
    if( listeners == null )
      listeners = new LinkedList<SafeCascadeListener>();

    return listeners;
    }

  public boolean hasListeners()
    {
    return listeners != null && !listeners.isEmpty();
    }

  public void addListener( CascadeListener flowListener )
    {
    getListeners().add( new SafeCascadeListener( flowListener ) );
    }

  public boolean removeListener( CascadeListener flowListener )
    {
    return getListeners().remove( new SafeCascadeListener( flowListener ) );
    }

  private void fireOnCompleted()
    {
    if( hasListeners() )
      {
      if( LOG.isDebugEnabled() )
        logDebug( "firing onCompleted event: " + getListeners().size() );

      for( CascadeListener cascadeListener : getListeners() )
        cascadeListener.onCompleted( this );
      }
    }

  private void fireOnThrowable()
    {
    if( hasListeners() )
      {
      if( LOG.isDebugEnabled() )
        logDebug( "firing onThrowable event: " + getListeners().size() );

      boolean isHandled = false;

      for( CascadeListener cascadeListener : getListeners() )
        isHandled = cascadeListener.onThrowable( this, throwable ) || isHandled;

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

      for( CascadeListener cascadeListener : getListeners() )
        cascadeListener.onStopping( this );
      }
    }

  protected void fireOnStarting()
    {
    if( hasListeners() )
      {
      if( LOG.isDebugEnabled() )
        logDebug( "firing onStarting event: " + getListeners().size() );

      for( CascadeListener cascadeListener : getListeners() )
        cascadeListener.onStarting( this );
      }
    }

  private CascadingServices getCascadingServices()
    {
    if( cascadingServices == null )
      cascadingServices = new CascadingServices( properties );

    return cascadingServices;
    }

  private ClientState getClientState()
    {
    return getCascadingServices().createClientState( getID() );
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

  @Override
  public CascadeStats getStats()
    {
    return getCascadeStats();
    }

  private void setIDOnFlow()
    {
    for( Flow<?> flow : getFlows() )
      ( (BaseFlow<?>) flow ).setCascade( this );
    }

  protected FlowGraph getFlowGraph()
    {
    return flowGraph;
    }

  protected IdentifierGraph getIdentifierGraph()
    {
    return identifierGraph;
    }

  /**
   * Method getFlows returns the flows managed by this Cascade object. The returned {@link cascading.flow.Flow} instances
   * will be in topological order.
   *
   * @return the flows (type Collection<Flow>) of this Cascade object.
   */
  public List<Flow> getFlows()
    {
    List<Flow> flows = new LinkedList<Flow>();
    TopologicalOrderIterator<Flow, Integer> topoIterator = flowGraph.getTopologicalIterator();

    while( topoIterator.hasNext() )
      flows.add( topoIterator.next() );

    return flows;
    }

  /**
   * Method findFlows returns a List of flows whose names match the given regex pattern.
   *
   * @param regex of type String
   * @return List<Flow>
   */
  public List<Flow> findFlows( String regex )
    {
    List<Flow> flows = new ArrayList<Flow>();

    for( Flow flow : getFlows() )
      {
      if( flow.getName().matches( regex ) )
        flows.add( flow );
      }

    return flows;
    }

  /**
   * Method getHeadFlows returns all Flow instances that are at the "head" of the flow graph.
   * <p/>
   * That is, they are the first to execute and have no Tap source dependencies with Flow instances in the this Cascade
   * instance.
   *
   * @return Collection<Flow>
   */
  public Collection<Flow> getHeadFlows()
    {
    Set<Flow> flows = new HashSet<Flow>();

    for( Flow flow : flowGraph.vertexSet() )
      {
      if( flowGraph.inDegreeOf( flow ) == 0 )
        flows.add( flow );
      }

    return flows;
    }

  /**
   * Method getTailFlows returns all Flow instances that are at the "tail" of the flow graph.
   * <p/>
   * That is, they are the last to execute and have no Tap sink dependencies with Flow instances in the this Cascade
   * instance.
   *
   * @return Collection<Flow>
   */
  public Collection<Flow> getTailFlows()
    {
    Set<Flow> flows = new HashSet<Flow>();

    for( Flow flow : flowGraph.vertexSet() )
      {
      if( flowGraph.outDegreeOf( flow ) == 0 )
        flows.add( flow );
      }

    return flows;
    }

  /**
   * Method getIntermediateFlows returns all Flow instances that are neither at the "tail" or "tail" of the flow graph.
   *
   * @return Collection<Flow>
   */
  public Collection<Flow> getIntermediateFlows()
    {
    Set<Flow> flows = new HashSet<Flow>( flowGraph.vertexSet() );

    flows.removeAll( getHeadFlows() );
    flows.removeAll( getTailFlows() );

    return flows;
    }

  protected TapGraph getTapGraph()
    {
    if( tapGraph == null )
      tapGraph = new TapGraph( flowGraph.vertexSet() );

    return tapGraph;
    }

  /**
   * Method getSourceTaps returns all source Tap instances in this Cascade instance.
   * <p/>
   * That is, none of returned Tap instances are the sinks of other Flow instances in this Cascade.
   * <p/>
   * All {@link cascading.tap.CompositeTap} instances are unwound if addressed directly by a managed Flow instance.
   *
   * @return Collection<Tap>
   */
  public Collection<Tap> getSourceTaps()
    {
    TapGraph tapGraph = getTapGraph();
    Set<Tap> taps = new HashSet<Tap>();

    for( Tap tap : tapGraph.vertexSet() )
      {
      if( tapGraph.inDegreeOf( tap ) == 0 )
        taps.add( tap );
      }

    return taps;
    }

  /**
   * Method getSinkTaps returns all sink Tap instances in this Cascade instance.
   * <p/>
   * That is, none of returned Tap instances are the sources of other Flow instances in this Cascade.
   * <p/>
   * All {@link cascading.tap.CompositeTap} instances are unwound if addressed directly by a managed Flow instance.
   * <p/>
   * This method will return checkpoint Taps managed by Flow instances if not used as a source by other Flow instances.
   *
   * @return Collection<Tap>
   */
  public Collection<Tap> getSinkTaps()
    {
    TapGraph tapGraph = getTapGraph();
    Set<Tap> taps = new HashSet<Tap>();

    for( Tap tap : tapGraph.vertexSet() )
      {
      if( tapGraph.outDegreeOf( tap ) == 0 )
        taps.add( tap );
      }

    return taps;
    }

  /**
   * Method getCheckpointTaps returns all checkpoint Tap instances from all the Flow instances in this Cascade instance.
   *
   * @return Collection<Tap>
   */
  public Collection<Tap> getCheckpointsTaps()
    {
    Set<Tap> taps = new HashSet<Tap>();

    for( Flow flow : getFlows() )
      taps.addAll( flow.getCheckpointsCollection() );

    return taps;
    }

  /**
   * Method getIntermediateTaps returns all Tap instances that are neither at the source or sink of the flow graph.
   * <p/>
   * This method does consider checkpoint Taps managed by Flow instances in this Cascade instance.
   *
   * @return Collection<Flow>
   */
  public Collection<Tap> getIntermediateTaps()
    {
    TapGraph tapGraph = getTapGraph();
    Set<Tap> taps = new HashSet<Tap>( tapGraph.vertexSet() );

    taps.removeAll( getSourceTaps() );
    taps.removeAll( getSinkTaps() );

    return taps;
    }

  /**
   * Method getAllTaps returns all source, sink, and checkpoint Tap instances associated with the managed
   * Flow instances in this Cascade instance.
   *
   * @return Collection<Tap>
   */
  public Collection<Tap> getAllTaps()
    {
    return new HashSet<Tap>( getTapGraph().vertexSet() );
    }

  /**
   * Method getSuccessorFlows returns a Collection of all the Flow instances that will be
   * executed after the given Flow instance.
   *
   * @param flow of type Flow
   * @return Collection<Flow>
   */
  public Collection<Flow> getSuccessorFlows( Flow flow )
    {
    return Graphs.successorListOf( flowGraph, flow );
    }

  /**
   * Method getPredecessorFlows returns a Collection of all the Flow instances that will be
   * executed before the given Flow instance.
   *
   * @param flow of type Flow
   * @return Collection<Flow>
   */
  public Collection<Flow> getPredecessorFlows( Flow flow )
    {
    return Graphs.predecessorListOf( flowGraph, flow );
    }

  /**
   * Method findFlowsSourcingFrom returns all Flow instances that reads from a source with the given identifier.
   *
   * @param identifier of type String
   * @return Collection<Flow>
   */
  public Collection<Flow> findFlowsSourcingFrom( String identifier )
    {
    try
      {
      return unwrapFlows( identifierGraph.outgoingEdgesOf( identifier ) );
      }
    catch( Exception exception )
      {
      return Collections.emptySet();
      }
    }

  /**
   * Method findFlowsSinkingTo returns all Flow instances that writes to a sink with the given identifier.
   *
   * @param identifier of type String
   * @return Collection<Flow>
   */
  public Collection<Flow> findFlowsSinkingTo( String identifier )
    {
    try
      {
      return unwrapFlows( identifierGraph.incomingEdgesOf( identifier ) );
      }
    catch( Exception exception )
      {
      return Collections.emptySet();
      }
    }

  private Collection<Flow> unwrapFlows( Set<BaseFlow.FlowHolder> flowHolders )
    {
    Set<Flow> flows = new HashSet<Flow>();

    for( BaseFlow.FlowHolder flowHolder : flowHolders )
      flows.add( flowHolder.flow );

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
   * If a strategy is given, it will be used as the strategy for all {@link cascading.flow.BaseFlow} instances managed by this Cascade instance.
   * To revert back to consulting the strategies associated with each Flow instance, re-set this value to {@code null}, its
   * default value.
   * <p/>
   * FlowSkipStrategy instances define when a Flow instance should be skipped. The default strategy is {@link cascading.flow.FlowSkipIfSinkNotStale}
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

  @Override
  public void prepare()
    {
    }

  /**
   * Method start begins the current Cascade process. It returns immediately. See method {@link #complete()} to block
   * until the Cascade completes.
   */
  public void start()
    {
    if( thread != null )
      return;

    thread = new Thread( new Runnable()
    {
    @Override
    public void run()
      {
      Cascade.this.run();
      }
    }, ( "cascade " + Util.toNull( getName() ) ).trim() );

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
      shutdownHook = null;
      cascadeStats.cleanup();
      }
    }

  public synchronized void stop()
    {
    if( stop )
      return;

    stop = true;

    fireOnStopping();

    if( !cascadeStats.isFinished() )
      cascadeStats.markStopped();

    internalStopAllFlows();
    handleExecutorShutdown();

    cascadeStats.cleanup();
    }

  @Override
  public void cleanup()
    {
    }

  /** Method run implements the Runnable run method. */
  private void run()
    {
    Version.printBanner();

    if( LOG.isInfoEnabled() )
      logInfo( "starting" );

    registerShutdownHook();

    try
      {
      if( stop )
        return;

      // mark started, not submitted
      cascadeStats.markStartedThenRunning();

      fireOnStarting();

      initializeNewJobsMap();

      int numThreads = getMaxConcurrentFlows( properties, maxConcurrentFlows );

      if( numThreads == 0 )
        numThreads = jobsMap.size();

      int numLocalFlows = numLocalFlows();

      boolean runFlowsLocal = numLocalFlows > 1;

      if( runFlowsLocal )
        numThreads = 1;

      if( LOG.isInfoEnabled() )
        {
        logInfo( " parallel execution is enabled: " + !runFlowsLocal );
        logInfo( " starting flows: " + jobsMap.size() );
        logInfo( " allocating threads: " + numThreads );
        }

      List<Future<Throwable>> futures = spawnStrategy.start( this, numThreads, jobsMap.values() );

      for( Future<Throwable> future : futures )
        {
        throwable = future.get();

        if( throwable != null )
          {
          cascadeStats.markFailed( throwable );

          if( !stop )
            {
            internalStopAllFlows();
            fireOnThrowable();
            }

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

      try
        {
        fireOnCompleted();
        }
      finally
        {
        deregisterShutdownHook();
        }
      }
    }

  private void registerShutdownHook()
    {
    if( !isStopJobsOnExit() )
      return;

    shutdownHook = new ShutdownUtil.Hook()
    {
    @Override
    public Priority priority()
      {
      return Priority.WORK_PARENT;
      }

    @Override
    public void execute()
      {
      logInfo( "shutdown hook calling stop on cascade" );

      Cascade.this.stop();
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

  private boolean isStopJobsOnExit()
    {
    if( getFlows().isEmpty() )
      return false; // don't bother registering hook

    return getFlows().get( 0 ).isStopJobsOnExit();
    }

  /**
   * If the number of flows that are local is greater than one, force the Cascade to run without parallelization.
   *
   * @return of type int
   */
  private int numLocalFlows()
    {
    int countLocalJobs = 0;

    for( Flow flow : getFlows() )
      {
      if( flow.stepsAreLocal() )
        countLocalJobs++;
      }

    return countLocalJobs;
    }

  private void initializeNewJobsMap()
    {
    synchronized( jobsMap )
      {
      // keep topo order
      TopologicalOrderIterator<Flow, Integer> topoIterator = flowGraph.getTopologicalIterator();

      while( topoIterator.hasNext() )
        {
        Flow flow = topoIterator.next();

        cascadeStats.addFlowStats( flow.getFlowStats() );

        CascadeJob job = new CascadeJob( flow );

        jobsMap.put( flow.getName(), job );

        List<CascadeJob> predecessors = new ArrayList<CascadeJob>();

        for( Flow predecessor : Graphs.predecessorListOf( flowGraph, flow ) )
          predecessors.add( (CascadeJob) jobsMap.get( predecessor.getName() ) );

        job.init( predecessors );
        }
      }
    }

  private void handleExecutorShutdown()
    {
    if( spawnStrategy.isCompleted( this ) )
      return;

    logInfo( "shutting down flow executor" );

    try
      {
      spawnStrategy.complete( this, 5 * 60, TimeUnit.SECONDS );
      }
    catch( InterruptedException exception )
      {
      // ignore
      }

    logInfo( "shutdown complete" );
    }

  private void internalStopAllFlows()
    {
    logInfo( "stopping all flows" );

    synchronized( jobsMap )
      {
      List<Callable<Throwable>> jobs = new ArrayList<Callable<Throwable>>( jobsMap.values() );

      Collections.reverse( jobs );

      for( Callable<Throwable> callable : jobs )
        ( (CascadeJob) callable ).stop();
      }

    logInfo( "stopped all flows" );
    }

  /**
   * Method writeDOT writes this element graph to a DOT file for easy visualization and debugging.
   *
   * @param filename of type String
   */
  public void writeDOT( String filename )
    {
    printElementGraph( filename, identifierGraph );
    }

  protected void printElementGraph( String filename, SimpleDirectedGraph<String, BaseFlow.FlowHolder> graph )
    {
    try
      {
      Writer writer = new FileWriter( filename );

      Util.writeDOT( writer, graph, new IntegerNameProvider<String>(), new VertexNameProvider<String>()
        {
        public String getVertexName( String object )
          {
          return object.toString().replaceAll( "\"", "\'" );
          }
        }, new EdgeNameProvider<BaseFlow.FlowHolder>()
        {
        public String getEdgeName( BaseFlow.FlowHolder object )
          {
          return object.flow.getName().replaceAll( "\"", "\'" ).replaceAll( "\n", "\\\\n" ); // fix for newlines in graphviz
          }
        }
      );

      writer.close();
      }
    catch( IOException exception )
      {
      LOG.error( "failed printing graph to: {}, with exception: {}", filename, exception );
      }
    }

  @Override
  public String toString()
    {
    return getName();
    }

  private void logDebug( String message )
    {
    LOG.debug( "[" + Util.truncate( getName(), 25 ) + "] " + message );
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
    final Flow flow;
    /** Field predecessors */
    private List<CascadeJob> predecessors;
    /** Field latch */
    private final CountDownLatch latch = new CountDownLatch( 1 );
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

            flow.getFlowStats().markSkipped();

            return null;
            }

          flow.prepare(); // do not delete append/update mode taps
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
        finally
          {
          flow.cleanup();
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

  @Override
  public UnitOfWorkSpawnStrategy getSpawnStrategy()
    {
    return spawnStrategy;
    }

  @Override
  public void setSpawnStrategy( UnitOfWorkSpawnStrategy spawnStrategy )
    {
    this.spawnStrategy = spawnStrategy;
    }

  /**
   * Class SafeCascadeListener safely calls a wrapped CascadeListener.
   * <p/>
   * This is done for a few reasons, the primary reason is so exceptions thrown by the Listener
   * can be caught by the calling Thread. Since Cascade is asynchronous, much of the work is done in the run() method
   * which in turn is run in a new Thread.
   */
  private class SafeCascadeListener implements CascadeListener
    {
    /** Field flowListener */
    final CascadeListener cascadeListener;
    /** Field throwable */
    Throwable throwable;

    private SafeCascadeListener( CascadeListener cascadeListener )
      {
      this.cascadeListener = cascadeListener;
      }

    public void onStarting( Cascade cascade )
      {
      try
        {
        cascadeListener.onStarting( cascade );
        }
      catch( Throwable throwable )
        {
        handleThrowable( throwable );
        }
      }

    public void onStopping( Cascade cascade )
      {
      try
        {
        cascadeListener.onStopping( cascade );
        }
      catch( Throwable throwable )
        {
        handleThrowable( throwable );
        }
      }

    public void onCompleted( Cascade cascade )
      {
      try
        {
        cascadeListener.onCompleted( cascade );
        }
      catch( Throwable throwable )
        {
        handleThrowable( throwable );
        }
      }

    public boolean onThrowable( Cascade cascade, Throwable flowThrowable )
      {
      try
        {
        return cascadeListener.onThrowable( cascade, flowThrowable );
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

      logWarn( String.format( "cascade listener %s threw throwable", cascadeListener ), throwable );

      // stop this flow
      stop();
      }

    public boolean equals( Object object )
      {
      if( object instanceof SafeCascadeListener )
        return cascadeListener.equals( ( (SafeCascadeListener) object ).cascadeListener );

      return cascadeListener.equals( object );
      }

    public int hashCode()
      {
      return cascadeListener.hashCode();
      }
    }
  }
