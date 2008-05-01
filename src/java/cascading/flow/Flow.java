/*
 * Copyright (c) 2007-2008 Chris K Wensel. All Rights Reserved.
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

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
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
import cascading.tap.TapCollector;
import cascading.tap.TapIterator;
import cascading.tap.hadoop.HttpFileSystem;
import cascading.tap.hadoop.S3HttpFileSystem;
import cascading.util.Util;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;
import org.jgrapht.Graphs;
import org.jgrapht.ext.DOTExporter;
import org.jgrapht.ext.EdgeNameProvider;
import org.jgrapht.ext.IntegerNameProvider;
import org.jgrapht.ext.VertexNameProvider;
import org.jgrapht.graph.SimpleDirectedGraph;
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
 */
public class Flow implements Runnable
  {
  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( Flow.class );

  /** Field name */
  private String name;
  /** Field pipeGraph */
  private SimpleDirectedGraph<FlowElement, Scope> pipeGraph;
  /** Field stepGraph */
  private SimpleDirectedGraph<FlowStep, Integer> stepGraph;
  /** Field jobConf */
  private JobConf jobConf;
  /** Field listeners */
  private List<SafeFlowListener> listeners;
  /** Field flowStats */
  private final FlowStats flowStats = new FlowStats(); // don't use a listener to set values
  /** Field thread */
  private Thread thread;
  /** Field throwable */
  private Throwable throwable;
  /** Field sources */
  private Map<String, Tap> sources;
  /** Field sinks */
  private Map<String, Tap> sinks;
  /** Field preserveTemporaryFiles */
  private boolean preserveTemporaryFiles = false;
  /** Field stop */
  private boolean stop = false;

  /** Field steps */
  private transient List<FlowStep> steps;

  /** Used for testing. */
  protected Flow()
    {
    }

  protected Flow( JobConf jobConf, String name, SimpleDirectedGraph<FlowElement, Scope> pipeGraph, SimpleDirectedGraph<FlowStep, Integer> stepGraph, Map<String, Tap> sources, Map<String, Tap> sinks )
    {
    setJobConf( jobConf );
    this.name = name;
    this.pipeGraph = pipeGraph;
    this.stepGraph = stepGraph;
    this.sources = sources;
    this.sinks = sinks;
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

  private void setJobConf( JobConf jobConf )
    {
    if( jobConf == null )
      return;

    this.jobConf = new JobConf( jobConf ); // prevent local values from being shared
    this.jobConf.set( "fs.http.impl", HttpFileSystem.class.getName() );
    this.jobConf.set( "fs.https.impl", HttpFileSystem.class.getName() );
    this.jobConf.set( "fs.s3tp.impl", S3HttpFileSystem.class.getName() );
    }

  /**
   * Method getJobConf returns the jobConf of this Flow object.
   *
   * @return the jobConf (type JobConf) of this Flow object.
   */
  public JobConf getJobConf()
    {
    if( jobConf == null )
      setJobConf( new JobConf() );

    return jobConf;
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
   * Method setPreserveTemporaryFiles sets the preserveTemporaryFiles of this Flow object. Defaults to false. Set
   * to true if temporary files should be kept.
   *
   * @param preserveTemporaryFiles the preserveTemporaryFiles of this Flow object.
   */
  public void setPreserveTemporaryFiles( boolean preserveTemporaryFiles )
    {
    this.preserveTemporaryFiles = preserveTemporaryFiles;
    }

  /**
   * Method areSinksStale returns true if any of the sinks referenced are out of date in relation to the sources.
   *
   * @return boolean
   * @throws IOException when
   */
  public boolean areSinksStale() throws IOException
    {
    long sinkMod = Long.MAX_VALUE;
    JobConf confCopy = new JobConf( getJobConf() ); // let's not add unused values by accident

    for( Tap sink : sinks.values() )
      {
      if( sink.isDeleteOnSinkInit() )
        sinkMod = -1L;
      else
        {
        if( !sink.pathExists( confCopy ) )
          sinkMod = 0L;
        else
          sinkMod = Math.min( sinkMod, sink.getPathModified( confCopy ) ); // return youngest mod date
        }
      }

    if( LOG.isInfoEnabled() )
      {
      if( sinkMod == -1L )
        logInfo( "atleast one sink is marked for delete" );
      if( sinkMod == 0L )
        logInfo( "atleast one sink does not exist" );
      else
        logInfo( "sink oldest modified date: " + new Date( sinkMod ) );
      }

    long sourceMod = 0;

    try
      {
      for( Tap source : sources.values() )
        {
        if( !source.pathExists( confCopy ) )
          throw new FlowException( "source does not exist: " + source );

        sourceMod = source.getPathModified( confCopy );

        if( sinkMod < sourceMod )
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
   * to block until this Flow completes. The given JobConf instance will replace any previous value.
   *
   * @param jobConf of type JobConf
   */
  public void start( JobConf jobConf )
    {
    setJobConf( jobConf );
    start();
    }

  /**
   * Method start begins the execution of this Flow instance. It will return immediately. Use the method {@link #complete()}
   * to block until this Flow completes.
   */
  public synchronized void start()
    {
    if( thread != null )
      return;

    thread = new Thread( this, ( "flow " + Util.toNull( getName() ) ).trim() );

    thread.start();
    }

  /** Method stop suggests to the flow to stop execution. It does not kill running jobs. */
  public void stop()
    {
    this.stop = true;
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
        throw new FlowException( "thread interrupted", exception );
        }

      if( throwable instanceof CascadingException )
        throw (CascadingException) throwable;

      if( throwable != null )
        throw new FlowException( "unhandled exception", throwable );

      if( hasListeners() )
        {
        for( SafeFlowListener safeFlowListener : getListeners() )
          {
          if( safeFlowListener.throwable != null )
            throw new FlowException( "unhandled listener exception", throwable );
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
   * @return TapIterator
   * @throws IOException when
   */
  public TapIterator openSource() throws IOException
    {
    return sources.values().iterator().next().openForRead( getJobConf() );
    }

  /**
   * Method openSource opens the named source Tap.
   *
   * @param name of type String
   * @return TapIterator
   * @throws IOException when
   */
  public TapIterator openSource( String name ) throws IOException
    {
    return sources.get( name ).openForRead( getJobConf() );
    }

  /**
   * Method openSink opens the first sink Tap.
   *
   * @return TapIterator
   * @throws IOException when
   */
  public TapIterator openSink() throws IOException
    {
    return sinks.values().iterator().next().openForRead( getJobConf() );
    }

  /**
   * Method openSink opens the named sink Tap.
   *
   * @param name of type String
   * @return TapIterator
   * @throws IOException when
   */
  public TapIterator openSink( String name ) throws IOException
    {
    return sinks.get( name ).openForRead( getJobConf() );
    }

  /**
   * Method deleteSinks deletes all sinks. Typically used by a {@link Cascade} before executing the flow if the sinks are stale.
   * Use with caution.
   *
   * @throws IOException when
   */
  public void deleteSinks() throws IOException
    {
    for( Tap tap : sinks.values() )
      tap.deletePath( getJobConf() );
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
    return tap.pathExists( getJobConf() );
    }

  /**
   * Method openTapForRead return a {@link TapIterator} for the given Tap instance.
   *
   * @param tap of type Tap
   * @return TapIterator
   * @throws IOException when
   */
  public TapIterator openTapForRead( Tap tap ) throws IOException
    {
    return tap.openForRead( getJobConf() );
    }

  /**
   * Method openTapForWrite returns a (@link TapCollector} for the given Tap instance.
   *
   * @param tap of type Tap
   * @return TapCollector
   * @throws IOException when
   */
  public TapCollector openTapForWrite( Tap tap ) throws IOException
    {
    return tap.openForWrite( getJobConf() );
    }

  /**
   * Method jobsAreLocal returns true if all jobs are executed in-process as a single map and reduce task.
   *
   * @return boolean
   */
  public boolean jobsAreLocal()
    {
    return getJobConf().get( "mapred.job.tracker" ).equalsIgnoreCase( "local" );
    }

  /** Method run implements the Runnable run method. */
  public void run()
    {
    try
      {
      flowStats.markRunning();

      if( hasListeners() )
        {
        if( LOG.isDebugEnabled() )
          logDebug( "firing onStarting event: " + getListeners().size() );

        for( FlowListener flowListener : getListeners() )
          flowListener.onStarting( this );
        }

      if( LOG.isInfoEnabled() )
        {
        logInfo( "starting" );

        for( Tap source : getSources().values() )
          logInfo( " source: " + source );
        for( Tap sink : getSinks().values() )
          logInfo( " sink: " + sink );
        }

      // keep topo order
      Map<String, Callable<Throwable>> jobsMap = new LinkedHashMap<String, Callable<Throwable>>();
      TopologicalOrderIterator topoIterator = new TopologicalOrderIterator<FlowStep, Integer>( stepGraph );

      while( topoIterator.hasNext() )
        {
        FlowStep step = (FlowStep) topoIterator.next();
        step.setParentFlowName( getName() );
        FlowStep.FlowStepJob flowStepJob = step.getFlowStepJob();

        jobsMap.put( step.getName(), flowStepJob );

        List<FlowStep.FlowStepJob> predecessors = new ArrayList<FlowStep.FlowStepJob>();

        for( FlowStep flowStep : Graphs.predecessorListOf( stepGraph, step ) )
          predecessors.add( (FlowStep.FlowStepJob) jobsMap.get( flowStep.getName() ) );

        flowStepJob.init( getJobConf(), predecessors );
        }

      // if jobs are run local, then only use one thread to force execution serially
      int numThreads = jobsAreLocal() ? 1 : jobsMap.size();

      if( LOG.isInfoEnabled() )
        {
        logInfo( " parallel execution is enabled: " + !jobsAreLocal() );
        logInfo( " starting jobs: " + jobsMap.size() );
        logInfo( " allocating threads: " + numThreads );
        }

      flowStats.setStepsCount( jobsMap.size() );

      ExecutorService executor = Executors.newFixedThreadPool( numThreads );
      List<Future<Throwable>> futures = executor.invokeAll( jobsMap.values() );

      executor.shutdown(); // don't accept any more work

      for( Future<Throwable> future : futures )
        {
        throwable = future.get();

        if( throwable != null || stop )
          {
          if( stop && hasListeners() )
            {
            if( LOG.isDebugEnabled() )
              logDebug( "firing onStopping event: " + getListeners().size() );

            for( FlowListener flowListener : getListeners() )
              flowListener.onStopping( this );
            }

          LOG.warn( "stopping jobs" );

          for( Callable<Throwable> callable : jobsMap.values() )
            ( (FlowStep.FlowStepJob) callable ).stop();

          if( stop )
            flowStats.markStopped();

          LOG.warn( "shutting down job executor" );

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
    finally
      {
      if( !isPreserveTemporaryFiles() )
        cleanTemporaryFiles();

      if( hasListeners() )
        {
        if( throwable != null )
          {
          flowStats.markFailed( throwable );

          if( LOG.isDebugEnabled() )
            logDebug( "firing onThrowable event: " + getListeners().size() );

          boolean isHandled = false;

          for( FlowListener flowListener : getListeners() )
            isHandled = flowListener.onThrowable( this, throwable ) || isHandled;

          if( isHandled )
            throwable = null;
          }

        if( !flowStats.isFinished() )
          flowStats.markCompleted();

        if( LOG.isDebugEnabled() )
          logDebug( "firing onCompleted event: " + getListeners().size() );

        for( FlowListener flowListener : getListeners() )
          flowListener.onCompleted( this );
        }
      }
    }

  private void cleanTemporaryFiles()
    {
    for( FlowStep step : getSteps() )
      step.clean( getJobConf() );
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

  /**
   * Method writeDOT writes this Flow instance to the given filename as a DOT file for import into a graphics package.
   *
   * @param filename of type String
   */
  public void writeDOT( String filename )
    {
    printElementGraph( filename, pipeGraph );
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

  // DOT WRITER

  protected static void printElementGraph( String filename, SimpleDirectedGraph<FlowElement, Scope> graph )
    {
    try
      {
      Writer writer = new FileWriter( filename );

      printElementGraph( writer, graph );

      writer.close();
      }
    catch( IOException exception )
      {
      exception.printStackTrace();
      }
    }

  protected static void printElementGraph( Writer writer, final SimpleDirectedGraph<FlowElement, Scope> graph )
    {
    DOTExporter dot = new DOTExporter<FlowElement, Scope>( new IntegerNameProvider<FlowElement>(), new VertexNameProvider<FlowElement>()
    {
    public String getVertexName( FlowElement object )
      {
      if( object instanceof Tap || object instanceof FlowConnector.Extent )
        return object.toString().replaceAll( "\"", "\'" );

      Scope scope = graph.outgoingEdgesOf( object ).iterator().next();

      return ( (Pipe) object ).print( scope ).replaceAll( "\"", "\'" );
      }
    }, new EdgeNameProvider<Scope>()
    {
    public String getEdgeName( Scope object )
      {
      return object.toString().replaceAll( "\"", "\'" );
      }
    } );

    dot.export( writer, graph );
    }


  /** Class SafeFlowListener safely calls a wrapped FlowListener */
  private class SafeFlowListener implements FlowListener
    {
    final FlowListener flowListener;
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
        this.throwable = throwable;
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
        this.throwable = throwable;
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
        this.throwable = throwable;
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
        this.throwable = throwable;
        }

      return false;
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
