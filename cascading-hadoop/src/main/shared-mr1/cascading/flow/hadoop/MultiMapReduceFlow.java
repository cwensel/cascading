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

package cascading.flow.hadoop;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import cascading.flow.hadoop.util.HadoopUtil;
import cascading.flow.planner.PlatformInfo;
import cascading.flow.planner.process.FlowStepGraph;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import static cascading.flow.planner.graph.ElementGraphs.asFlowElementGraph;
import static cascading.util.Util.asList;

/**
 * Class MultiMapReduceFlow is a {@link cascading.flow.hadoop.HadoopFlow} subclass that supports custom MapReduce jobs
 * pre-configured via one or more {@link JobConf} objects.
 * <p/>
 * Use this class to group multiple JobConf instances together as a single Flow. MultiMapReduceFlow will automatically
 * topologically order the JobConf instances and schedule them on the cluster once {@link #start()} or {@link #complete()}
 * are called.
 * <p>
 * If you have a single JobConf instance, see {@link MapReduceFlow} as a alternative to this class.
 * <p>
 * This class will not delete any sinks before execution, it is up to the developer to make sure any intermediate and
 * sink paths be removed/deleted before calling {@link #start()} or {@link #complete()}, otherwise Hadoop will throw
 * an exception.
 * <p>
 * JobConf instances can be incrementally added at any point before the {@link #complete()} method is called. But they must
 * logically (topologically) come after any previously provided JobConf instances. In practice the Flow will fail if
 * the input source path is missing because a prior JobConf was not provided before the Flow was started.
 * <p>
 * The ordering is done by comparing the input and output paths of the given JobConf instances. By default, this class
 * only works with JobConf instances that read and write from the Hadoop FileSystem (HDFS) (any path that would work
 * with the {@link Hfs} Tap.
 * <p>
 * If the configured JobConf instance uses some other identifier instead of Hadoop FS paths, you should override the
 * {@link #createSources(org.apache.hadoop.mapred.JobConf)}, {@link #createSinks(org.apache.hadoop.mapred.JobConf)}, and
 * {@link #createTraps(org.apache.hadoop.mapred.JobConf)} methods to properly resolve the configured paths into
 * usable {@link Tap} instances. By default createTraps returns an empty collection and should probably be left alone.
 * <p/>
 * MultiMapReduceFlow supports both org.apache.hadoop.mapred.* and org.apache.hadoop.mapreduce.* API Jobs.
 */
public class MultiMapReduceFlow extends BaseMapReduceFlow
  {
  /** Field tapCache */
  private Map<String, Tap> tapCache = new HashMap<>();
  /** Field queuedSteps */
  private List<MapReduceFlowStep> queuedSteps = new LinkedList<>();
  /** Field completeCalled */
  private volatile boolean completeCalled = false;
  /** Field block */
  private final Object lock = new Object();

  /**
   * Constructor MultiMapReduceFlow creates a new MultiMapReduceFlow instance.
   *
   * @param name     of String
   * @param jobConf  of JobConf
   * @param jobConfs of JobConf...
   */
  public MultiMapReduceFlow( String name, JobConf jobConf, JobConf... jobConfs )
    {
    this( HadoopUtil.getPlatformInfo( JobConf.class, "org/apache/hadoop", "Hadoop MR" ), new Properties(), name );

    initializeFrom( asList( jobConf, jobConfs ) );
    }

  /**
   * Constructor MultiMapReduceFlow creates a new MultiMapReduceFlow instance.
   *
   * @param properties of Map<Object, Object>
   * @param name       of String
   * @param jobConf    of JobConf
   * @param jobConfs   of JobConf...
   */
  public MultiMapReduceFlow( Map<Object, Object> properties, String name, JobConf jobConf, JobConf... jobConfs )
    {
    this( HadoopUtil.getPlatformInfo( JobConf.class, "org/apache/hadoop", "Hadoop MR" ), properties, name, null );

    initializeFrom( asList( jobConf, jobConfs ) );
    }

  /**
   * Constructor MultiMapReduceFlow creates a new MultiMapReduceFlow instance.
   *
   * @param properties     of Map<Object, Object>
   * @param name           of String
   * @param flowDescriptor of Map<String, String>
   * @param jobConf        of JobConf
   * @param jobConfs       of JobConf...
   */
  public MultiMapReduceFlow( Map<Object, Object> properties, String name, Map<String, String> flowDescriptor, JobConf jobConf, JobConf... jobConfs )
    {
    this( HadoopUtil.getPlatformInfo( JobConf.class, "org/apache/hadoop", "Hadoop MR" ), properties, name, flowDescriptor );

    initializeFrom( asList( jobConf, jobConfs ) );
    }

  /**
   * Constructor MultiMapReduceFlow creates a new MultiMapReduceFlow instance.
   *
   * @param properties     of Map<Object, Object>
   * @param name           of String
   * @param flowDescriptor of Map<String, String>
   * @param stopJobsOnExit of boolean
   * @param jobConf        of JobConf
   * @param jobConfs       of JobConf...
   */
  public MultiMapReduceFlow( Map<Object, Object> properties, String name, Map<String, String> flowDescriptor, boolean stopJobsOnExit, JobConf jobConf, JobConf... jobConfs )
    {
    this( HadoopUtil.getPlatformInfo( JobConf.class, "org/apache/hadoop", "Hadoop MR" ), properties, name, flowDescriptor );
    this.stopJobsOnExit = stopJobsOnExit;

    initializeFrom( asList( jobConf, jobConfs ) );
    }

  /**
   * Constructor MultiMapReduceFlow creates a new MultiMapReduceFlow instance.
   *
   * @param platformInfo of PlatformInfo
   * @param properties   of Map<Object, Object>
   * @param name         of String
   */
  protected MultiMapReduceFlow( PlatformInfo platformInfo, Map<Object, Object> properties, String name )
    {
    this( platformInfo, properties, name, null );
    }

  /**
   * Constructor MultiMapReduceFlow creates a new MultiMapReduceFlow instance.
   *
   * @param platformInfo   of PlatformInfo
   * @param properties     of Map<Object, Object>
   * @param name           of String
   * @param flowDescriptor of Map<String, String>
   */
  protected MultiMapReduceFlow( PlatformInfo platformInfo, Map<Object, Object> properties, String name, Map<String, String> flowDescriptor )
    {
    super( platformInfo, properties, name, flowDescriptor, false );
    }

  protected void initializeFrom( List<JobConf> jobConfs )
    {
    List<MapReduceFlowStep> steps = new ArrayList<>();

    for( JobConf jobConf : jobConfs )
      steps.add( createMapReduceFlowStep( jobConf ) );

    updateWithFlowSteps( steps );
    }

  protected MapReduceFlowStep createMapReduceFlowStep( JobConf jobConf )
    {
    return new MapReduceFlowStep( this, jobConf );
    }

  public void notifyComplete()
    {
    completeCalled = true;

    synchronized( lock )
      {
      // forces blockingContinuePollingSteps to stop blocking
      lock.notifyAll();
      }
    }

  @Override
  public void complete()
    {
    notifyComplete();

    super.complete();
    }

  @Override
  protected boolean spawnSteps() throws InterruptedException, ExecutionException
    {
    // continue to spawn jobs until no longer required
    while( !stop && throwable == null )
      {
      if( !blockingContinuePollingSteps() )
        return true;

      if( isInfoEnabled() )
        {
        logInfo( "updated" );

        for( Tap source : getSourcesCollection() )
          logInfo( " source: " + source );
        for( Tap sink : getSinksCollection() )
          logInfo( " sink: " + sink );
        }

      // will not return until all current steps are complete, or one failed
      if( !super.spawnSteps() )
        return false;
      }

    return true;
    }

  protected boolean blockingContinuePollingSteps()
    {
    synchronized( lock )
      {
      // block until queue has items, or complete is called
      while( queuedSteps.isEmpty() && !completeCalled )
        {
        try
          {
          lock.wait();
          }
        catch( InterruptedException exception )
          {
          // do nothing
          }
        }

      updateWithFlowSteps( queuedSteps ).clear();
      }

    if( getEligibleJobsSize() != 0 ) // new ones were added
      return true;

    return !completeCalled;
    }

  @Override
  protected Tap createTap( JobConf jobConf, Path path, SinkMode sinkMode )
    {
    Tap tap = tapCache.get( path.toString() );

    if( tap == null )
      {
      tap = super.createTap( jobConf, path, sinkMode );
      tapCache.put( path.toString(), tap );
      }

    return tap;
    }

  public void attachFlowStep( JobConf jobConf )
    {
    if( completeCalled )
      throw new IllegalStateException( "cannot attach new FlowStep after complete() has been called" );

    addFlowStep( createMapReduceFlowStep( jobConf ) );
    }

  protected void addFlowStep( MapReduceFlowStep flowStep )
    {
    synchronized( lock )
      {
      queuedSteps.add( flowStep );
      lock.notifyAll();
      }
    }

  protected FlowStepGraph getOrCreateFlowStepGraph()
    {
    FlowStepGraph flowStepGraph = getFlowStepGraph();

    if( flowStepGraph == null )
      {
      flowStepGraph = new FlowStepGraph();
      setFlowStepGraph( flowStepGraph );
      }

    return flowStepGraph;
    }

  protected Collection<MapReduceFlowStep> updateWithFlowSteps( Collection<MapReduceFlowStep> flowSteps )
    {
    if( flowSteps.isEmpty() )
      return flowSteps;

    FlowStepGraph flowStepGraph = getOrCreateFlowStepGraph();

    updateFlowStepGraph( flowStepGraph, flowSteps );

    setFlowElementGraph( asFlowElementGraph( platformInfo, flowStepGraph ) );

    removeListeners( getSourcesCollection() );
    removeListeners( getSinksCollection() );
    removeListeners( getTrapsCollection() );

    // re-adds listeners
    setSources( flowStepGraph.getSourceTapsMap() );
    setSinks( flowStepGraph.getSinkTapsMap() );
    setTraps( flowStepGraph.getTrapsMap() );

    // this mirrors BaseFlow#initialize()

    initSteps();

    if( flowStats == null )
      flowStats = createPrepareFlowStats(); // must be last

    if( !isJobsMapInitialized() )
      initializeNewJobsMap();
    else
      updateJobsMap();

    initializeChildStats();

    return flowSteps;
    }

  protected FlowStepGraph updateFlowStepGraph( FlowStepGraph flowStepGraph, Collection<MapReduceFlowStep> flowSteps )
    {
    for( MapReduceFlowStep flowStep : flowSteps )
      flowStepGraph.addVertex( flowStep );

    flowStepGraph.bindEdges();

    return flowStepGraph;
    }
  }
