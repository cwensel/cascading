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

package cascading.stats.hadoop;

import java.io.IOException;

import cascading.flow.FlowException;
import cascading.flow.FlowNode;
import cascading.flow.FlowStep;
import cascading.flow.planner.BaseFlowStep;
import cascading.management.state.ClientState;
import cascading.stats.BaseCachedStepStats;
import cascading.util.Util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskCompletionEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class HadoopStepStats provides Hadoop specific statistics and methods to underlying Hadoop facilities. */
public abstract class HadoopStepStats extends BaseCachedStepStats<Configuration, RunningJob, Counters>
  {
  private static final Logger LOG = LoggerFactory.getLogger( HadoopStepStats.class );

  private HadoopNodeStats mapperNodeStats;
  private HadoopNodeStats reducerNodeStats;

  protected static Job getJob( RunningJob runningJob )
    {
    if( runningJob == null ) // if null, job hasn't been submitted
      return null;

    Job job = Util.returnInstanceFieldIfExistsSafe( runningJob, "job" );

    if( job == null )
      {
      LOG.warn( "unable to get underlying org.apache.hadoop.mapreduce.Job from org.apache.hadoop.mapred.RunningJob, task level task counters will be unavailable" );
      return null;
      }

    return job;
    }

  protected HadoopStepStats( FlowStep<JobConf> flowStep, ClientState clientState )
    {
    super( flowStep, clientState );

    BaseFlowStep<JobConf> step = (BaseFlowStep<JobConf>) flowStep;

    // don't rely on the iterator topological sort to identify mapper or reducer
    for( FlowNode current : step.getFlowNodeGraph().vertexSet() )
      {
      if( step.getFlowNodeGraph().inDegreeOf( current ) == 0 )
        {
        if( mapperNodeStats != null )
          throw new IllegalStateException( "mapper node already found" );

        mapperNodeStats = new HadoopNodeStats( this, getConfig(), HadoopSliceStats.Kind.MAPPER, current, clientState );
        addNodeStats( mapperNodeStats );
        }
      else
        {
        if( reducerNodeStats != null )
          throw new IllegalStateException( "reducer node already found" );

        reducerNodeStats = new HadoopNodeStats( this, getConfig(), HadoopSliceStats.Kind.REDUCER, current, clientState );
        addNodeStats( reducerNodeStats );
        }
      }

    if( mapperNodeStats == null )
      throw new IllegalStateException( "mapper node not found" );

    counterCache = new HadoopStepCounterCache( this, (Configuration) getConfig() )
    {
    @Override
    protected RunningJob getJobStatusClient()
      {
      return HadoopStepStats.this.getJobStatusClient();
      }
    };
    }

  private Configuration getConfig()
    {
    return (Configuration) this.getFlowStep().getConfig();
    }

  /**
   * Method getNumMapTasks returns the numMapTasks from the Hadoop job file.
   *
   * @return the numMapTasks (type int) of this HadoopStepStats object.
   */
  public int getNumMapTasks()
    {
    return mapperNodeStats.getChildren().size();
    }

  /**
   * Method getNumReduceTasks returns the numReducerTasks from the Hadoop job file.
   *
   * @return the numReducerTasks (type int) of this HadoopStepStats object.
   */
  public int getNumReduceTasks()
    {
    return reducerNodeStats == null ? 0 : reducerNodeStats.getChildren().size();
    }

  @Override
  public String getProcessStepID()
    {
    if( getJobStatusClient() == null )
      return null;

    return getJobStatusClient().getJobID().toString();
    }

  /**
   * Method getJobClient returns the Hadoop {@link JobClient} managing this Hadoop job.
   *
   * @return the jobClient (type JobClient) of this HadoopStepStats object.
   */
  public abstract JobClient getJobClient();

  /**
   * Returns the underlying Map tasks progress percentage.
   * <p/>
   * This method is experimental.
   *
   * @return float
   */
  public float getMapProgress()
    {
    Job runningJob = getJob( getJobStatusClient() );

    if( runningJob == null )
      return 0;

    try
      {
      return runningJob.mapProgress();
      }
    catch( IOException exception )
      {
      throw new FlowException( "unable to get progress" );
      }
    }

  /**
   * Returns the underlying Reduce tasks progress percentage.
   * <p/>
   * This method is experimental.
   *
   * @return float
   */
  public float getReduceProgress()
    {
    Job runningJob = getJob( getJobStatusClient() );

    if( runningJob == null )
      return 0;

    try
      {
      return runningJob.reduceProgress();
      }
    catch( IOException exception )
      {
      throw new FlowException( "unable to get progress" );
      }
    }

  @Override
  public String getProcessStatusURL()
    {
    return getStatusURL();
    }

  /**
   * @deprecated see {@link #getProcessStatusURL()}
   */
  @Deprecated
  public String getStatusURL()
    {
    Job runningJob = getJob( getJobStatusClient() );

    if( runningJob == null )
      return null;

    return runningJob.getTrackingURL();
    }

  /** Method captureDetail captures statistics task details and completion events. */
  @Override
  public synchronized void captureDetail( Type depth )
    {
    if( !getType().isChild( depth ) || !isDetailStale() )
      return;

    Job runningJob = getJob( getJobStatusClient() );

    if( runningJob == null )
      return;

    try
      {
      mapperNodeStats.captureDetail( depth );

      if( reducerNodeStats != null )
        reducerNodeStats.captureDetail( depth );

      int count = 0;

      while( depth == Type.ATTEMPT )
        {
        TaskCompletionEvent[] events = runningJob.getTaskCompletionEvents( count );

        if( events.length == 0 )
          break;

        addAttemptsToTaskStats( events );
        count += events.length;
        }

      markDetailCaptured();
      }
    catch( IOException exception )
      {
      LOG.warn( "unable to get task stats", exception );
      }
    }

  private void addAttemptsToTaskStats( TaskCompletionEvent[] events )
    {
    for( TaskCompletionEvent event : events )
      {
      if( event == null )
        {
        LOG.warn( "found empty completion event" );
        continue;
        }

      if( event.isMapTask() )
        mapperNodeStats.addAttempt( event );
      else
        reducerNodeStats.addAttempt( event );
      }
    }
  }
