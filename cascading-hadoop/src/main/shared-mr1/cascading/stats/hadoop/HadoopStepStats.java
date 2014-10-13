/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
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
import java.util.Iterator;

import cascading.flow.FlowException;
import cascading.flow.FlowNode;
import cascading.flow.FlowStep;
import cascading.flow.planner.BaseFlowStep;
import cascading.management.state.ClientState;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class HadoopStepStats provides Hadoop specific statistics and methods to underlying Hadoop facilities. */
public abstract class HadoopStepStats extends BaseHadoopStepStats<RunningJob, Counters>
  {
  private static final Logger LOG = LoggerFactory.getLogger( HadoopStepStats.class );

  private HadoopNodeStats mapperNodeStats;
  private HadoopNodeStats reducerNodeStats;

  protected HadoopStepStats( FlowStep<JobConf> flowStep, ClientState clientState )
    {
    super( flowStep, clientState );

    BaseFlowStep<JobConf> step = (BaseFlowStep<JobConf>) flowStep;

    Iterator<FlowNode> iterator = step.getFlowNodeGraph().getTopologicalIterator();

    mapperNodeStats = new HadoopNodeStats( this, getConfig(), HadoopSliceStats.Kind.MAPPER, iterator.next(), clientState );

    addNodeStats( mapperNodeStats );

    if( iterator.hasNext() )
      {
      reducerNodeStats = new HadoopNodeStats( this, getConfig(), HadoopSliceStats.Kind.REDUCER, iterator.next(), clientState );
      addNodeStats( reducerNodeStats );
      }

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

  /**
   * Method getJobID returns the Hadoop running job JobID.
   *
   * @return the jobID (type String) of this HadoopStepStats object.
   */
  public String getJobID()
    {
    if( getJobStatusClient() == null )
      return null;

    return getJobStatusClient().getJobID();
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
    RunningJob runningJob = getJobStatusClient();

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
    RunningJob runningJob = getJobStatusClient();

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

  public String getStatusURL()
    {
    RunningJob runningJob = getJobStatusClient();

    if( runningJob == null )
      return null;

    return runningJob.getTrackingURL();
    }

  private boolean stepHasReducers()
    {
    return getFlowStep().getNumFlowNodes() > 1;
    }

  /** Method captureDetail captures statistics task details and completion events. */
  @Override
  public synchronized void captureDetail()
    {
    captureDetail( true );
    }

  public void captureDetail( boolean captureAttempts )
    {
    JobClient jobClient = getJobClient();
    RunningJob runningJob = getJobStatusClient();

    if( jobClient == null || runningJob == null )
      return;

    try
      {
      mapperNodeStats.captureDetail();

      if( reducerNodeStats != null )
        reducerNodeStats.captureDetail();

      int count = 0;

      while( captureAttempts )
        {
        // todo: we may be able to continue where we left off if we retain the count
        TaskCompletionEvent[] events = runningJob.getTaskCompletionEvents( count );

        if( events.length == 0 )
          break;

        addAttemptsToTaskStats( events );
        count += events.length;
        }
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
