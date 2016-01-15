/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow.hadoop.planner;

import java.io.IOException;

import cascading.flow.FlowException;
import cascading.flow.hadoop.HadoopFlowStep;
import cascading.flow.planner.BaseFlowStep;
import cascading.flow.planner.FlowStepJob;
import cascading.management.state.ClientState;
import cascading.stats.FlowNodeStats;
import cascading.stats.FlowStepStats;
import cascading.stats.hadoop.HadoopStepStats;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskCompletionEvent;

import static cascading.flow.FlowProps.JOB_POLLING_INTERVAL;
import static cascading.stats.CascadingStats.STATS_COMPLETE_CHILD_DETAILS_BLOCK_DURATION;
import static cascading.stats.CascadingStats.STATS_STORE_INTERVAL;

/**
 *
 */
public class HadoopFlowStepJob extends FlowStepJob<JobConf>
  {
  /** static field to capture errors in hadoop local mode */
  private static Throwable localError;
  /** Field jobClient */
  protected JobClient jobClient;
  /** Field runningJob */
  protected RunningJob runningJob;

  private static long getStoreInterval( JobConf jobConf )
    {
    return jobConf.getLong( STATS_STORE_INTERVAL, 60 * 1000 );
    }

  private static long getChildDetailsBlockingDuration( JobConf jobConf )
    {
    return jobConf.getLong( STATS_COMPLETE_CHILD_DETAILS_BLOCK_DURATION, 60 * 1000 );
    }

  public static long getJobPollingInterval( JobConf jobConf )
    {
    return jobConf.getLong( JOB_POLLING_INTERVAL, 5000 );
    }

  public HadoopFlowStepJob( ClientState clientState, BaseFlowStep<JobConf> flowStep, JobConf currentConf )
    {
    super( clientState, currentConf, flowStep, getJobPollingInterval( currentConf ), getStoreInterval( currentConf ), getChildDetailsBlockingDuration( currentConf ) );

    if( flowStep.isDebugEnabled() )
      flowStep.logDebug( "using polling interval: " + pollingInterval );
    }

  @Override
  protected FlowStepStats createStepStats( ClientState clientState )
    {
    return new HadoopStepStats( flowStep, clientState )
    {
    @Override
    public JobClient getJobClient()
      {
      return jobClient;
      }

    @Override
    public RunningJob getJobStatusClient()
      {
      return runningJob;
      }
    };
    }

  protected void internalBlockOnStop() throws IOException
    {
    if( runningJob != null && !runningJob.isComplete() )
      runningJob.killJob();
    }

  protected void internalNonBlockingStart() throws IOException
    {
    jobClient = new JobClient( jobConfiguration );
    runningJob = internalNonBlockingSubmit();

    flowStep.logInfo( "submitted hadoop job: " + runningJob.getID() );

    if( runningJob.getTrackingURL() != null )
      flowStep.logInfo( "tracking url: " + runningJob.getTrackingURL() );
    }

  protected RunningJob internalNonBlockingSubmit() throws IOException
    {
    return jobClient.submitJob( jobConfiguration );
    }

  @Override
  protected void updateNodeStatus( FlowNodeStats flowNodeStats )
    {
    try
      {
      if( runningJob == null || flowNodeStats.isFinished() )
        return;

      boolean isMapper = flowNodeStats.getOrdinal() == 0;
      int jobState = runningJob.getJobState();

      if( JobStatus.FAILED == jobState )
        {
        flowNodeStats.markFailed();
        return;
        }

      if( JobStatus.KILLED == jobState )
        {
        flowNodeStats.markStopped();
        return;
        }

      float progress;

      if( isMapper )
        progress = runningJob.mapProgress();
      else
        progress = runningJob.reduceProgress();

      if( progress == 0.0F ) // not yet running, is only started
        return;

      if( progress != 1.0F )
        {
        flowNodeStats.markRunning();
        return;
        }

      flowNodeStats.markRunning();

      if( isMapper && runningJob.reduceProgress() > 0.0F )
        {
        flowNodeStats.markSuccessful();
        return;
        }

      if( JobStatus.SUCCEEDED == jobState )
        flowNodeStats.markSuccessful();
      }
    catch( IOException exception )
      {
      flowStep.logError( "failed setting node status", throwable );
      }
    }

  @Override
  public boolean isSuccessful()
    {
    try
      {
      return super.isSuccessful();
      }
    catch( NullPointerException exception )
      {
      throw new FlowException( "Hadoop is not keeping a large enough job history, please increase the \'mapred.jobtracker.completeuserjobs.maximum\' property", exception );
      }
    }

  protected boolean internalNonBlockingIsSuccessful() throws IOException
    {
    return runningJob != null && runningJob.isSuccessful();
    }

  @Override
  protected boolean isRemoteExecution()
    {
    return !( (HadoopFlowStep) flowStep ).isHadoopLocalMode( getConfig() );
    }

  @Override
  protected Throwable getThrowable()
    {
    return localError;
    }

  protected String internalJobId()
    {
    return runningJob.getJobID();
    }

  protected boolean internalNonBlockingIsComplete() throws IOException
    {
    return runningJob.isComplete();
    }

  protected void dumpDebugInfo()
    {
    try
      {
      if( runningJob == null )
        return;

      int jobState = runningJob.getJobState(); // may throw an NPE internally

      flowStep.logWarn( "hadoop job " + runningJob.getID() + " state at " + JobStatus.getJobRunState( jobState ) );
      flowStep.logWarn( "failure info: " + runningJob.getFailureInfo() );

      TaskCompletionEvent[] events = runningJob.getTaskCompletionEvents( 0 );
      flowStep.logWarn( "task completion events identify failed tasks" );
      flowStep.logWarn( "task completion events count: " + events.length );

      for( TaskCompletionEvent event : events )
        flowStep.logWarn( "event = " + event );
      }
    catch( Throwable throwable )
      {
      flowStep.logError( "failed reading task completion events", throwable );
      }
    }

  protected boolean internalIsStartedRunning()
    {
    if( runningJob == null )
      return false;

    try
      {
      return runningJob.mapProgress() > 0;
      }
    catch( IOException exception )
      {
      flowStep.logWarn( "unable to test for map progress", exception );
      return false;
      }
    }

  /**
   * Internal method to report errors that happen on hadoop local mode. Hadoops local
   * JobRunner does not give access to TaskReports, but we want to be able to capture
   * the exception and not just print it to stderr. FlowMapper and FlowReducer use this method.
   *
   * @param throwable the throwable to be reported.
   */
  public static void reportLocalError( Throwable throwable )
    {
    localError = throwable;
    }
  }
