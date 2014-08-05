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

package cascading.flow.hadoop.planner;

import java.io.IOException;

import cascading.flow.hadoop.HadoopFlowStep;
import cascading.flow.planner.BaseFlowStep;
import cascading.flow.planner.FlowStepJob;
import cascading.management.state.ClientState;
import cascading.stats.FlowStepStats;
import cascading.stats.hadoop.HadoopStepStats;
import cascading.tap.hadoop.fs.DistributedCacheFileSystem;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskCompletionEvent;

import static cascading.flow.FlowProps.JOB_POLLING_INTERVAL;
import static cascading.stats.CascadingStats.STATS_STORE_INTERVAL;

/**
 *
 */
public class HadoopFlowStepJob extends FlowStepJob<JobConf>
  {
  /** static field to capture errors in hadoop local mode */
  private static Throwable localError;
  /** Field currentConf */
  private final JobConf currentConf;
  /** Field jobClient */
  private JobClient jobClient;
  /** Field runningJob */
  private RunningJob runningJob;

  private static long getStoreInterval( JobConf jobConf )
    {
    return jobConf.getLong( STATS_STORE_INTERVAL, 60 * 1000 );
    }

  public static long getJobPollingInterval( JobConf jobConf )
    {
    return jobConf.getLong( JOB_POLLING_INTERVAL, 5000 );
    }

  public HadoopFlowStepJob( ClientState clientState, BaseFlowStep flowStep, JobConf currentConf )
    {
    super( clientState, flowStep, getJobPollingInterval( currentConf ), getStoreInterval( currentConf ) );
    this.currentConf = currentConf;

    if( flowStep.isDebugEnabled() )
      flowStep.logDebug( "using polling interval: " + pollingInterval );
    }

  @Override
  public JobConf getConfig()
    {
    return currentConf;
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
    public RunningJob getRunningJob()
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
    DistributedCacheFileSystem.populateDistCache( flowStep.getSources(), currentConf );
    jobClient = new JobClient( currentConf );
    runningJob = jobClient.submitJob( currentConf );

    flowStep.logInfo( "submitted hadoop job: " + runningJob.getID() );

    if( runningJob.getTrackingURL() != null )
      flowStep.logInfo( "tracking url: " + runningJob.getTrackingURL() );
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

      flowStep.logWarn( "hadoop job " + runningJob.getID() + " state at " + JobStatus.getJobRunState( runningJob.getJobState() ) );
      flowStep.logWarn( "failure info: " + runningJob.getFailureInfo() );

      TaskCompletionEvent[] events = runningJob.getTaskCompletionEvents( 0 );
      flowStep.logWarn( "task completion events identify failed tasks" );
      flowStep.logWarn( "task completion events count: " + events.length );

      for( TaskCompletionEvent event : events )
        flowStep.logWarn( "event = " + event );
      }
    catch( IOException exception )
      {
      flowStep.logError( "failed reading task completion events", exception );
      }
    }

  protected boolean internalIsStarted()
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
   *
   * */
  public static void reportLocalError( Throwable throwable )
    {
    localError = throwable;
    }
  }
