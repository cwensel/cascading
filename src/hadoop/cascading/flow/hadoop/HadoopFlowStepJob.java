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

package cascading.flow.hadoop;

import java.io.IOException;

import cascading.flow.planner.FlowStep;
import cascading.flow.planner.FlowStepJob;
import cascading.management.ClientState;
import cascading.stats.FlowStepStats;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class HadoopFlowStepJob extends FlowStepJob
  {
  private static final Logger LOG = LoggerFactory.getLogger( HadoopFlowStepJob.class );

  /** Field currentConf */
  private final JobConf currentConf;
  /** Field jobClient */
  private JobClient jobClient;
  /** Field runningJob */
  private RunningJob runningJob;

  public HadoopFlowStepJob( ClientState clientState, FlowStep flowStep, JobConf currentConf )
    {
    super( clientState, flowStep, HadoopFlow.getJobPollingInterval( currentConf ) );
    this.currentConf = currentConf;

    if( flowStep.isDebugEnabled() )
      flowStep.logDebug( "using polling interval: " + pollingInterval );
    }

  @Override
  protected FlowStepStats createStepStats( ClientState clientState )
    {
    return new HadoopStepStats( currentConf, flowStep, clientState )
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

  protected void internalStop() throws IOException
    {
    if( runningJob != null )
      runningJob.killJob();
    }

  protected void internalNonBlockingStart() throws IOException
    {
    jobClient = new JobClient( currentConf );
    runningJob = jobClient.submitJob( currentConf );
    }

  protected boolean internalNonBlockingIsSuccessful() throws IOException
    {
    return runningJob != null && runningJob.isSuccessful();
    }

  @Override
  protected boolean isRemoteExecution()
    {
    return true;
    }

  @Override
  protected Throwable getThrowable()
    {
    return null;
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
      LOG.warn( "unable to test for map progress", exception );
      return false;
      }
    }
  }
