/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow.hadoop;

import java.io.IOException;

import cascading.flow.planner.FlowStep;
import cascading.flow.planner.FlowStepJob;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskCompletionEvent;

/**
 *
 */
public class HadoopFlowStepJob extends FlowStepJob
  {
  /** Field currentConf */
  private final JobConf currentConf;
  /** Field jobClient */
  private JobClient jobClient;
  /** Field runningJob */
  private RunningJob runningJob;

  public HadoopFlowStepJob( final FlowStep flowStep, String stepName, JobConf currentConf )
    {
    super( flowStep, stepName, HadoopFlow.getJobPollingInterval( currentConf ) );
    this.currentConf = currentConf;

    if( flowStep.isDebugEnabled() )
      flowStep.logDebug( "using polling interval: " + pollingInterval );

    stepStats = new HadoopStepStats( stepName )
    {

    @Override
    public Object getID()
      {
      return flowStep.getID();
      }

    @Override
    protected JobClient getJobClient()
      {
      return jobClient;
      }

    @Override
    protected RunningJob getRunningJob()
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
    return runningJob != null;
    }
  }
