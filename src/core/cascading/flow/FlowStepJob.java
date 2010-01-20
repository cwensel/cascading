/*
 * Copyright (c) 2007-2009 Concurrent, Inc. All Rights Reserved.
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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

import cascading.flow.hadoop.HadoopStepStats;
import cascading.stats.StepStats;
import cascading.CascadingException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskCompletionEvent;

/**
 *
 */
public class FlowStepJob implements Callable<Throwable>
  {
  /** Field stepName */
  private final String stepName;
  /** Field currentConf */
  private Job currentJob;
  /** Field runningJob */
  private JobStatus jobStatus;
  /** Field pollingInterval */
  private long pollingInterval = 5000;

  /** Field predecessors */
  protected List<FlowStepJob> predecessors;
  /** Field latch */
  private final CountDownLatch latch = new CountDownLatch( 1 );
  /** Field stop */
  private boolean stop = false;
  /** Field flowStep */
  private FlowStep flowStep;
  /** Field stepStats */
  private HadoopStepStats stepStats;

  /** Field throwable */
  protected Throwable throwable;

  public FlowStepJob( FlowStep flowStep, String stepName, final Job currentJob )
    {
    this.flowStep = flowStep;
    this.stepName = stepName;
    this.currentJob = currentJob;
    this.pollingInterval = Flow.getJobPollingInterval( currentJob.getConfiguration() );

    if( flowStep.isDebugEnabled() )
      flowStep.logDebug( "using polling interval: " + pollingInterval );

    stepStats = new HadoopStepStats()
    {
    @Override
    protected JobStatus getRunningJob()
      {
      return jobStatus;
      }

    @Override
    protected Job getJob()
      {
      return currentJob;
      }
    };
    }

  public void stop()
    {
    if( flowStep.isInfoEnabled() )
      flowStep.logInfo( "stopping: " + stepName );

    stop = true;

    if( !stepStats.isPending() && !stepStats.isFinished() )
      stepStats.markStopped();

    try
      {
      if( currentJob != null )
        currentJob.killJob();
      }
    catch( IOException exception )
      {
      flowStep.logWarn( "unable to kill job: " + stepName, exception );
      }
    catch( InterruptedException exception )
      {
      flowStep.logWarn( "unable to kill job: " + stepName, exception );
      }
    }

  public void setPredecessors( List<FlowStepJob> predecessors ) throws IOException
    {
    this.predecessors = predecessors;
    }

  public Throwable call()
    {
    start();

    return throwable;
    }

  protected void start()
    {
    try
      {
      blockOnPredecessors();

      blockOnJob();
      }
    catch( Throwable throwable )
      {
      dumpCompletionEvents();
      this.throwable = throwable;
      }
    finally
      {
      latch.countDown();
      }
    }

  protected void blockOnJob() throws IOException, InterruptedException
    {
    if( stop )
      return;

    if( flowStep.isInfoEnabled() )
      flowStep.logInfo( "starting step: " + stepName );

    stepStats.markRunning();

    try
      {
      currentJob.submit();

      jobStatus = currentJob.getStatus();
      }
    catch( InterruptedException exception )
      {
      throw new CascadingException( exception );
      }
    catch( ClassNotFoundException exception )
      {
      throw new CascadingException( exception );
      }

    blockTillCompleteOrStopped();

    try
      {
      if( !stop && !currentJob.isSuccessful() )
        {
        if( !stepStats.isFinished() )
          stepStats.markFailed( null );

        dumpCompletionEvents();

        throwable = new FlowException( "step failed: " + stepName );
        }
      else
        {
        if( currentJob.isSuccessful() && !stepStats.isFinished() )
          stepStats.markSuccessful();
        }
      }
    catch( InterruptedException exception )
      {
      throw new CascadingException( exception );
      }

    stepStats.captureJobStats();
    }

  protected void blockTillCompleteOrStopped() throws IOException, InterruptedException
    {
    while( true )
      {
      if( stop || currentJob.isComplete() )
        break;

      sleep();
      }
    }

  protected void sleep()
    {
    try
      {
      Thread.sleep( pollingInterval );
      }
    catch( InterruptedException exception )
      {
      // do nothing
      }
    }

  protected void blockOnPredecessors()
    {
    for( FlowStepJob predecessor : predecessors )
      {
      if( !predecessor.isSuccessful() )
        {
        flowStep.logWarn( "abandoning step: " + stepName + ", predecessor failed: " + predecessor.stepName );

        stop();
        }
      }
    }

  private void dumpCompletionEvents()
    {
    try
      {
      if( jobStatus == null )
        return;

      TaskCompletionEvent[] events = currentJob.getTaskCompletionEvents( 0, Integer.MAX_VALUE );
      flowStep.logWarn( "completion events count: " + events.length );

      for( TaskCompletionEvent event : events )
        flowStep.logWarn( "event = " + event );
      }
    catch( IOException exception )
      {
      flowStep.logError( "failed reading completion events", exception );
      }
    catch( InterruptedException exception )
      {
      flowStep.logError( "failed reading completion events", exception );
      }
    }

  /**
   * Method isSuccessful returns true if this step completed successfully.
   *
   * @return the successful (type boolean) of this FlowStepJob object.
   */
  public boolean isSuccessful()
    {
    try
      {
      latch.await();

      return jobStatus != null && currentJob.isSuccessful();
      }
    catch( InterruptedException exception )
      {
      flowStep.logWarn( "latch interrupted", exception );
      }
    catch( IOException exception )
      {
      flowStep.logWarn( "error querying job", exception );
      }

    return false;
    }

  /**
   * Method wasStarted returns true if this job was started
   *
   * @return boolean
   */
  public boolean wasStarted()
    {
    return jobStatus != null;
    }

  /**
   * Method getStepStats returns the stepStats of this FlowStepJob object.
   *
   * @return the stepStats (type StepStats) of this FlowStepJob object.
   */
  public StepStats getStepStats()
    {
    return stepStats;
    }
  }
