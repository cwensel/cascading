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

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskCompletionEvent;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

/**
 *
 */
public class FlowStepJob implements Callable<Throwable>
  {
  /** Field stepName */
  private final String stepName;
  /** Field currentConf */
  private JobConf currentConf;
  /** Field runningJob */
  private RunningJob runningJob;
  /** Field pollingInterval */
  private int pollingInterval = 5000;

  /** Field predecessors */
  protected List<FlowStepJob> predecessors;
  /** Field latch */
  private final CountDownLatch latch = new CountDownLatch( 1 );
  /** Field stop */
  private boolean stop = false;
  /** Field flowStep */
  private FlowStep flowStep;

  /** Field throwable */
  protected Throwable throwable;

  public FlowStepJob( FlowStep flowStep, String stepName, JobConf currentConf )
    {
    this.flowStep = flowStep;
    this.stepName = stepName;
    this.currentConf = currentConf;
    this.pollingInterval = MultiMapReducePlanner.getJobStatusPollInterval( currentConf );

    if( flowStep.isDebugEnabled() )
      flowStep.logDebug( "using polling interval: " + pollingInterval );
    }

  public void stop()
    {
    if( flowStep.isInfoEnabled() )
      flowStep.logInfo( "stopping: " + stepName );

    stop = true;

    try
      {
      if( runningJob != null )
        runningJob.killJob();
      }
    catch( IOException exception )
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

  protected void blockOnJob() throws IOException
    {
    if( stop )
      return;

    if( flowStep.isInfoEnabled() )
      flowStep.logInfo( "starting step: " + stepName );

    runningJob = new JobClient( currentConf ).submitJob( currentConf );

    blockTillCompleteOrStopped();

    if( !stop && !runningJob.isSuccessful() )
      {
      dumpCompletionEvents();

      throwable = new FlowException( "step failed: " + stepName );
      }
    }

  protected void blockTillCompleteOrStopped() throws IOException
    {
    while( true )
      {
      if( stop || runningJob.isComplete() )
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
      if( runningJob == null )
        return;

      TaskCompletionEvent[] events = runningJob.getTaskCompletionEvents( 0 );
      flowStep.logWarn( "completion events count: " + events.length );

      for( TaskCompletionEvent event : events )
        flowStep.logWarn( "event = " + event );
      }
    catch( IOException exception )
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

      return runningJob != null && runningJob.isSuccessful();
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
    return runningJob != null;
    }
  }
