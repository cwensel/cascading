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

package cascading.flow.planner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

import cascading.flow.Flow;
import cascading.flow.FlowException;
import cascading.flow.FlowStep;
import cascading.flow.FlowStepStrategy;
import cascading.management.state.ClientState;
import cascading.stats.CascadingStats;
import cascading.stats.FlowNodeStats;
import cascading.stats.FlowStats;
import cascading.stats.FlowStepStats;
import cascading.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cascading.util.Util.formatDurationFromMillis;

/**
 *
 */
public abstract class FlowStepJob<Config> implements Callable<Throwable>
  {
  // most logs messages should be delegated to the FlowStep.log* methods
  // non job related issues can use this logger
  private static final Logger LOG = LoggerFactory.getLogger( FlowStepJob.class );

  /** Field stepName */
  protected final String stepName;
  /** Field jobConfiguration */
  protected final Config jobConfiguration;
  /** Field pollingInterval */
  protected long pollingInterval = 1000;
  /** Field recordStatsInterval */
  protected long statsStoreInterval = 60 * 1000;
  /** Field waitTillCompletedChildStatsDuration */
  protected long blockForCompletedChildDetailDuration = 60 * 1000;
  /** Field predecessors */
  protected List<FlowStepJob<Config>> predecessors;
  /** Field latch */
  private final CountDownLatch latch = new CountDownLatch( 1 );
  /** Field stop */
  private boolean stop = false;
  /** Field flowStep */
  protected final BaseFlowStep<Config> flowStep;
  /** Field stepStats */
  protected FlowStepStats flowStepStats;
  /** Field throwable */
  protected Throwable throwable;

  public FlowStepJob( ClientState clientState, Config jobConfiguration, BaseFlowStep<Config> flowStep, long pollingInterval, long statsStoreInterval, long blockForCompletedChildDetailDuration )
    {
    this.jobConfiguration = jobConfiguration;
    this.stepName = flowStep.getName();
    this.pollingInterval = pollingInterval;
    this.statsStoreInterval = statsStoreInterval;
    this.blockForCompletedChildDetailDuration = blockForCompletedChildDetailDuration;
    this.flowStep = flowStep;
    this.flowStepStats = createStepStats( clientState );

    this.flowStepStats.prepare();
    this.flowStepStats.markPending();

    for( FlowNodeStats flowNodeStats : this.flowStepStats.getFlowNodeStats() )
      {
      flowNodeStats.prepare();
      flowNodeStats.markPending();
      }
    }

  public Config getConfig()
    {
    return jobConfiguration;
    }

  protected abstract FlowStepStats createStepStats( ClientState clientState );

  public synchronized void stop()
    {
    if( flowStep.isInfoEnabled() )
      flowStep.logInfo( "stopping: " + stepName );

    stop = true;

    // allow pending -> stopped transition
    // never want a hanging pending state
    if( !flowStepStats.isFinished() )
      flowStepStats.markStopped();

    try
      {
      internalBlockOnStop();
      }
    catch( IOException exception )
      {
      flowStep.logWarn( "unable to kill job: " + stepName, exception );
      }
    finally
      {
      // call rollback after the job has been stopped, only if it was stopped
      if( flowStepStats.isStopped() )
        {
        flowStep.rollbackSinks();
        flowStep.fireOnStopping();
        }

      flowStepStats.cleanup();
      }
    }

  protected abstract void internalBlockOnStop() throws IOException;

  public void setPredecessors( List<FlowStepJob<Config>> predecessors )
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
      if( isSkipFlowStep() )
        {
        markSkipped();

        if( flowStep.isInfoEnabled() && flowStepStats.isSkipped() )
          flowStep.logInfo( "skipping step: " + stepName );

        return;
        }

      synchronized( this ) // added in 3.0, jdk1.7 may have a aggravated
        {
        if( stop )
          {
          if( flowStep.isInfoEnabled() )
            flowStep.logInfo( "stop called before start: " + stepName );

          return;
          }

        markStarted();
        }

      blockOnPredecessors();

      prepareResources(); // throws Throwable to skip next steps

      applyFlowStepConfStrategy(); // prepare resources beforehand

      blockOnJob();
      }
    catch( Throwable throwable )
      {
      this.throwable = throwable; // store first, in case throwable leaks out of dumpDebugInfo

      dumpDebugInfo();

      // in case isSkipFlowStep fails before the previous markStarted, we don't fail advancing to the failed state
      if( flowStepStats.isPending() )
        markStarted();

      if( !flowStepStats.isFinished() )
        {
        flowStepStats.markFailed( this.throwable );
        flowStep.fireOnThrowable( this.throwable );
        }
      }
    finally
      {
      latch.countDown();

      // lets free the latch and capture any failure info if exiting via a throwable
      // remain in this thread to keep client side alive until shutdown
      finalizeNodeSliceCapture();

      flowStepStats.cleanup();
      }

    internalCleanup();
    }

  private void prepareResources() throws Throwable
    {
    if( stop ) // true if a predecessor failed
      return;

    Throwable throwable = flowStep.prepareResources();

    if( throwable != null )
      throw throwable;
    }

  private synchronized boolean markStarted()
    {
    if( flowStepStats.isFinished() ) // if stopped, return
      return false;

    flowStepStats.markStarted();

    return true;
    }

  private void applyFlowStepConfStrategy()
    {
    FlowStepStrategy flowStepStrategy = flowStep.getFlow().getFlowStepStrategy();

    if( flowStepStrategy == null )
      return;

    List<FlowStep> predecessorSteps = new ArrayList<FlowStep>();

    for( FlowStepJob predecessor : predecessors )
      predecessorSteps.add( predecessor.flowStep );

    flowStepStrategy.apply( flowStep.getFlow(), predecessorSteps, flowStep );
    }

  protected boolean isSkipFlowStep() throws IOException
    {
    // if runID is not set, never skip a step
    if( flowStep.getFlow().getRunID() == null )
      return false;

    return flowStep.allSourcesExist() && !flowStep.areSourcesNewer( flowStep.getSinkModified() );
    }

  protected void blockOnJob() throws IOException
    {
    if( stop ) // true if a predecessor failed
      return;

    if( flowStep.isInfoEnabled() )
      flowStep.logInfo( "starting step: " + stepName );

    internalNonBlockingStart();

    markSubmitted();
    flowStep.fireOnStarting();

    blockTillCompleteOrStopped();

    if( !stop && !internalNonBlockingIsSuccessful() )
      {
      if( !flowStepStats.isFinished() )
        {
        flowStep.rollbackSinks();
        flowStepStats.markFailed( getThrowable() );
        updateNodesStatus();
        flowStep.fireOnThrowable( getThrowable() );
        }

      // if available, rethrow the unrecoverable error
      if( getThrowable() instanceof OutOfMemoryError )
        throw (OutOfMemoryError) getThrowable();

      dumpDebugInfo();

      if( !isRemoteExecution() )
        this.throwable = new FlowException( "local step failed: " + stepName, getThrowable() );
      else
        this.throwable = new FlowException( "step failed: " + stepName + ", step id: " + getStepStats().getID() + ", job id: " + internalJobId() + ", please see cluster logs for failure messages" );
      }
    else
      {
      if( internalNonBlockingIsSuccessful() && !flowStepStats.isFinished() )
        {
        this.throwable = flowStep.commitSinks();

        if( this.throwable != null )
          {
          flowStepStats.markFailed( this.throwable );
          updateNodesStatus();
          flowStep.fireOnThrowable( this.throwable );
          }
        else
          {
          flowStepStats.markSuccessful();
          updateNodesStatus();
          flowStep.fireOnCompleted();
          }
        }
      }
    }

  protected void finalizeNodeSliceCapture()
    {
    long startOfFinalPolling = System.currentTimeMillis();
    long lastLog = 0;
    long retries = 0;

    boolean allNodesFinished;

    while( true )
      {
      allNodesFinished = updateNodesStatus();

      flowStepStats.recordChildStats();

      if( allNodesFinished && flowStepStats.hasCapturedFinalDetail() )
        break;

      if( ( System.currentTimeMillis() - startOfFinalPolling ) >= blockForCompletedChildDetailDuration )
        break;

      if( System.currentTimeMillis() - lastLog > 1000 )
        {
        if( !allNodesFinished )
          flowStep.logInfo( "did not capture all completed node details, will retry in {}, prior retries: {}", formatDurationFromMillis( pollingInterval ), retries );
        else
          flowStep.logInfo( "did not capture all completed slice details, will retry in {}, prior retries: {}", formatDurationFromMillis( pollingInterval ), retries );

        lastLog = System.currentTimeMillis();
        }

      retries++;

      sleepForPollingInterval();
      }

    if( !allNodesFinished )
      flowStep.logWarn( "unable to capture all completed node details or determine final state within configured duration: {}, configure property to increase wait duration: '{}'", formatDurationFromMillis( blockForCompletedChildDetailDuration ), CascadingStats.STATS_COMPLETE_CHILD_DETAILS_BLOCK_DURATION );

    if( !flowStepStats.hasCapturedFinalDetail() )
      flowStep.logWarn( "unable to capture all completed slice details within configured duration: {}, configure property to increase wait duration: '{}'", formatDurationFromMillis( blockForCompletedChildDetailDuration ), CascadingStats.STATS_COMPLETE_CHILD_DETAILS_BLOCK_DURATION );
    }

  protected abstract boolean isRemoteExecution();

  protected abstract String internalJobId();

  protected abstract boolean internalNonBlockingIsSuccessful() throws IOException;

  protected abstract Throwable getThrowable();

  protected abstract void internalNonBlockingStart() throws IOException;

  protected void blockTillCompleteOrStopped() throws IOException
    {
    int iterations = (int) Math.floor( statsStoreInterval / pollingInterval );
    int count = 0;

    while( true )
      {
      // test stop last, internalIsStartedRunning may block causing a race condition
      if( flowStepStats.isSubmitted() && internalIsStartedRunning() && !stop )
        {
        markRunning();
        flowStep.fireOnRunning();
        }

      if( flowStepStats.isRunning() )
        updateNodesStatus(); // records node stats on node status change, not slices

      if( stop || internalNonBlockingIsComplete() )
        break;

      if( iterations == count++ )
        {
        count = 0;
        flowStepStats.recordStats();
        flowStepStats.recordChildStats(); // records node and slice stats
        }

      sleepForPollingInterval();
      }
    }

  private synchronized void markSubmitted()
    {
    if( flowStepStats.isStarted() )
      {
      flowStepStats.markSubmitted();

      Collection<FlowNodeStats> children = flowStepStats.getChildren();

      for( FlowNodeStats flowNodeStats : children )
        flowNodeStats.markStarted();
      }

    Flow flow = flowStep.getFlow();

    if( flow == null )
      {
      LOG.warn( "no parent flow set" );
      return;
      }

    FlowStats flowStats = flow.getFlowStats();

    synchronized( flowStats )
      {
      if( flowStats.isStarted() )
        flowStats.markSubmitted();
      }
    }

  private synchronized void markSkipped()
    {
    if( flowStepStats.isFinished() )
      return;

    try
      {
      flowStepStats.markSkipped();
      flowStep.fireOnCompleted();
      }
    finally
      {
      markFlowRunning(); // move to running before marking failed
      }
    }

  private synchronized void markRunning()
    {
    flowStepStats.markRunning();

    markFlowRunning();
    }

  private synchronized void markFlowRunning()
    {
    Flow flow = flowStep.getFlow();

    if( flow == null )
      {
      LOG.warn( "no parent flow set" );
      return;
      }

    FlowStats flowStats = flow.getFlowStats();

    synchronized( flowStats )
      {
      if( flowStats.isStarted() || flowStats.isSubmitted() )
        flowStats.markRunning();
      }
    }

  private boolean updateNodesStatus()
    {
    boolean allFinished = true;

    Collection<FlowNodeStats> children = flowStepStats.getFlowNodeStats();

    for( FlowNodeStats child : children )
      {
      // child#markStarted is called above
      if( child.isFinished() || child.isPending() )
        continue;

      updateNodeStatus( child );

      allFinished &= child.isFinished();
      }

    return allFinished;
    }

  protected abstract void updateNodeStatus( FlowNodeStats flowNodeStats );

  protected abstract boolean internalNonBlockingIsComplete() throws IOException;

  protected void sleepForPollingInterval()
    {
    Util.safeSleep( pollingInterval );
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

  protected abstract void dumpDebugInfo();

  /**
   * Method isSuccessful returns true if this step completed successfully or was skipped.
   *
   * @return the successful (type boolean) of this FlowStepJob object.
   */
  public boolean isSuccessful()
    {
    try
      {
      latch.await(); // freed after step completes in #start()

      return flowStepStats.isSuccessful() || flowStepStats.isSkipped();
      }
    catch( InterruptedException exception )
      {
      flowStep.logWarn( "latch interrupted", exception );

      return false;
      }
    }

  /**
   * Method isStarted returns true if this underlying job has started running
   *
   * @return boolean
   */
  public boolean isStarted()
    {
    return internalIsStartedRunning();
    }

  protected abstract boolean internalIsStartedRunning();

  protected void internalCleanup()
    {
    // optional, safe to override
    }

  /**
   * Method getStepStats returns the stepStats of this FlowStepJob object.
   *
   * @return the stepStats (type StepStats) of this FlowStepJob object.
   */
  public FlowStepStats getStepStats()
    {
    return flowStepStats;
    }
  }
