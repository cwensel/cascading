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

package cascading.stats;

import java.io.Serializable;
import java.util.Collection;

import cascading.management.ClientState;

/**
 * Class CascadingStats is the base class for all Cascading statistics gathering. It also reports the status of
 * core elements that have state.
 * <p/>
 * There are eight states the stats object reports; PENDING, SKIPPED, STARTED, SUBMITTED, RUNNING, SUCCESSFUL, STOPPED, and FAILED.
 * <ul>
 * <li>{@code pending} - when the Flow or Cascade has yet to start.</li>
 * <li>{@code skipped} - when the Flow was skipped by the parent Cascade.</li>
 * <li>{@code submitted} - when the Step was submitted to the underlying platform for work.</li>
 * <li>{@code running} - when the Flow or Cascade is executing a workload.</li>
 * <li>{@code stopped} - when the user calls stop() on the Flow or Cascade.</li>
 * <li>{@code failed} - when the Flow or Cascade threw an error and failed to finish the workload.</li>
 * <li>{@code successful} - when the Flow or Cascade naturally completed its workload without failure.</li>
 * </ul>
 * <p/>
 * A unit of work is considered {@code finished} when the Flow or Cascade is no longer processing a workload and {@code successful},
 * {@code failed}, or {@code stopped} is true.
 *
 * @see CascadeStats
 * @see FlowStats
 * @see StepStats
 */
public abstract class CascadingStats<Config> implements Serializable
  {
  public enum Status
    {
      PENDING, SKIPPED, STARTED, SUBMITTED, RUNNING, SUCCESSFUL, STOPPED, FAILED
    }

  /** Field name */
  final String name;
  final ClientState clientState;

  /** Field status */
  Status status = Status.PENDING;

  /** Field startTime */
  long startTime;
  /** Field submitTime */
  long submitTime;
  /** Field runTime */
  long runTime;
  /** Field finishedTime */
  long finishedTime;
  /** Field throwable */
  Throwable throwable;

  /** Constructor CascadingStats creates a new CascadingStats instance. */
  CascadingStats( String name, ClientState clientState )
    {
    this.name = name;
    this.clientState = clientState;
    }

  public void prepare()
    {
    clientState.startService();
    }

  public void cleanup()
    {
    clientState.stopService();
    }

  /**
   * Method getID returns the ID of this CascadingStats object.
   *
   * @return the ID (type Object) of this CascadingStats object.
   */
  public abstract String getID();

  /**
   * Method getName returns the name of this CascadingStats object.
   *
   * @return the name (type String) of this CascadingStats object.
   */
  public String getName()
    {
    return name;
    }

  /**
   * Method isPending returns true if no work has been submitted.
   *
   * @return the pending (type boolean) of this CascadingStats object.
   */
  public boolean isPending()
    {
    return status == Status.PENDING;
    }

  /**
   * Method isSkipped returns true when the works was skipped.
   * <p/>
   * Flows are skipped if the appropriate {@link cascading.flow.FlowSkipStrategy#skipFlow(cascading.flow.Flow)}
   * returns {@code true};
   *
   * @return the skipped (type boolean) of this CascadingStats object.
   */
  public boolean isSkipped()
    {
    return status == Status.SKIPPED;
    }

  /**
   * Method isStarted returns true when work has started.
   *
   * @return the started (type boolean) of this CascadingStats object.
   */
  public boolean isStarted()
    {
    return status == Status.STARTED;
    }

  /**
   * Method isSubmitted returns true if no work has started.
   *
   * @return the submitted (type boolean) of this CascadingStats object.
   */
  public boolean isSubmitted()
    {
    return status == Status.SUBMITTED;
    }

  /**
   * Method isRunning returns true when work has begun.
   *
   * @return the running (type boolean) of this CascadingStats object.
   */
  public boolean isRunning()
    {
    return status == Status.RUNNING;
    }

  /**
   * Method isSuccessful returns true when work has completed successfully.
   *
   * @return the completed (type boolean) of this CascadingStats object.
   */
  public boolean isSuccessful()
    {
    return status == Status.SUCCESSFUL;
    }

  /**
   * Method isFailed returns true when the work ended with an error.
   *
   * @return the failed (type boolean) of this CascadingStats object.
   */
  public boolean isFailed()
    {
    return status == Status.FAILED;
    }

  /**
   * Method isStopped returns true when the user stopped the work.
   *
   * @return the stopped (type boolean) of this CascadingStats object.
   */
  public boolean isStopped()
    {
    return status == Status.STOPPED;
    }

  /**
   * Method isFinished returns true if the current status show no work currently being executed. This method
   * returns true if {@link #isSuccessful()}, {@link #isFailed()}, or {@link #isStopped()} returns true.
   *
   * @return the finished (type boolean) of this CascadingStats object.
   */
  public boolean isFinished()
    {
    return status == Status.SUCCESSFUL || status == Status.FAILED || status == Status.STOPPED;
    }

  /**
   * Method getStatus returns the status of this CascadingStats object.
   *
   * @return the status (type Status) of this CascadingStats object.
   */
  public Status getStatus()
    {
    return status;
    }

  public void markPending()
    {
    recordStats();
    recordInfo();
    }

  public void recordStats()
    {
    this.clientState.recordStats( this );
    }

  public abstract void recordInfo();

  public void markStartedThenRunning()
    {
    if( status != Status.PENDING )
      throw new IllegalStateException( "may not mark flow as " + Status.STARTED + ", is already " + status );

    markStartAndRunTime();
    markStarted();
    markRunning();
    }

  protected void markStartAndRunTime()
    {
    startTime = runTime = System.currentTimeMillis();
    }

  public void markStarted()
    {
    if( status != Status.PENDING )
      throw new IllegalStateException( "may not mark flow as " + Status.STARTED + ", is already " + status );

    status = Status.STARTED;
    markStartTime();

    clientState.start( startTime );
    clientState.setStatus( status, startTime );
    recordStats();
    }

  protected void markStartTime()
    {
    if( startTime == 0 )
      startTime = System.currentTimeMillis();
    }

  public void markSubmitted()
    {
    if( status != Status.STARTED )
      throw new IllegalStateException( "may not mark flow as " + Status.SUBMITTED + ", is already " + status );

    status = Status.SUBMITTED;
    markSubmitTime();

    clientState.submit( submitTime );
    clientState.setStatus( status, submitTime );
    recordStats();
    }

  protected void markSubmitTime()
    {
    submitTime = System.currentTimeMillis();
    }

  /** Method markRunning sets the status to running. */
  public void markRunning()
    {
    if( status == Status.RUNNING )
      return;

    if( status != Status.STARTED && status != Status.SUBMITTED )
      throw new IllegalStateException( "may not mark flow as " + Status.RUNNING + ", is already " + status );

    status = Status.RUNNING;
    markRunTime();

    clientState.run( runTime );
    clientState.setStatus( status, runTime );
    recordStats();
    }

  protected void markRunTime()
    {
    if( runTime == 0 )
      runTime = System.currentTimeMillis();
    }

  /** Method markSuccessful sets the status to successful. */
  public void markSuccessful()
    {
    if( status != Status.RUNNING && status != Status.SUBMITTED )
      throw new IllegalStateException( "may not mark flow as " + Status.SUCCESSFUL + ", is already " + status );

    status = Status.SUCCESSFUL;
    markFinishedTime();

    clientState.setStatus( status, finishedTime );
    clientState.stop( finishedTime );
    recordStats();
    recordInfo();
    }

  private void markFinishedTime()
    {
    finishedTime = System.currentTimeMillis();
    }

  /**
   * Method markFailed sets the status to failed.
   *
   * @param throwable of type Throwable
   */
  public void markFailed( Throwable throwable )
    {
    if( status != Status.STARTED && status != Status.RUNNING && status != Status.SUBMITTED )
      throw new IllegalStateException( "may not mark flow as " + Status.FAILED + ", is already " + status );

    status = Status.FAILED;
    markFinishedTime();
    this.throwable = throwable;

    clientState.setStatus( status, finishedTime );
    clientState.stop( finishedTime );
    recordStats();
    recordInfo();
    }

  /** Method markStopped sets the status to stopped. */
  public void markStopped()
    {
    if( status != Status.PENDING && status != Status.STARTED && status != Status.SUBMITTED && status != Status.RUNNING )
      throw new IllegalStateException( "may not mark flow as " + Status.STOPPED + ", is already " + status );

    status = Status.STOPPED;
    markFinishedTime();

    clientState.setStatus( status, finishedTime );
    recordStats();
    recordInfo();
    clientState.stop( finishedTime );
    }

  /** Method markSkipped sets the status to skipped. */
  public void markSkipped()
    {
    if( status != Status.PENDING )
      throw new IllegalStateException( "may not mark flow as " + Status.SKIPPED + ", is already " + status );

    status = Status.SKIPPED;

    clientState.setStatus( status, System.currentTimeMillis() );
    recordStats();
    }

  /**
   * Method getSubmitTime returns the submitTime of this CascadingStats object.
   *
   * @return the submitTime (type long) of this CascadingStats object.
   */
  public long getSubmitTime()
    {
    return submitTime;
    }

  /**
   * Method getStartTime returns the startTime of this CascadingStats object.
   *
   * @return the startTime (type long) of this CascadingStats object.
   */
  public long getStartTime()
    {
    return startTime;
    }

  /**
   * Method getFinishedTime returns the finishedTime of this CascadingStats object.
   *
   * @return the finishedTime (type long) of this CascadingStats object.
   */
  public long getFinishedTime()
    {
    return finishedTime;
    }

  /**
   * Method getDuration returns the duration the work executed before being finished.
   * <p/>
   * This method will return zero until the work is finished. See {@link #getCurrentDuration()}
   * if you wish to poll for the current duration value.
   * <p/>
   * Duration is calculated as {@code finishedTime - startTime}.
   *
   * @return the duration (type long) of this CascadingStats object.
   */
  public long getDuration()
    {
    if( finishedTime != 0 )
      return finishedTime - startTime;
    else
      return 0;
    }

  /**
   * Method getCurrentDuration returns the current duration of the current work whether or not
   * the work is finished. When finished, the return value will be the same as {@link #getDuration()}.
   *
   * @return the currentDuration (type long) of this CascadingStats object.
   */
  public long getCurrentDuration()
    {
    if( finishedTime != 0 )
      return finishedTime - startTime;
    else
      return System.currentTimeMillis() - startTime;
    }

  /**
   * Method getRunDuration returns the runtime duration the work executed before being finished. If this workload
   * enters the {@link Status#SUBMITTED} state, it will represent the actual time executing the work on the underlying
   * platform.
   * <p/>
   * This method will return zero until the work is finished or hasn't begun.
   * <p/>
   * Duration is calculated as {@code finishedTime - runTime}.
   *
   * @return the runDuration (type long) of this CascadingStats object.
   */
  public long getRunDuration()
    {
    if( finishedTime != 0 )
      return finishedTime - runTime;
    else
      return 0;
    }

  /**
   * Method getCounterGroups returns all the available counter group names.
   *
   * @return the counterGroups (type Collection<String>) of this CascadingStats object.
   */
  public abstract Collection<String> getCounterGroups();

  /**
   * Method getCounterGroupsMatching returns all the available counter group names that match
   * the given regular expression.
   *
   * @param regex of type String
   * @return Collection<String>
   */
  public abstract Collection<String> getCounterGroupsMatching( String regex );

  /**
   * Method getCountersFor returns all the counter names for the give group name.
   *
   * @param group
   * @return Collection<String>
   */
  public abstract Collection<String> getCountersFor( String group );

  /**
   * Method getCountersFor returns all the counter names for the counter Enum.
   *
   * @param group
   * @return Collection<String>
   */
  public Collection<String> getCountersFor( Class<? extends Enum> group )
    {
    return getCountersFor( group.getName() );
    }

  /**
   * Method getCounter returns the current value for the given counter Enum.
   *
   * @param counter of type Enum
   * @return the current counter value
   */
  public abstract long getCounterValue( Enum counter );

  /**
   * Method getCounter returns the current value for the given group and counter.
   *
   * @param group   of type String
   * @param counter of type String
   * @return the current counter value
   */
  public abstract long getCounterValue( String group, String counter );

  /**
   * Method captureDetail will recursively capture details about nested systems. Use this method to persist
   * statistics about a given Cascade, Flow, or FlowStep.
   * <p/>
   * Each CascadingStats object must be individually inspected for any system specific details.
   */
  public abstract void captureDetail();

  public abstract Collection getChildren();

  protected String getStatsString()
    {
    String string = "status=" + status + ", startTime=" + startTime;

    if( finishedTime != 0 )
      string += ", duration=" + ( finishedTime - startTime );

    return string;
    }

  @Override
  public String toString()
    {
    return "Cascading{" + getStatsString() + '}';
    }
  }
