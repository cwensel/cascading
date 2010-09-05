/*
 * Copyright (c) 2007-2010 Concurrent, Inc. All Rights Reserved.
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

package cascading.stats;

import java.io.Serializable;
import java.util.Collection;

/**
 * Class CascadingStats is the base class for all Cascading statistics gathering. It also reports the status of
 * core elements that have state.
 * <p/>
 * There are five states the stats object reports; pending, running, completed, failed, stopped, and finished.
 * <ul>
 * <li><code>pending</code> - when the Flow or Cascade has yet to start.</li>
 * <li><code>running</code> - when the Flow or Cascade is executing a workload.</li>
 * <li><code>successful</code> - when the Flow or Cascade naturally completed its workload without failure.</li>
 * <li><code>failed</code> - when the Flow or Cascade threw an error and failed to finish the workload.</li>
 * <li><code>stopped</code> - when the user calls stop() on the Flow or Cascade.</li>
 * <li><code>finished</code> - when the Flow or Cascade is no longer processing a workload and <code>completed</code>,
 * <code>failed</code>, or <code>stopped</code> is true.</li>
 * </ul>
 *
 * @see CascadeStats
 * @see FlowStats
 * @see StepStats
 */
public abstract class CascadingStats implements Serializable
  {
  public enum Status
    {
      PENDING, RUNNING, SUCCESSFUL, FAILED, STOPPED, SKIPPED;
    }

  /** Field name */
  String name;
  /** Field status */
  Status status = Status.PENDING;
  /** Field startTime */
  long startTime;
  /** Field finishedTime */
  long finishedTime;
  /** Field throwable */
  Throwable throwable;

  /** Constructor CascadingStats creates a new CascadingStats instance. */
  CascadingStats( String name )
    {
    this.name = name;
    }

  /**
   * Method getID returns the ID of this CascadingStats object.
   *
   * @return the ID (type Object) of this CascadingStats object.
   */
  public abstract Object getID();

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
   * Method isPending returns true if no work has started.
   *
   * @return the pending (type boolean) of this CascadingStats object.
   */
  public boolean isPending()
    {
    return status == Status.PENDING;
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
   * Method isSkipped returns true when the works was skipped.
   * <p/>
   * Flows are skipped if the apporpriate {@link cascading.flow.FlowSkipStrategy#skipFlow(cascading.flow.Flow)}
   * returns {@code true};
   *
   * @return the skipped (type boolean) of this CascadingStats object.
   */
  public boolean isSkipped()
    {
    return status == Status.SKIPPED;
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

  /** Method markRunning sets the status to running. */
  public void markRunning()
    {
    if( status != Status.PENDING )
      throw new IllegalStateException( "may not mark flow as " + Status.RUNNING + ", is already " + status );

    status = Status.RUNNING;
    markStartTime();
    }

  protected void markStartTime()
    {
    startTime = System.currentTimeMillis();
    }

  /** Method markSuccessful sets the status to successful. */
  public void markSuccessful()
    {
    if( status != Status.RUNNING )
      throw new IllegalStateException( "may not mark flow as " + Status.SUCCESSFUL + ", is already " + status );

    status = Status.SUCCESSFUL;
    markFinishedTime();
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
    if( status != Status.RUNNING )
      throw new IllegalStateException( "may not mark flow as " + Status.FAILED + ", is already " + status );

    status = Status.FAILED;
    markFinishedTime();
    this.throwable = throwable;
    }

  /** Method markStopped sets the status to stopped. */
  public void markStopped()
    {
    if( status != Status.RUNNING )
      throw new IllegalStateException( "may not mark flow as " + Status.STOPPED + ", is already " + status );

    status = Status.STOPPED;
    markFinishedTime();
    }

  /** Method markSkipped sets the status to skipped. */
  public void markSkipped()
    {
    if( status != Status.PENDING )
      throw new IllegalStateException( "may not mark flow as " + Status.SKIPPED + ", is already " + status );

    status = Status.SKIPPED;
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
