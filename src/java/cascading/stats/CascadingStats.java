/*
 * Copyright (c) 2007-2008 Vinculum Technologies, Inc. All Rights Reserved.
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

/**
 * Class CascadingStats is the base class for all Cascading statistics gathering. It also reports the status of
 * core elements that have state.
 */
public class CascadingStats
  {
  enum Status
    {
      PENDING, RUNNING, COMPLETED, FAILED, STOPPED;
    }

  Status status = Status.PENDING;
  long startTime;
  long finishedTime;
  Throwable throwable;

  public CascadingStats()
    {
    }

  /**
   * Method isFinished returns true if the current status show no work currently being executed.
   *
   * @return the finished (type boolean) of this CascadingStats object.
   */
  public boolean isFinished()
    {
    return status == Status.COMPLETED || status == Status.FAILED || status == Status.STOPPED;
    }

  public boolean isPending()
    {
    return status == Status.PENDING;
    }

  public boolean isRunning()
    {
    return status == Status.RUNNING;
    }

  public boolean isCompleted()
    {
    return status == Status.COMPLETED;
    }

  public boolean isFailed()
    {
    return status == Status.FAILED;
    }

  public boolean isStopped()
    {
    return status == Status.STOPPED;
    }

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

  public void markCompleted()
    {
    if( status != Status.RUNNING )
      throw new IllegalStateException( "may not mark flow as " + Status.COMPLETED + ", is already " + status );

    status = Status.COMPLETED;
    markFinishedTime();
    }

  private void markFinishedTime()
    {
    finishedTime = System.currentTimeMillis();
    }

  public void markFailed( Throwable throwable )
    {
    if( status != Status.RUNNING )
      throw new IllegalStateException( "may not mark flow as " + Status.FAILED + ", is already " + status );

    status = Status.FAILED;
    markFinishedTime();
    this.throwable = throwable;
    }

  public void markStopped()
    {
    if( status != Status.RUNNING )
      throw new IllegalStateException( "may not mark flow as " + Status.STOPPED + ", is already " + status );

    status = Status.STOPPED;
    markFinishedTime();
    }

  protected String getStatsString()
    {
    String string = "status=" + status + ", startTime=" + startTime;

    if( finishedTime != 0 )
      string += ", duration=" + ( finishedTime - startTime );

    return string;
    }

  public String toString()
    {
    return "Cascading{" + getStatsString() + '}';
    }
  }
