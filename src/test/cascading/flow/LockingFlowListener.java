/*
 * Copyright (c) 2007-20010 Concurrent, Inc. All Rights Reserved.
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

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;

/**
 *
 */
public class LockingFlowListener implements FlowListener
  {
  public Semaphore started = new Semaphore( 0 );
  public Semaphore stopped = new Semaphore( 0 );
  public Semaphore completed = new Semaphore( 0 );
  public Semaphore thrown = new Semaphore( 0 );

  public static Map<String, Callable<Throwable>> getJobsMap( Flow flow )
    {
    return flow.getJobsMap();
    }

  public LockingFlowListener()
    {
    }

  public void onStarting( Flow flow )
    {
    started.release();
    }

  public void onStopping( Flow flow )
    {
    stopped.release();
    }

  public void onCompleted( Flow flow )
    {
    completed.release();
    }

  public boolean onThrowable( Flow flow, Throwable throwable )
    {
    thrown.release();
    return false;
    }
  }
