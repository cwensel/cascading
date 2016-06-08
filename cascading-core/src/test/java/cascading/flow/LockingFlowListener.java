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

package cascading.flow;

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
