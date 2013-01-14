/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
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

package cascading.cascade;

import java.util.concurrent.Semaphore;

/**
 *
 */
public class LockingCascadeListener implements CascadeListener
  {
  public Semaphore started = new Semaphore( 0 );
  public Semaphore stopped = new Semaphore( 0 );
  public Semaphore completed = new Semaphore( 0 );
  public Semaphore thrown = new Semaphore( 0 );

  public LockingCascadeListener()
    {
    }

  @Override
  public void onStarting( Cascade cascade )
    {
    started.release();
    }

  @Override
  public void onStopping( Cascade cascade )
    {
    stopped.release();
    }

  @Override
  public void onCompleted( Cascade cascade )
    {
    completed.release();
    }

  @Override
  public boolean onThrowable( Cascade cascade, Throwable throwable )
    {
    thrown.release();
    return false;
    }
  }
