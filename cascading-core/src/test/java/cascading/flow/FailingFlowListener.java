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

/**
 *
 */
public class FailingFlowListener extends LockingFlowListener
  {
  public static enum OnFail
    {
      STARTING, STOPPING, COMPLETED, THROWABLE
    }

  private final OnFail onFail;

  public FailingFlowListener( OnFail onFail )
    {
    this.onFail = onFail;
    }

  public void onStarting( Flow flow )
    {
    super.onStarting( flow );

    if( onFail == OnFail.STARTING )
      throw new RuntimeException( "intentionally failed on: " + onFail );
    }

  public void onStopping( Flow flow )
    {
    super.onStopping( flow );

    if( onFail == OnFail.STOPPING )
      throw new RuntimeException( "intentionally failed on: " + onFail );
    }

  public void onCompleted( Flow flow )
    {
    super.onCompleted( flow );

    if( onFail == OnFail.COMPLETED )
      throw new RuntimeException( "intentionally failed on: " + onFail );
    }

  public boolean onThrowable( Flow flow, Throwable throwable )
    {
    super.onThrowable( flow, throwable );

    if( onFail == OnFail.THROWABLE )
      throw new RuntimeException( "intentionally failed on: " + onFail );

    return false;
    }
  }