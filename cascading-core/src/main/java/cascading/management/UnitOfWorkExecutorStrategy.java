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

package cascading.management;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Class UnitOfWorkExecutorStrategy uses a simple {@link Executors#newFixedThreadPool(int)} {@link ExecutorService}
 * to spawn threads.
 * <p/>
 * This is the default spawn strategy.
 */
public class UnitOfWorkExecutorStrategy implements UnitOfWorkSpawnStrategy
  {
  private ExecutorService executor;

  public List<Future<Throwable>> start( UnitOfWork unitOfWork, int maxConcurrentThreads, Collection<Callable<Throwable>> values ) throws InterruptedException
    {
    executor = Executors.newFixedThreadPool( maxConcurrentThreads );

    List<Future<Throwable>> futures = executor.invokeAll( values ); // todo: consider submit()

    executor.shutdown(); // don't accept any more work

    return futures;
    }

  @Override
  public boolean isCompleted( UnitOfWork unitOfWork )
    {
    return executor == null || executor.isTerminated();
    }

  @Override
  public void complete( UnitOfWork unitOfWork, int duration, TimeUnit unit ) throws InterruptedException
    {
    if( executor == null )
      return;

    executor.awaitTermination( duration, unit );
    }
  }
