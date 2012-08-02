/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
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
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * The interface UnitOfWorkSpawnStrategy is a strategy for allowing pluggable thread management services into
 * any {@link UnitOfWork} class.
 * <p/>
 * The default strategy is {@link UnitOfWorkExecutorStrategy}.
 *
 * @see UnitOfWork
 * @see cascading.flow.Flow
 * @see cascading.cascade.Cascade
 */
public interface UnitOfWorkSpawnStrategy
  {
  List<Future<Throwable>> start( UnitOfWork unitOfWork, int maxConcurrentThreads, Collection<Callable<Throwable>> values ) throws InterruptedException;

  boolean isCompleted( UnitOfWork unitOfWork );

  void complete( UnitOfWork unitOfWork, int duration, TimeUnit unit ) throws InterruptedException;
  }
