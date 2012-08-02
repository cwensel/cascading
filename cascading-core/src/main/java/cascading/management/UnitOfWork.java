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

import cascading.stats.CascadingStats;

/**
 * Interface UnitOfWork is a common interface for {@link cascading.flow.Flow} and {@link cascading.cascade.Cascade}
 * allowing them to be used interchangeably.
 */
public interface UnitOfWork<Stats extends CascadingStats>
  {
  String getName();

  String getID();

  Stats getStats();

  String getTags();

  void prepare();

  void start();

  void stop();

  void complete();

  void cleanup();

  void setSpawnStrategy( UnitOfWorkSpawnStrategy spawnStrategy );

  UnitOfWorkSpawnStrategy getSpawnStrategy();
  }
