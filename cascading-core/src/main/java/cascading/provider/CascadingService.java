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

package cascading.provider;

import java.util.Map;

/**
 * Interface CascadingService defines a pluggable "service" class that can be loaded by the {@link ServiceLoader}
 * utility.
 *
 * @see ServiceLoader
 */
public interface CascadingService
  {
  void setProperties( Map<Object, Object> properties );

  /** May be called more than once, but only the first invocation will start the service. */
  void startService();

  /** May be called more than once, but only the first invocation will stop the service. */
  void stopService();

  /**
   * Returns true if this service has been enabled.
   *
   * @return boolean
   */
  boolean isEnabled();
  }