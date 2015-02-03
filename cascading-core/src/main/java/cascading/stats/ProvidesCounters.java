/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
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

package cascading.stats;

import java.util.Collection;

/**
 *
 */
public interface ProvidesCounters
  {
  /**
   * Method getCounterGroups returns all the available counter group names.
   *
   * @return the counterGroups (type Collection<String>) of this CascadingStats object.
   */
  Collection<String> getCounterGroups();

  /**
   * Method getCountersFor returns all the counter names for the give group name.
   *
   * @param group
   * @return Collection<String>
   */
  Collection<String> getCountersFor( String group );

  /**
   * Method getCountersFor returns all the counter names for the counter Enum.
   *
   * @param group
   * @return Collection<String>
   */
  Collection<String> getCountersFor( Class<? extends Enum> group );

  /**
   * Method getCounter returns the current value for the given counter Enum.
   *
   * @param counter of type Enum
   * @return the current counter value
   */
  long getCounterValue( Enum counter );

  /**
   * Method getCounter returns the current value for the given group and counter.
   *
   * @param group   of type String
   * @param counter of type String
   * @return the current counter value
   */
  long getCounterValue( String group, String counter );
  }
