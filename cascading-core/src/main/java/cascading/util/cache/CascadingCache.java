/*
 * Copyright (c) 2007-2017 Xplenty, Inc. All Rights Reserved.
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

package cascading.util.cache;

import java.util.Map;

/**
 * Interface that defines a Cache.
 */
public interface CascadingCache<Key, Value> extends Map<Key, Value>
  {
  /** Method to initialize the Cache. Any setup should be done in here. */
  void initialize();

  /** Sets the capacity of the Cache. */
  void setCapacity( int capacity );

  /**
   * Returns the capacity of this cache.
   */
  int getCapacity();

  /** Sets the {@link CacheEvictionCallback} of this Cache. */
  void setCacheEvictionCallback( CacheEvictionCallback cacheEvictionCallback );
  }
