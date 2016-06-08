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

package cascading.util;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class SetMultiMap<K, V> extends MultiMap<Set<V>, K, V>
  {
  @Override
  protected Map<K, Set<V>> createMap()
    {
    return new HashMap<>();
    }

  protected Set<V> createCollection()
    {
    return new LinkedHashSet<>();
    }

  protected Set<V> emptyCollection()
    {
    return Collections.emptySet();
    }
  }
