/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
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

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class EnumMultiMap<V> extends SetMultiMap<Enum, V>
  {
  public EnumMultiMap()
    {
    }

  public EnumMultiMap( EnumMultiMap map )
    {
    addAll( map );
    }

  @Override
  protected Map<Enum, Set<V>> createMap()
    {
    return new IdentityHashMap<>();
    }

  @Override
  protected Set<V> createCollection()
    {
    return Util.createIdentitySet();
    }
  }
