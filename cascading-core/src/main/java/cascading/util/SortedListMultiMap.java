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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 *
 */
public class SortedListMultiMap<K, V> extends MultiMap<List<V>, K, V>
  {
  Comparator<K> comparator = null;
  int initialListSize = 10;

  public SortedListMultiMap()
    {
    }

  public SortedListMultiMap( int initialListSize )
    {
    this.initialListSize = initialListSize;
    }

  public SortedListMultiMap( Comparator<K> comparator, int initialListSize )
    {
    this.comparator = comparator;
    this.initialListSize = initialListSize;
    }

  public V get( K key, int pos )
    {
    List<V> multiValues = getMultiValues( key );

    return multiValues.get( pos );
    }

  public V set( K key, int pos, V value )
    {
    List<V> multiValues = getMultiValues( key );

    return multiValues.set( pos, value );
    }

  @Override
  protected Map<K, List<V>> createMap()
    {
    return new TreeMap<>( comparator );
    }

  @Override
  protected List<V> createCollection()
    {
    return new ArrayList<>( initialListSize );
    }

  @Override
  protected List<V> emptyCollection()
    {
    return Collections.emptyList();
    }

  public Map.Entry<K, List<V>> firstEntry()
    {
    return ( (TreeMap) getMap() ).firstEntry();
    }

  public Map.Entry<K, List<V>> pollFirstEntry()
    {
    return ( (TreeMap) getMap() ).pollFirstEntry();
    }
  }
