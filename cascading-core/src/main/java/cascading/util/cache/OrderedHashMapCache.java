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

package cascading.util.cache;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the {@link CascadingCache} interface backed by a {@link java.util.LinkedHashMap}. This implementation
 * is used by default by {@link cascading.pipe.assembly.Unique} and {@link cascading.pipe.assembly.AggregateBy} and their
 * subclasses.
 *
 * @see cascading.pipe.assembly.Unique
 * @see cascading.pipe.assembly.AggregateBy
 * @see DirectMappedCacheFactory
 * @see OrderedHashMapCacheFactory
 */
public class OrderedHashMapCache<Key, Value> implements CascadingCache<Key, Value>
  {
  /** logger */
  private static final Logger LOG = LoggerFactory.getLogger( OrderedHashMapCache.class );

  /** capacity of the map. */
  private int capacity;

  /** call-back used, when entries are removed from the cache. */
  private CacheEvictionCallback callback = CacheEvictionCallback.NULL;

  /** counts flushes. */
  private long flushes = 0;

  private LinkedHashMap<Key, Value> backingMap;

  @Override
  public int getCapacity()
    {
    return capacity;
    }

  @Override
  public void setCacheEvictionCallback( CacheEvictionCallback cacheEvictionCallback )
    {
    this.callback = cacheEvictionCallback;
    }

  @Override
  public void setCapacity( int capacity )
    {
    this.capacity = capacity;
    }

  @Override
  public void initialize()
    {
    this.backingMap = new LinkedHashMap<Key, Value>( capacity, 0.75f, true )
    {
    @Override
    protected boolean removeEldestEntry( Map.Entry<Key, Value> eldest )
      {
      boolean doRemove = size() > capacity;
      if( doRemove )
        {
        callback.evict( eldest );
        if( flushes % getCapacity() == 0 ) // every multiple, write out data
          {
          Runtime runtime = Runtime.getRuntime();
          long freeMem = runtime.freeMemory() / 1024 / 1024;
          long maxMem = runtime.maxMemory() / 1024 / 1024;
          long totalMem = runtime.totalMemory() / 1024 / 1024;
          LOG.info( "flushed keys num times: {}, with capacity: {}", flushes + 1, capacity );
          LOG.info( "mem on flush (mb), free: " + freeMem + ", total: " + totalMem + ", max: " + maxMem );
          float percent = (float) totalMem / (float) maxMem;
          if( percent < 0.80F )
            LOG.info( "total mem is {}% of max mem, to better utilize unused memory consider increasing the cache size", (int) ( percent * 100.0F ) );
          }
        flushes++;
        }
      return doRemove;
      }
    };
    }

  @Override
  public int size()
    {
    return backingMap.size();
    }

  @Override
  public boolean isEmpty()
    {
    return backingMap.isEmpty();
    }

  @Override
  public boolean containsKey( Object key )
    {
    return backingMap.containsKey( key );
    }

  @Override
  public boolean containsValue( Object value )
    {
    return backingMap.containsValue( value );
    }

  @Override
  public Value get( Object key )
    {
    return backingMap.get( key );
    }

  @Override
  public Value put( Key key, Value value )
    {
    return backingMap.put( key, value );
    }

  @Override
  public Value remove( Object key )
    {
    return backingMap.remove( key );
    }

  @Override
  public void putAll( Map<? extends Key, ? extends Value> m )
    {
    backingMap.putAll( m );
    }

  @Override
  public void clear()
    {
    backingMap.clear();
    }

  @Override
  public Set<Key> keySet()
    {
    return backingMap.keySet();
    }

  @Override
  public Collection<Value> values()
    {
    return backingMap.values();
    }

  @Override
  public Set<Entry<Key, Value>> entrySet()
    {
    return backingMap.entrySet();
    }
  }
