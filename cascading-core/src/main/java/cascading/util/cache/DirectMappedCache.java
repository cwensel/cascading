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

import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DirectMappedCache is an implementation of the {@link cascading.util.cache.CascadingCache} interface following the semantics of
 * http://en.wikipedia.org/wiki/CPU_cache#Direct-mapped_cache. The Cache is backed by an array that stays constant in size.
 * <p/>
 * Unlike other implementation of a Map a hash collision will lead to the entry being overwritten.
 * The {@link CacheEvictionCallback} is called with the entry that will be overwritten.
 * <p/>
 * Use this cache if the keys are arriving in a random if not uniformly distributed order in order to reduce the number
 * of hash and equality comparisons.
 * <p/>
 * If duplicate keys are clustered near each other in the incoming tuple stream, consider the {@link cascading.util.cache.LRUHashMapCache} cache
 * instead.
 * <p/>
 * DirectMappedCache does not permit <code>null</code> keys nor <code>null</code> values
 *
 * @see cascading.pipe.assembly.Unique
 * @see cascading.pipe.assembly.AggregateBy
 * @see DirectMappedCacheFactory
 * @see LRUHashMapCacheFactory
 * @see LRUHashMapCache
 */
public final class DirectMappedCache<Key, Value> implements CascadingCache<Key, Value>
  {
  /** logger */
  private static final Logger LOG = LoggerFactory.getLogger( DirectMappedCache.class );

  /** size of the backing array. */
  private int capacity;

  /** number of actual key in the map. */
  private int actualSize = 0;

  /** array used for storing the entries */
  private Entry<Key, Value>[] elements;

  private long collisions = 0;

  private long putCalls = 0;

  public boolean initialized = false;

  /** callback that is called whenever an entry is overwritten or removed from the cache. */
  private CacheEvictionCallback evictionCallBack = CacheEvictionCallback.NULL;

  @Override
  public int size()
    {
    return actualSize;
    }

  @Override
  public boolean isEmpty()
    {
    return actualSize < 1;
    }

  @Override
  public boolean containsKey( Object key )
    {
    if( key == null )
      throw new IllegalArgumentException( "null keys are not permitted" );

    return get( key ) != null;
    }

  @Override
  public boolean containsValue( Object value )
    {
    if( value == null )
      throw new IllegalArgumentException( "value cannot be null" );

    Iter<Entry<Key, Value>> iter = new Iter<Entry<Key, Value>>( elements );

    while( iter.hasNext() )
      {
      Value current = iter.next().getValue();
      if( current.equals( value ) )
        return true;
      }

    return false;
    }

  @Override
  public Value get( Object key )
    {
    int index = index( key );
    Entry<Key, Value> existing = elements[ index ];

    if( existing == null || !key.equals( existing.getKey() ) )
      return null;

    return existing.getValue();
    }

  @Override
  public Value put( Key key, Value value )
    {
    if( key == null )
      throw new IllegalArgumentException( "key cannot be null" );

    if( value == null )
      throw new IllegalArgumentException( "value cannot be null" );

    putCalls++;

    Value previous = null;
    int index = index( key );
    Entry<Key, Value> existing = elements[ index ];

    if( existing != null )
      {
      previous = existing.getValue();
      collisions++;
      evictionCallBack.evict( existing );
      }
    else
      actualSize++;

    elements[ index ] = new CacheEntry<Key, Value>( key, value );

    if( putCalls % getCapacity() == 0 )
      {
      Runtime runtime = Runtime.getRuntime();
      long freeMem = runtime.freeMemory() / 1024 / 1024;
      long maxMem = runtime.maxMemory() / 1024 / 1024;
      long totalMem = runtime.totalMemory() / 1024 / 1024;
      LOG.info( "mem on flush (mb), free: " + freeMem + ", total: " + totalMem + ", max: " + maxMem );
      LOG.info( "capacity={}, puts={}, collisions={}, fill factor={}%", getCapacity(), putCalls, collisions,
        ( (double) getCapacity() / actualSize ) * 100 );
      float percent = (float) totalMem / (float) maxMem;
      if( percent < 0.80F )
        LOG.info( "total mem is {}% of max mem, to better utilize unused memory consider increasing the cache size", (int) ( percent * 100.0F ) );
      }

    return previous;
    }

  private int index( Object key )
    {
    return Math.abs( key.hashCode() % capacity );
    }

  @Override
  public Value remove( Object key )
    {
    if( key == null )
      throw new IllegalArgumentException( "key cannot be null" );

    int index = index( key );
    Entry<Key, Value> existing = elements[ index ];

    if( existing == null || !existing.getKey().equals( key ) )
      return null;

    elements[ index ] = null;
    actualSize--;
    evictionCallBack.evict( existing );

    return existing.getValue();
    }

  @Override
  public void putAll( Map<? extends Key, ? extends Value> m )
    {
    for( Entry<? extends Key, ? extends Value> entry : m.entrySet() )
      put( entry.getKey(), entry.getValue() );
    }

  @Override
  public void clear()
    {
    elements = new CacheEntry[ capacity ];
    actualSize = 0;
    }

  @Override
  public Set<Key> keySet()
    {
    return new KeySetView<Key>( new SetView<Map.Entry<Key, ?>>( elements, actualSize ) );
    }

  @Override
  public Collection<Value> values()
    {
    return new ValueCollection( new SetView<Map.Entry<?, Value>>( elements, actualSize ) );
    }

  @Override
  public Set<Map.Entry<Key, Value>> entrySet()
    {
    return new SetView<Entry<Key, Value>>( elements, actualSize );
    }

  @Override
  public int getCapacity()
    {
    return capacity;
    }

  @Override
  public void setCacheEvictionCallback( CacheEvictionCallback cacheEvictionCallback )
    {
    if( initialized )
      throw new IllegalStateException( "cannot set callback after initialization" );

    this.evictionCallBack = cacheEvictionCallback;
    }

  @Override
  public void setCapacity( int capacity )
    {
    if( initialized )
      throw new IllegalArgumentException( "cannot set size after initialization" );

    this.capacity = capacity;
    }

  @Override
  public void initialize()
    {
    if( capacity < 1 )
      throw new IllegalStateException( "capacity must be larger than 0" );

    if( evictionCallBack == null )
      throw new IllegalStateException( "evictionCallback cannot be null" );

    elements = new CacheEntry[ capacity ];
    initialized = true;
    }

  static class CacheEntry<Key, Value> implements Entry<Key, Value>
    {
    private final Key key;
    private Value value;

    CacheEntry( Key key, Value value )
      {
      this.key = key;
      this.value = value;
      }

    @Override
    public Key getKey()
      {
      return key;
      }

    @Override
    public Value getValue()
      {
      return value;
      }

    @Override
    public Value setValue( Value value )
      {
      Value oldValue = this.value;
      this.value = value;
      return oldValue;
      }

    @Override
    public boolean equals( Object object )
      {
      if( this == object )
        return true;
      if( object == null || getClass() != object.getClass() )
        return false;

      Entry that = (Entry) object;

      if( key != null ? !key.equals( that.getKey() ) : that.getKey() != null )
        return false;

      if( value != null ? !value.equals( that.getValue() ) : that.getValue() != null )
        return false;

      return true;
      }

    @Override
    public int hashCode()
      {
      int result = key != null ? key.hashCode() : 0;
      result = 31 * result + ( value != null ? value.hashCode() : 0 );
      return result;
      }

    @Override
    public String toString()
      {
      return "CacheEntry{" +
        "key=" + key +
        ", value=" + value +
        '}';
      }
    }

  class SetView<T> extends AbstractSet<T>
    {
    private final T[] elements;
    private final int size;
    private final Iter<T> iter;

    SetView( T[] elements, int size )
      {
      this.elements = elements;
      this.size = size;
      this.iter = new Iter<T>( elements );
      }

    @Override
    public int size()
      {
      return size;
      }

    @Override
    public Iterator<T> iterator()
      {
      return new Iter<T>( elements );
      }

    @Override
    public boolean containsAll( Collection<?> c )
      {
      for( Object object : c )
        {
        if( !contains( object ) )
          return false;
        }

      return true;
      }

    @Override
    public boolean addAll( Collection<? extends T> c )
      {
      throw new UnsupportedOperationException( "SetView is read only." );
      }

    @Override
    public boolean retainAll( Collection<?> c )
      {
      throw new UnsupportedOperationException( "SetView is read only." );
      }

    @Override
    public boolean removeAll( Collection<?> c )
      {
      throw new UnsupportedOperationException( "SetView is read only." );
      }

    @Override
    public void clear()
      {
      throw new UnsupportedOperationException( "SetView is read only." );
      }
    }

  static class Iter<T> implements Iterator<T>
    {
    private final T[] elements;
    private T nextElement;
    int currentIndex = -1;

    public Iter( T[] elements )
      {
      this.elements = elements;
      this.nextElement = findNext();
      }

    private T findNext()
      {
      while( currentIndex < elements.length - 1 )
        {
        currentIndex++;

        if( elements[ currentIndex ] != null )
          return elements[ currentIndex ];
        }

      return null;
      }

    @Override
    public boolean hasNext()
      {
      return nextElement != null;
      }

    @Override
    public T next()
      {
      if( !hasNext() )
        throw new NoSuchElementException();

      T current = nextElement;

      nextElement = findNext();

      return current;
      }

    @Override
    public void remove()
      {
      throw new UnsupportedOperationException( "Not supported" );
      }

    public void reset()
      {
      currentIndex = -1;
      nextElement = findNext();
      }
    }

  static class KeySetView<Key> extends AbstractSet<Key>
    {
    private final Set<Map.Entry<Key, ?>> parent;

    public KeySetView( Set<Map.Entry<Key, ?>> parent )
      {
      this.parent = parent;
      }

    @Override
    public Iterator<Key> iterator()
      {
      return new KeyIterator<Key>( parent.iterator() );
      }

    @Override
    public int size()
      {
      return parent.size();
      }
    }

  static class ValueCollection<Value> extends AbstractCollection<Value>
    {
    private Set<Entry<?, Value>> parent;

    ValueCollection( Set<Entry<?, Value>> parent )
      {
      this.parent = parent;
      }

    @Override
    public Iterator<Value> iterator()
      {
      return new ValueIterator<Value>( parent.iterator() );
      }

    @Override
    public int size()
      {
      return parent.size();
      }
    }

  static class KeyIterator<Key> implements Iterator<Key>
    {
    private final Iterator<Map.Entry<Key, ?>> parent;

    public KeyIterator( Iterator<Map.Entry<Key, ?>> parent )
      {
      this.parent = parent;
      }

    @Override
    public Key next()
      {
      return parent.next().getKey();
      }

    @Override
    public boolean hasNext()
      {
      return parent.hasNext();
      }

    @Override
    public void remove()
      {
      parent.remove();
      }
    }

  static class ValueIterator<Value> implements Iterator<Value>
    {
    private final Iterator<Map.Entry<?, Value>> parent;

    public ValueIterator( Iterator<Map.Entry<?, Value>> parent )
      {
      this.parent = parent;
      }

    @Override
    public Value next()
      {
      return parent.next().getValue();
      }

    @Override
    public boolean hasNext()
      {
      return parent.hasNext();
      }

    @Override
    public void remove()
      {
      parent.remove();
      }
    }
  }
