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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public abstract class MultiMap<C extends Collection<V>, K, V> implements Serializable
  {
  private Map<K, C> map = null;

  protected Map<K, C> getMap()
    {
    if( map == null )
      map = createMap();

    return map;
    }

  protected abstract Map<K, C> createMap();

  protected abstract C createCollection();

  protected abstract C emptyCollection();

  public boolean containsKey( K key )
    {
    return getMap().containsKey( key );
    }

  public Set<K> getKeys()
    {
    return getMap().keySet();
    }

  public Set<Map.Entry<K, C>> getEntries()
    {
    return getMap().entrySet();
    }

  public void addAll( MultiMap<C, K, V> sourceMap )
    {
    if( sourceMap == null )
      return;

    Map<K, C> destinationMap = sourceMap.getMap();

    for( Map.Entry<K, C> entry : destinationMap.entrySet() )
      addAll( entry.getKey(), entry.getValue() );
    }

  public void put( K key, V value )
    {
    getMultiValues( key ).add( value );
    }

  public C remove( K key )
    {
    return getMap().remove( key );
    }

  protected C getMultiValues( K key )
    {
    Map<K, C> map = getMap();
    C values = map.get( key );

    if( values == null )
      {
      values = createCollection();
      map.put( key, values );
      }

    return values;
    }

  public void addAll( K key, V... values )
    {
    addAll( key, Arrays.asList( values ) );
    }

  public void addAll( K key, Collection<V> values )
    {
    getMultiValues( key ).addAll( values );
    }

  public C getValues()
    {
    if( getMap().isEmpty() )
      return emptyCollection();

    C results = createCollection();

    for( C values : getMap().values() )
      results.addAll( values );

    return results;
    }

  public C getValues( K key )
    {
    C values = getMap().get( key );

    if( values == null )
      return emptyCollection();

    return values;
    }

  public C getAllValues( K... keys )
    {
    if( keys.length == 0 )
      return emptyCollection();

    if( keys.length == 1 )
      return getValues( keys[ 0 ] );

    C values = createCollection();

    for( K key : keys )
      {
      C current = getMap().get( key );

      if( current != null )
        values.addAll( current );
      }

    return values;
    }

  public Set<K> getKeysFor( V value )
    {
    Set<K> results = new HashSet<>();

    for( Map.Entry<K, C> entry : getMap().entrySet() )
      {
      if( entry.getValue().contains( value ) )
        results.add( entry.getKey() );
      }

    return results;
    }

  public boolean hasKey( V value )
    {
    Set<K> keys = getKeysFor( value );

    return keys != null && !keys.isEmpty();
    }

  public boolean hadKey( K key, V value )
    {
    C values = getMap().get( key );

    return values != null && values.contains( value );
    }

  public boolean isEmpty()
    {
    return getMap().isEmpty();
    }

  @Override
  public String toString()
    {
    final StringBuilder sb = new StringBuilder( "MultiMap{" );
    sb.append( "map=" ).append( getMap() );
    sb.append( '}' );
    return sb.toString();
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;

    if( !( object instanceof MultiMap ) )
      return false;

    MultiMap multiMap = (MultiMap) object;

    if( map != null ? !map.equals( multiMap.map ) : multiMap.map != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    return map != null ? map.hashCode() : 0;
    }
  }
