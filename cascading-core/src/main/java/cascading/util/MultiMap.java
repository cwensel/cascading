/*
 * Copyright (c) 2007-2009 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Cascading is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Cascading is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Cascading.  If not, see <http://www.gnu.org/licenses/>.
 */

package cascading.util;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class MultiMap<V> implements Serializable
  {
  private Map<Enum, Set<V>> map = null;

  public MultiMap()
    {
    }

  public MultiMap( MultiMap map )
    {
    addAll( map );
    }

  protected Map<Enum, Set<V>> getMap()
    {
    if( map == null )
      map = new IdentityHashMap<>();

    return map;
    }

  public Set<Enum> getAllKeys()
    {
    return getMap().keySet();
    }

  public void addAll( MultiMap<V> annotations )
    {
    if( annotations == null )
      return;

    for( Map.Entry<Enum, Set<V>> entry : annotations.getMap().entrySet() )
      addAll( entry.getKey(), entry.getValue() );
    }

  public void addAll( Enum key, V... values )
    {
    addAll( key, Arrays.asList( values ) );
    }

  public void addAll( Enum key, Collection<V> values )
    {
    if( !getMap().containsKey( key ) )
      getMap().put( key, new LinkedHashSet<V>() );

    getMap().get( key ).addAll( values );
    }

  public Set<V> getValues()
    {
    if( getMap().isEmpty() )
      return Collections.emptySet();

    Set<V> results = new HashSet<>();

    for( Set<V> values : getMap().values() )
      results.addAll( values );

    return results;
    }

  public Set<V> getValues( Enum key )
    {
    Set<V> values = getMap().get( key );

    if( values == null )
      return Collections.emptySet();

    return values;
    }

  public Set<V> getAllValues( Enum... keys )
    {
    if( keys.length == 0 )
      return Collections.emptySet();

    if( keys.length == 1 )
      return getValues( keys[ 0 ] );

    Set<V> values = new LinkedHashSet<>();

    for( Enum key : keys )
      {
      Set<V> current = getMap().get( key );

      if( current != null )
        values.addAll( current );
      }

    return values;
    }

  public Set<Enum> getKeysFor( V value )
    {
    Set<Enum> results = new HashSet<>();

    for( Map.Entry<Enum, Set<V>> entry : getMap().entrySet() )
      {
      if( entry.getValue().contains( value ) )
        results.add( entry.getKey() );
      }

    return results;
    }

  public boolean hasKey( V value )
    {
    Set<Enum> keys = getKeysFor( value );

    return keys != null && !keys.isEmpty();
    }

  public boolean hadKey( Enum key, V value )
    {
    Set<V> values = getMap().get( key );

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
  }
