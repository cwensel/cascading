/*
 * Copyright (c) 2016-2017 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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
import java.util.HashMap;
import java.util.Map;

/**
 * This class is based on the work found here by Vladimir Kroz, made available without restriction.
 * <p>
 * https://vkroz.wordpress.com/2012/03/23/prefix-table-trie-implementation-in-java/
 */
public class Trie<V extends Serializable> implements Serializable
  {
  static public class Entry<V> implements Serializable
    {
    String prefix;
    V value;

    public Entry()
      {
      }

    public Entry( String p, V v )
      {
      prefix = p;
      value = v;
      }

    public String prefix()
      {
      return prefix;
      }

    public V value()
      {
      return value;
      }

    @Override
    public String toString()
      {
      final StringBuilder sb = new StringBuilder( "Entry{" );
      sb.append( "prefix='" ).append( prefix ).append( '\'' );
      sb.append( ", value=" ).append( value );
      sb.append( '}' );
      return sb.toString();
      }
    }

  Entry<V> entry;
  char key;
  Map<Character, Trie<V>> children;

  public Trie()
    {
    this.children = new HashMap<>();
    this.entry = new Entry<>();
    }

  Trie( char key )
    {
    this.children = new HashMap<>();
    this.key = key;
    entry = new Entry<V>();
    }

  public void put( String key, V value )
    {
    put( new StringBuffer( key ), new StringBuffer( "" ), value );
    }

  void put( StringBuffer remainder, StringBuffer prefix, V value )
    {
    if( remainder.length() <= 0 )
      {
      this.entry.value = value;
      this.entry.prefix = prefix.toString();
      return;
      }

    char keyElement = remainder.charAt( 0 );
    Trie<V> trie = null;

    try
      {
      trie = children.get( keyElement );
      }
    catch( IndexOutOfBoundsException ignored )
      {
      }

    if( trie == null )
      {
      trie = new Trie<>( keyElement );
      children.put( keyElement, trie );
      }

    prefix.append( remainder.charAt( 0 ) );
    trie.put( remainder.deleteCharAt( 0 ), prefix, value );
    }

  public V get( String key )
    {
    return get( new StringBuffer( key ), 0 );
    }

  public boolean hasPrefix( String key )
    {
    return ( this.get( key ) != null );
    }

  V get( StringBuffer key, int level )
    {
    if( key.length() <= 0 )
      return entry.value;

    Trie<V> trie = children.get( key.charAt( 0 ) );

    if( trie != null )
      return trie.get( key.deleteCharAt( 0 ), ++level );
    else
      return ( level > 0 ) ? entry.value : null;
    }

  public String getCommonPrefix()
    {
    StringBuffer buffer = new StringBuffer();

    if( children.size() != 1 )
      return buffer.toString();

    buildPrefix( buffer, this );

    return buffer.toString();
    }

  private void buildPrefix( StringBuffer buffer, Trie<V> current )
    {
    if( current.children.size() != 1 )
      return;

    for( Map.Entry<Character, Trie<V>> entry : current.children.entrySet() )
      {
      buffer.append( entry.getKey() );

      buildPrefix( buffer, entry.getValue() );
      }
    }

  @Override
  public String toString()
    {
    final StringBuilder sb = new StringBuilder( "Trie{" );
    sb.append( "entry=" ).append( entry );
    sb.append( ", key=" ).append( key );
    sb.append( ", children=" ).append( children );
    sb.append( '}' );
    return sb.toString();
    }
  }
