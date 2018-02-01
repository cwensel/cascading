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

import org.junit.Test;

import static org.junit.Assert.*;

/**
 *
 */
public class TrieTest
  {
  @Test
  public void testGetPrefix()
    {
    Trie<String> trie = new Trie<>();

    trie.put( "a", "a" );
    trie.put( "aa", "aa" );
    trie.put( "aaa", "aaa" );
    trie.put( "ab", "ab" );
    trie.put( "abb", "abb" );
    trie.put( "aabb", "aabb" );

    assertEquals( null, trie.get( "" ) );
    assertEquals( "a", trie.get( "a" ) );
    assertEquals( "a", trie.get( "azzz" ) );
    assertEquals( "aa", trie.get( "aa" ) );
    assertEquals( "aaa", trie.get( "aaa" ) );
    assertEquals( "ab", trie.get( "ab" ) );
    assertEquals( "abb", trie.get( "abb" ) );
    assertEquals( "aabb", trie.get( "aabb" ) );
    assertEquals( "aabb", trie.get( "aabbcdef" ) );
    assertNull( trie.get( "zzz" ) );
    assertNull( trie.get( "" ) );
    }

  @Test
  public void testHasPrefix()
    {
    Trie<String> trie = new Trie<>();

    trie.put( "a", "a" );
    trie.put( "aa", "aa" );
    trie.put( "aaa", "aaa" );
    trie.put( "ab", "ab" );
    trie.put( "abb", "abb" );
    trie.put( "aabb", "aabb" );

    assertTrue( trie.hasPrefix( "a" ) );
    assertTrue( trie.hasPrefix( "aa" ) );
    assertTrue( trie.hasPrefix( "aaa" ) );
    assertFalse( trie.hasPrefix( "" ) );
    assertFalse( trie.hasPrefix( "5" ) );
    assertFalse( trie.hasPrefix( "zzzz" ) );
    }

  @Test
  public void testGetCommonPrefix()
    {
    Trie<String> trie = new Trie<>();

    trie.put( "a", "a" );
    trie.put( "aa", "aa" );
    trie.put( "aaa", "aaa" );
    trie.put( "aabb", "aabb" );
    trie.put( "aaabb", "aaabb" );
    trie.put( "aacbb", "aacbb" );

    assertEquals( "aa", trie.getCommonPrefix() );
    }

  @Test
  public void testGetCommonPrefixNone()
    {
    Trie<String> trie = new Trie<>();

    trie.put( "a", "a" );
    trie.put( "b", "a" );
    trie.put( "c", "a" );
    trie.put( "e", "a" );
    trie.put( "f", "a" );

    assertEquals( "", trie.getCommonPrefix() );
    }
  }
