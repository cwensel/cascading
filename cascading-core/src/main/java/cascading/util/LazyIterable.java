/*
 * Copyright (c) 2016-2018 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;

/**
 *
 */
public abstract class LazyIterable<V, T> implements Iterable<T>
  {
  LinkedList<T> cache;
  Iterable<V> iterable;

  public LazyIterable( V... values )
    {
    this( Arrays.asList( values ) );
    }

  public LazyIterable( boolean enableCache, V... values )
    {
    this( enableCache, Arrays.asList( values ) );
    }

  public LazyIterable( Iterable<V> iterable )
    {
    this( true, iterable );
    }

  public LazyIterable( boolean enableCache, Iterable<V> iterable )
    {
    if( enableCache )
      this.cache = new LinkedList<>();

    this.iterable = iterable;
    }

  @Override
  public Iterator<T> iterator()
    {
    if( cache != null && !cache.isEmpty() )
      return cache.iterator();

    final Iterator<V> iterator = iterable.iterator();

    return new Iterator<T>()
      {
      @Override
      public boolean hasNext()
        {
        return iterator.hasNext();
        }

      @Override
      public T next()
        {
        T converted = convert( iterator.next() );

        if( cache != null )
          cache.add( converted );

        return converted;
        }

      @Override
      public void remove()
        {
        iterator.remove();
        }
      };
    }

  protected abstract T convert( V next );
  }
