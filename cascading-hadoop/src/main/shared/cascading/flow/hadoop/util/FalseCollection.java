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

package cascading.flow.hadoop.util;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

import cascading.tuple.Tuple;

/**
 *
 */
public class FalseCollection implements Collection<Tuple>, ResettableCollection<Iterator<Tuple>>
  {
  boolean returnedIterator = false;
  Iterator<Tuple> iterator;

  @Override
  public void reset( Iterator<Tuple> iterator )
    {
    this.returnedIterator = false;
    this.iterator = iterator;
    }

  @Override
  public int size()
    {
    return 0;
    }

  @Override
  public boolean isEmpty()
    {
    return iterator == null || !iterator.hasNext();
    }

  @Override
  public boolean contains( Object o )
    {
    return false;
    }

  @Override
  public Iterator<Tuple> iterator()
    {
    if( returnedIterator )
      throw new IllegalStateException( "may not iterate this tuple stream more than once" );

    try
      {
      if( iterator == null )
        return Collections.emptyIterator();

      return iterator;
      }
    finally
      {
      returnedIterator = true;
      }
    }

  @Override
  public Object[] toArray()
    {
    return new Object[ 0 ];
    }

  @Override
  public <T> T[] toArray( T[] a )
    {
    return null;
    }

  @Override
  public boolean add( Tuple tuple )
    {
    return false;
    }

  @Override
  public boolean remove( Object o )
    {
    return false;
    }

  @Override
  public boolean containsAll( Collection<?> c )
    {
    return false;
    }

  @Override
  public boolean addAll( Collection<? extends Tuple> c )
    {
    return false;
    }

  @Override
  public boolean removeAll( Collection<?> c )
    {
    return false;
    }

  @Override
  public boolean retainAll( Collection<?> c )
    {
    return false;
    }

  @Override
  public void clear()
    {
    iterator = null;
    }
  }
