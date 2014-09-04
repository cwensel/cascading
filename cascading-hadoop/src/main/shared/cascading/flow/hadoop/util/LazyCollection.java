/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
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
import java.util.Iterator;

import cascading.tuple.Tuple;

/**
 *
 */
public class LazyCollection implements Collection<Tuple>, ResettableCollection<Iterator<Tuple>>
  {
  Iterator<Tuple> iterator;
  Collection<Tuple> parent;

  public static Collection<Tuple> getParent( LazyCollection collection )
    {
    return collection.parent;
    }

  public LazyCollection( Collection<Tuple> parent )
    {
    this.parent = parent;
    }

  @Override
  public int size()
    {
    return parent.size();
    }

  @Override
  public boolean isEmpty()
    {
    return !iterator.hasNext() && parent.isEmpty();
    }

  @Override
  public boolean contains( Object o )
    {
    return parent.contains( o );
    }

  @Override
  public Iterator<Tuple> iterator()
    {
    if( !iterator.hasNext() )
      return parent.iterator();

    return new Iterator<Tuple>()
    {
    @Override
    public boolean hasNext()
      {
      return iterator.hasNext();
      }

    @Override
    public Tuple next()
      {
      Tuple next = iterator.next();

      parent.add( next );

      return next;
      }

    @Override
    public void remove()
      {
      iterator.remove();
      }
    };
    }

  @Override
  public Object[] toArray()
    {
    return parent.toArray();
    }

  @Override
  public <T> T[] toArray( T[] a )
    {
    return parent.toArray( a );
    }

  @Override
  public boolean add( Tuple objects )
    {
    return parent.add( objects );
    }

  @Override
  public boolean remove( Object o )
    {
    return parent.remove( o );
    }

  @Override
  public boolean containsAll( Collection<?> c )
    {
    return parent.containsAll( c );
    }

  @Override
  public boolean addAll( Collection<? extends Tuple> c )
    {
    return parent.addAll( c );
    }

  @Override
  public boolean removeAll( Collection<?> c )
    {
    return parent.removeAll( c );
    }

  @Override
  public boolean retainAll( Collection<?> c )
    {
    return parent.retainAll( c );
    }

  @Override
  public void clear()
    {
    parent.clear();
    }

  @Override
  public boolean equals( Object o )
    {
    return parent.equals( o );
    }

  @Override
  public int hashCode()
    {
    return parent.hashCode();
    }

  @Override
  public void reset( Iterator<Tuple> iterator )
    {
    this.iterator = iterator;
    }
  }
