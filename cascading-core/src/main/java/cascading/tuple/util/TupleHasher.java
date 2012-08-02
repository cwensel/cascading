/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
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

package cascading.tuple.util;

import java.util.Comparator;
import java.util.List;

import cascading.tuple.Hasher;
import cascading.tuple.Tuple;

/**
 *
 */
public class TupleHasher
  {
  private static Hasher DEFAULT = new ObjectHasher();
  private Hasher[] hashers;

  public TupleHasher()
    {
    }

  public TupleHasher( Comparator defaultComparator, Comparator[] comparators )
    {
    initialize( defaultComparator, comparators );
    }

  protected void initialize( Comparator defaultComparator, Comparator[] comparators )
    {
    Hasher defaultHasher = DEFAULT;

    if( defaultComparator instanceof Hasher )
      defaultHasher = (Hasher) defaultComparator;

    hashers = new Hasher[ comparators.length ];

    for( int i = 0; i < comparators.length; i++ )
      {
      Comparator comparator = comparators[ i ];

      if( comparator instanceof Hasher )
        hashers[ i ] = (Hasher) comparator;
      else
        hashers[ i ] = defaultHasher;
      }
    }

  public final int hashCode( Tuple tuple )
    {
    int hash = 1;

    List<Object> elements = Tuple.elements( tuple );

    for( int i = 0; i < elements.size(); i++ )
      {
      Object element = elements.get( i );

      hash = 31 * hash + ( element != null ? hashers[ i % hashers.length ].hashCode( element ) : 0 );
      }

    return hash;
    }

  private static class ObjectHasher implements Hasher<Object>
    {
    @Override
    public int hashCode( Object value )
      {
      return value.hashCode();
      }
    }
  }
