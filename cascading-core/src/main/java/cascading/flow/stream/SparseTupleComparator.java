/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow.stream;

import java.util.Comparator;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 *
 */
public class SparseTupleComparator implements Comparator<Tuple>
  {
  private final static Comparator DEFAULT = new NaturalComparator();

  private static class NaturalComparator implements Comparator<Object>
    {
    @Override
    public int compare( Object lhs, Object rhs )
      {
      if( lhs == null && rhs == null )
        return 0;
      else if( lhs == null )
        return -1;
      else if( rhs == null )
        return 1;
      else
        return ( (Comparable) lhs ).compareTo( rhs ); // guaranteed to not be null
      }
    }

  final Comparator[] comparators;
  final Integer[] posMap;

  public SparseTupleComparator( Fields valuesField, Fields sortFields )
    {
    this( valuesField, sortFields, null );
    }

  public SparseTupleComparator( Fields groupFields, Comparator defaultComparator )
    {
    this( groupFields, groupFields, defaultComparator );
    }

  public SparseTupleComparator( Fields valuesFields, Fields sortFields, Comparator defaultComparator )
    {
    if( defaultComparator == null )
      defaultComparator = DEFAULT;

    int size = valuesFields != null && !valuesFields.isUnknown() ? valuesFields.size() : sortFields.size();
    comparators = new Comparator[ size ];
    posMap = new Integer[ size ];

    Comparator[] sortFieldComparators = sortFields.getComparators(); // returns a copy

    for( int i = 0; i < sortFields.size(); i++ )
      {
      Comparable field = sortFields.get( i );
      int pos = valuesFields != null ? valuesFields.getPos( field ) : i;

      comparators[ i ] = sortFieldComparators[ i ];
      posMap[ i ] = pos;

      if( comparators[ i ] == null )
        comparators[ i ] = defaultComparator;
      }
    }

  public Comparator[] getComparators()
    {
    return comparators;
    }

  @Override
  public int compare( Tuple lhs, Tuple rhs )
    {
    for( int i = 0; i < comparators.length; i++ )
      {
      Comparator comparator = comparators[ i ];

      if( comparator == null )
        continue;

      Integer pos = posMap[ i ];
      int c = comparator.compare( lhs.getObject( pos ), rhs.getObject( pos ) );

      if( c != 0 )
        return c;
      }

    return 0;
    }
  }
