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

package cascading.flow.stream;

import java.util.Comparator;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 *
 */
public class SparseTupleComparator implements Comparator<Tuple>
  {
  final Comparator[] comparators;

  private class NaturalComparator implements Comparator<Object>
    {
    @Override
    public int compare( Object lhs, Object rhs )
      {
      if( lhs == null && rhs == null )
        return 0;
      else if( lhs == null && rhs != null )
        return -1;
      else if( lhs != null && rhs == null )
        return 1;
      else
        return ( (Comparable) lhs ).compareTo( (Comparable) rhs ); // guaranteed to not be null
      }
    }

  public SparseTupleComparator( Fields valuesField, Fields sortField )
    {
    comparators = new Comparator[ valuesField.size() ];

    for( int i = 0; i < sortField.size(); i++ )
      {
      Comparable field = sortField.get( i );
      int pos = valuesField.getPos( field );

      comparators[ pos ] = sortField.getComparators()[ i ];

      if( comparators[ pos ] == null )
        comparators[ pos ] = new NaturalComparator();
      }
    }

  @Override
  public int compare( Tuple lhs, Tuple rhs )
    {
    for( int i = 0; i < comparators.length; i++ )
      {
      Comparator comparator = comparators[ i ];

      if( comparator == null )
        continue;

      int c = comparator.compare( lhs.getObject( i ), rhs.getObject( i ) );

      if( c != 0 )
        return c;
      }

    return 0;
    }
  }
