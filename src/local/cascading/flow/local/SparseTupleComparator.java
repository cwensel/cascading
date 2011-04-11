/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow.local;

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
