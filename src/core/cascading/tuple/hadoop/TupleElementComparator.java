/*
 * Copyright (c) 2007-2010 Concurrent, Inc. All Rights Reserved.
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

package cascading.tuple.hadoop;

import java.util.Comparator;

import cascading.CascadingException;
import cascading.tuple.StreamComparator;
import cascading.tuple.TupleInputStream;

/**
 *
 */
public class TupleElementComparator implements StreamComparator<TupleInputStream>, Comparator<Object>
  {
  Comparator comparator = new Comparator<Comparable>()
  {
  @Override
  public int compare( Comparable lhs, Comparable rhs )
    {
    if( lhs == null && rhs == null )
      return 0;

    if( lhs == null && rhs != null )
      return -1;

    if( lhs != null && rhs == null )
      return 1;

    return lhs.compareTo( rhs ); // guaranteed to not be null
    }
  };

  public TupleElementComparator()
    {
    }

  public TupleElementComparator( Comparator comparator )
    {
    if( comparator != null )
      this.comparator = comparator;
    }

  @Override
  public int compare( Object lhs, Object rhs )
    {
    return comparator.compare( lhs, rhs );
    }

  @Override
  public int compare( TupleInputStream lhsStream, TupleInputStream rhsStream )
    {
    try
      {
      Object lhs = lhsStream.getNextElement();
      Object rhs = rhsStream.getNextElement();

      return comparator.compare( lhs, rhs );
      }
    catch( Exception exception )
      {
      throw new CascadingException( "unable to compare Tuples, likely a CoGroup is being attempted on fields of " +
        "different types or custom comparators are incorrectly set on Fields", exception );
      }
    }
  }
