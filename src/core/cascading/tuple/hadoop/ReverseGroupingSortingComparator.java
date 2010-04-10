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

import cascading.tuple.TuplePair;

/** Class ReverseTuplePairComparator is an implementation of {@link org.apache.hadoop.io.RawComparator}. */
public class ReverseGroupingSortingComparator extends GroupingSortingComparator
  {
  @Override
  public int compare( byte[] b1, int s1, int l1, byte[] b2, int s2, int l2 )
    {
    return -1 * super.compare( b1, s1, l1, b2, s2, l2 );
    }

  @Override
  public int compare( TuplePair lhs, TuplePair rhs )
    {
    return super.compare( rhs, lhs );
    }

  }