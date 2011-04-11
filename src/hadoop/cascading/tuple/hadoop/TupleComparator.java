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

package cascading.tuple.hadoop;

import java.io.IOException;

import cascading.CascadingException;
import cascading.tuple.Tuple;
import org.apache.hadoop.conf.Configurable;

public class TupleComparator extends DeserializerComparator<Tuple> implements Configurable
  {
  public int compare( byte[] b1, int s1, int l1, byte[] b2, int s2, int l2 )
    {
    try
      {
      lhsBuffer.reset( b1, s1, l1 );
      rhsBuffer.reset( b2, s2, l2 );

      return compareTuples( groupComparators );
      }
    catch( IOException exception )
      {
      throw new CascadingException( exception );
      }
    finally
      {
      lhsBuffer.clear();
      rhsBuffer.clear();
      }
    }

  public int compare( Tuple lhs, Tuple rhs )
    {
    return compareTuples( groupComparators, lhs, rhs );
    }
  }